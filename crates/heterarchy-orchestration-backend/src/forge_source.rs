//! Typed, bounded observations from the configured Forge HTTP owner.
//!
//! These queries return source observations only. They do not grant Forge
//! mutation permission, establish lease ownership, or prove request replay
//! equivalence.

use forge_backend::ops::{CommandOp, LocatorDto, ScopeDto};
use forge_backend::{ForgeHttpClientError, ForgeHttpServiceCode};
use forge_control::ResolvedWorkspaceScope;
use forge_domain::{ActivityId, ActivityState, WorkspaceId};
use serde::{Deserialize, Deserializer};
use serde_json::{Value, json};
use thiserror::Error;
use uuid::Uuid;

use crate::forge_commands::ForgeCommandHttpClient;
mod coordinator;
pub mod forge_reads;
pub mod node_assignment;

/// A typed failure from a Forge source observation.
///
/// Peer messages, response bodies, credentials, and endpoint details are never
/// retained in this error.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum ForgeSourceError {
    /// The trusted source scope or Activity selector is invalid for this query.
    #[error("Forge source scope is invalid")]
    InvalidScope,
    /// Forge reported that the requested source scope is stale.
    #[error("Forge source scope is stale")]
    Stale,
    /// Forge rejected the source scope under its confinement policy.
    #[error("Forge denied the source scope")]
    Denied,
    /// The requested source subject does not exist. A not-found observation
    /// is classified only for reads whose Forge HTTP failure retains the
    /// control category; it grants nothing.
    #[error("Forge source subject was not found")]
    NotFound,
    /// The configured Forge source could not provide a fresh observation.
    #[error("Forge source is unavailable")]
    Unavailable,
    /// The Forge response violated the bounded source-query protocol.
    #[error("Forge source response violated its protocol")]
    Protocol,
}

/// One validated Activity row observed from Forge.
///
/// Other durable row fields are checked before construction but remain private;
/// callers only need the exact identities and lifecycle state for their own
/// authorization decision.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ForgeActivityObservation {
    activity_id: ActivityId,
    workspace_id: WorkspaceId,
    state: ActivityState,
}

impl ForgeActivityObservation {
    /// The exact Activity identity returned by Forge.
    #[must_use]
    pub const fn activity_id(&self) -> ActivityId {
        self.activity_id
    }

    /// The durable Workspace identity returned by Forge.
    #[must_use]
    pub const fn workspace_id(&self) -> WorkspaceId {
        self.workspace_id
    }

    /// The durable Activity lifecycle state returned by Forge.
    #[must_use]
    pub const fn state(&self) -> ActivityState {
        self.state
    }
}

/// Typed Workspace and Activity observations over the shared Forge command
/// transport.
#[must_use]
#[derive(Clone, Debug)]
pub struct ForgeSourceHttpClient {
    commands: ForgeCommandHttpClient,
}

impl ForgeSourceHttpClient {
    /// Reuse the host-configured, bounded Forge command transport.
    pub fn new(commands: ForgeCommandHttpClient) -> Self {
        Self { commands }
    }

    /// Share the already bounded authenticated transport with sibling source protocols.
    pub(super) fn commands(&self) -> &ForgeCommandHttpClient {
        &self.commands
    }

    /// Re-resolve the exact Workspace locator and compare the source identity
    /// with the caller's trusted scope reference.
    ///
    /// This is a fresh source observation, not a permission grant.
    ///
    /// # Errors
    /// Returns a typed invalid-scope, stale, denied, unavailable, or protocol
    /// error. A non-UTF-8 relative root is rejected without lossy conversion.
    pub async fn resolve_workspace(
        &self,
        expected: &ResolvedWorkspaceScope,
    ) -> Result<(), ForgeSourceError> {
        self.resolve_workspace_detailed(expected)
            .await
            .map_err(WorkspaceObservationError::source_class)
    }

    /// Preserve the redacted command class for host diagnostics without another source request.
    ///
    /// # Errors
    /// Returns the same scope failures or exact bounded transport/service error; no peer text.
    pub async fn resolve_workspace_detailed(
        &self,
        expected: &ResolvedWorkspaceScope,
    ) -> Result<(), WorkspaceObservationError> {
        let (scope, relative_root) =
            validated_scope(expected).map_err(WorkspaceObservationError::Scope)?;
        let payload = json!({
            "location_id": scope.locator.location_id,
            "expected_revision": scope.locator.expected_revision,
            "relative_root": relative_root,
        });
        let result = self
            .commands
            .command(CommandOp::WorkspaceResolve, payload)
            .await
            .map_err(WorkspaceObservationError::Command)?;
        let readback: WorkspaceResolveReadback =
            decode_result(result).map_err(WorkspaceObservationError::Scope)?;
        let workspace_id =
            canonical_uuid(&readback.workspace_id).map_err(WorkspaceObservationError::Scope)?;
        if readback.workspace_revision <= 0 {
            return Err(WorkspaceObservationError::Scope(ForgeSourceError::Protocol));
        }
        if workspace_id != scope.workspace_id
            || readback.workspace_revision != scope.workspace_revision
        {
            return Err(WorkspaceObservationError::Scope(ForgeSourceError::Stale));
        }
        Ok(())
    }

    /// Read the current durable row for one Activity in the exact trusted
    /// Workspace scope.
    ///
    /// A missing row is `Ok(None)`. Open, closed, and interrupted rows are all
    /// returned as observations; lease fields are validated but do not become
    /// permission or liveness checks.
    ///
    /// # Errors
    /// Returns a typed invalid-scope, stale, denied, unavailable, or protocol
    /// error. The response must echo the exact scope and Activity identity and
    /// must disavow both retry and request-equivalence authorization.
    pub async fn activity(
        &self,
        expected: &ResolvedWorkspaceScope,
        activity_id: Uuid,
    ) -> Result<Option<ForgeActivityObservation>, ForgeSourceError> {
        if activity_id.is_nil() {
            return Err(ForgeSourceError::InvalidScope);
        }
        let (scope, _) = validated_scope(expected)?;
        let payload = json!({
            "scope": scope,
            "activity_id": activity_id,
        });
        let result = self
            .commands
            .command(CommandOp::ActivityGet, payload)
            .await
            .map_err(map_command_error)?;
        let readback: ActivityGetReadback = decode_result(result)?;
        if readback.scope != scope
            || canonical_uuid(&readback.activity_id)? != activity_id
            || readback.retry_authorized
            || readback.request_equivalence_proven
        {
            return Err(ForgeSourceError::Protocol);
        }

        let activity = readback.activity;
        if readback.found != activity.is_some() {
            return Err(ForgeSourceError::Protocol);
        }
        activity
            .map(|row| validate_activity_row(&row, scope.workspace_id, activity_id))
            .transpose()
    }
}

/// Redacted Workspace observation failure retained by the existing single source call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkspaceObservationError {
    Scope(ForgeSourceError),
    Command(ForgeHttpClientError),
}

impl WorkspaceObservationError {
    /// Retain the established scope classification without altering retry semantics.
    #[must_use]
    pub fn source_class(self) -> ForgeSourceError {
        match self {
            Self::Scope(error) => error,
            Self::Command(error) => map_command_error(error),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WorkspaceResolveReadback {
    workspace_id: String,
    workspace_revision: i64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ActivityGetReadback {
    scope: ScopeDto,
    activity_id: String,
    found: bool,
    #[serde(deserialize_with = "deserialize_required_option")]
    activity: Option<ActivityRowReadback>,
    retry_authorized: bool,
    request_equivalence_proven: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ActivityRowReadback {
    activity_id: String,
    workspace_id: String,
    state: ActivityStateReadback,
    revision: i64,
    created_at_ms: i64,
    updated_at_ms: i64,
    #[serde(deserialize_with = "deserialize_required_option")]
    lease_owner_id: Option<String>,
    #[serde(deserialize_with = "deserialize_required_option")]
    lease_until_ms: Option<i64>,
    lease_revision: i64,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ActivityStateReadback {
    Open,
    Closed,
    Interrupted,
}

impl From<ActivityStateReadback> for ActivityState {
    fn from(value: ActivityStateReadback) -> Self {
        match value {
            ActivityStateReadback::Open => Self::Open,
            ActivityStateReadback::Closed => Self::Closed,
            ActivityStateReadback::Interrupted => Self::Interrupted,
        }
    }
}

pub(super) fn validated_scope(
    expected: &ResolvedWorkspaceScope,
) -> Result<(ScopeDto, String), ForgeSourceError> {
    let locator = expected.locator();
    let workspace_id = expected.workspace_id().into_uuid();
    let workspace_revision = expected.workspace_revision().get();
    let location_id = locator.location_id().into_uuid();
    let expected_revision = locator.expected_revision().get();
    let relative_root = locator
        .relative_root()
        .to_str()
        .ok_or(ForgeSourceError::InvalidScope)?
        .to_owned();
    if workspace_id.is_nil()
        || location_id.is_nil()
        || workspace_revision <= 0
        || expected_revision <= 0
    {
        return Err(ForgeSourceError::InvalidScope);
    }
    let scope = ScopeDto {
        workspace_id,
        workspace_revision,
        locator: LocatorDto {
            location_id,
            expected_revision,
            relative_root: relative_root.clone(),
        },
    };
    Ok((scope, relative_root))
}

pub(super) fn decode_result<T: serde::de::DeserializeOwned>(
    value: Value,
) -> Result<T, ForgeSourceError> {
    serde_json::from_value(value).map_err(|_| ForgeSourceError::Protocol)
}

fn deserialize_required_option<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer)
}

pub(super) fn canonical_uuid(value: &str) -> Result<Uuid, ForgeSourceError> {
    let parsed = Uuid::parse_str(value).map_err(|_| ForgeSourceError::Protocol)?;
    if parsed.is_nil() || parsed.to_string() != value {
        return Err(ForgeSourceError::Protocol);
    }
    Ok(parsed)
}

fn validate_activity_row(
    row: &ActivityRowReadback,
    expected_workspace_id: Uuid,
    expected_activity_id: Uuid,
) -> Result<ForgeActivityObservation, ForgeSourceError> {
    let activity_id = canonical_uuid(&row.activity_id)?;
    let workspace_id = canonical_uuid(&row.workspace_id)?;
    let lease_owner_id = row
        .lease_owner_id
        .as_deref()
        .map(canonical_uuid)
        .transpose()?;
    let lease_until_ms = row.lease_until_ms;
    if activity_id != expected_activity_id
        || workspace_id != expected_workspace_id
        || row.revision <= 0
        || row.lease_revision <= 0
        || row.created_at_ms < 0
        || row.updated_at_ms < 0
        || lease_owner_id.is_some() != lease_until_ms.is_some()
        || lease_until_ms.is_some_and(|value| value < 0)
    {
        return Err(ForgeSourceError::Protocol);
    }
    Ok(ForgeActivityObservation {
        activity_id: ActivityId::from_uuid(activity_id),
        workspace_id: WorkspaceId::from_uuid(workspace_id),
        state: row.state.into(),
    })
}

pub(super) fn map_command_error(error: ForgeHttpClientError) -> ForgeSourceError {
    match error {
        ForgeHttpClientError::Protocol => ForgeSourceError::Protocol,
        ForgeHttpClientError::Service { code, .. } => match code {
            ForgeHttpServiceCode::InvalidInput | ForgeHttpServiceCode::InvalidPathKind => {
                ForgeSourceError::InvalidScope
            }
            ForgeHttpServiceCode::WorkspaceConfinementDenied
            | ForgeHttpServiceCode::WorkspaceScopeOverlap => ForgeSourceError::Denied,
            ForgeHttpServiceCode::StaleRevision | ForgeHttpServiceCode::ResyncRequired => {
                ForgeSourceError::Stale
            }
            // Other classifications default to unavailable. In particular,
            // SOURCE_CONFLICT is a file-mutation class, not a Workspace or
            // Activity read-scope classification.
            _ => ForgeSourceError::Unavailable,
        },
        // The configured service credential is not the requesting user's
        // authority. Authentication failures therefore mean the source is
        // unavailable to this host and are never surfaced as caller denial.
        ForgeHttpClientError::Authentication
        | ForgeHttpClientError::Capacity
        | ForgeHttpClientError::Transport
        | ForgeHttpClientError::Timeout
        | ForgeHttpClientError::Request
        | ForgeHttpClientError::RequestTooLarge
        | ForgeHttpClientError::ResponseHeadersTooLarge
        | ForgeHttpClientError::ResponseTooLarge
        | ForgeHttpClientError::DestinationMismatch
        | ForgeHttpClientError::InvalidRequest => ForgeSourceError::Unavailable,
    }
}

#[cfg(test)]
mod tests;
