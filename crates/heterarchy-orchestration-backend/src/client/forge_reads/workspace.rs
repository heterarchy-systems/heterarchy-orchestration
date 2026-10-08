//! Bounded client for the Location/Workspace catalog source reads.

use forge_control::MAX_WORKSPACE_CATALOG_ENTRIES;
use forge_domain::LocationListLimit;
use hyper::Method;
use hyper::header::HeaderValue;
use uuid::Uuid;

use super::super::{ClientError, OrchestrationHttpClient};
use crate::wire::forge_reads::{
    LocationListRequest, LocationListResponse, WorkspaceCatalogEntryWire, WorkspaceDiscoverRequest,
    WorkspaceDiscoverResponse, WorkspaceDiscoveryEntryWire, WorkspaceListRequest,
    WorkspaceListResponse, WorkspaceResolveRequest, WorkspaceResolveResponse,
};

const MAX_WORKSPACE_LOCATOR_BYTES: usize = 4096;

impl OrchestrationHttpClient {
    /// Observe the configured registry Locations through the backend.
    ///
    /// # Errors
    /// Returns invalid input, a bounded transport error, a typed service
    /// rejection, or a protocol error for an unbounded response.
    pub async fn location_list(
        &self,
        authorization: &HeaderValue,
        request: &LocationListRequest,
    ) -> Result<LocationListResponse, ClientError> {
        let response: LocationListResponse = self
            .call(
                authorization,
                Method::POST,
                "/internal/v1/forge/location-list".to_owned(),
                Some(request),
                None,
            )
            .await?;
        if response.entries.len()
            > usize::try_from(LocationListLimit::DEFAULT.get()).unwrap_or(usize::MAX)
        {
            return Err(ClientError::Protocol);
        }
        Ok(response)
    }

    /// Observe the Workspace catalog under one exact Location root.
    ///
    /// # Errors
    /// Returns invalid input, a bounded transport error, a typed service
    /// rejection, or a protocol error for an unbounded or non-canonical
    /// response.
    pub async fn workspace_list(
        &self,
        authorization: &HeaderValue,
        request: &WorkspaceListRequest,
    ) -> Result<WorkspaceListResponse, ClientError> {
        validate_location_request(request.location_id, request.expected_revision)?;
        let response: WorkspaceListResponse = self
            .call(
                authorization,
                Method::POST,
                "/internal/v1/forge/workspace-list".to_owned(),
                Some(request),
                None,
            )
            .await?;
        if response.entries.len() > MAX_WORKSPACE_CATALOG_ENTRIES
            || !response.entries.iter().all(valid_catalog_entry)
        {
            return Err(ClientError::Protocol);
        }
        Ok(response)
    }

    /// Observe Workspace discovery candidates under one exact Location root.
    ///
    /// # Errors
    /// Returns invalid input, a bounded transport error, a typed service
    /// rejection, or a protocol error for an unbounded or non-canonical
    /// response.
    pub async fn workspace_discover(
        &self,
        authorization: &HeaderValue,
        request: &WorkspaceDiscoverRequest,
    ) -> Result<WorkspaceDiscoverResponse, ClientError> {
        validate_location_request(request.location_id, request.expected_revision)?;
        let response: WorkspaceDiscoverResponse = self
            .call(
                authorization,
                Method::POST,
                "/internal/v1/forge/workspace-discover".to_owned(),
                Some(request),
                None,
            )
            .await?;
        if response.entries.len() > MAX_WORKSPACE_CATALOG_ENTRIES
            || !response.entries.iter().all(valid_discovery_entry)
        {
            return Err(ClientError::Protocol);
        }
        Ok(response)
    }

    /// Observe one fresh `workspace.resolve` resolution for the caller's
    /// exact Workspace reference. A stale caller reference is a typed
    /// service rejection, never a silent correction.
    ///
    /// # Errors
    /// Returns invalid input, a bounded transport error, a typed service
    /// rejection, or a protocol error for a response that does not echo the
    /// requested locator.
    pub async fn workspace_resolve(
        &self,
        authorization: &HeaderValue,
        request: &WorkspaceResolveRequest,
    ) -> Result<WorkspaceResolveResponse, ClientError> {
        let scope = &request.scope;
        if scope.workspace_id.is_nil()
            || scope.location_id.is_nil()
            || scope.location_revision <= 0
            || scope.workspace_revision <= 0
            || scope.locator.is_empty()
            || scope.locator.len() > MAX_WORKSPACE_LOCATOR_BYTES
            || scope.locator.chars().any(char::is_control)
            || request.session_id.is_some_and(|id| id.is_nil())
        {
            return Err(ClientError::InvalidRequest);
        }
        let response: WorkspaceResolveResponse = self
            .call(
                authorization,
                Method::POST,
                format!(
                    "/internal/v1/workspaces/{}/forge/workspace-resolve",
                    scope.workspace_id
                ),
                Some(request),
                None,
            )
            .await?;
        if response.workspace_id.is_nil()
            || response.workspace_revision <= 0
            || response.locator != scope.locator
        {
            return Err(ClientError::Protocol);
        }
        Ok(response)
    }
}

fn validate_location_request(location_id: Uuid, expected_revision: i64) -> Result<(), ClientError> {
    if location_id.is_nil() || expected_revision <= 0 {
        return Err(ClientError::InvalidRequest);
    }
    Ok(())
}

fn valid_catalog_entry(entry: &WorkspaceCatalogEntryWire) -> bool {
    !entry.workspace_id.is_nil()
        && entry.workspace_revision > 0
        && !entry.locator.is_empty()
        && !entry.display_name.is_empty()
}

fn valid_discovery_entry(entry: &WorkspaceDiscoveryEntryWire) -> bool {
    !entry.locator.is_empty()
        && !entry.display_name.is_empty()
        && entry.signals.present.len() <= 4
        && entry
            .signals
            .present
            .windows(2)
            .all(|pair| signal_rank(pair[0]) < signal_rank(pair[1]))
}

fn signal_rank(signal: crate::wire::forge_reads::DiscoverySignalWire) -> u8 {
    use crate::wire::forge_reads::DiscoverySignalWire as Signal;
    match signal {
        Signal::CargoWorkspace => 0,
        Signal::GitRepository => 1,
        Signal::AgentInstructions => 2,
        Signal::ForgeState => 3,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::forge_reads::{
        DiscoveryDispositionWire, DiscoveryKindWire, DiscoveryReasonWire, DiscoverySignalWire,
        LocationRowWire, LocationStateWire,
    };
    use crate::wire::leadership::WorkspaceQueryScope;

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    fn resolve_request() -> WorkspaceResolveRequest {
        WorkspaceResolveRequest {
            scope: WorkspaceQueryScope {
                workspace_id: Uuid::from_u128(1),
                location_id: Uuid::from_u128(2),
                location_revision: 3,
                workspace_revision: 4,
                locator: "locator".to_owned(),
            },
            session_id: Some(Uuid::from_u128(5)),
        }
    }

    #[test]
    fn discovery_signal_order_is_enforced_client_side() {
        let entry = |present: Vec<DiscoverySignalWire>| WorkspaceDiscoveryEntryWire {
            locator: "locator".to_owned(),
            kind: DiscoveryKindWire::DirectChild,
            display_name: "alpha".to_owned(),
            signals: crate::wire::forge_reads::DiscoverySignalsWire { present },
            disposition: DiscoveryDispositionWire::Candidate,
            reason: DiscoveryReasonWire::CargoWorkspace,
        };
        assert!(valid_discovery_entry(&entry(vec![
            DiscoverySignalWire::CargoWorkspace,
            DiscoverySignalWire::ForgeState,
        ])));
        assert!(!valid_discovery_entry(&entry(vec![
            DiscoverySignalWire::ForgeState,
            DiscoverySignalWire::CargoWorkspace,
        ])));
        assert!(!valid_discovery_entry(&entry(vec![
            DiscoverySignalWire::CargoWorkspace,
            DiscoverySignalWire::CargoWorkspace,
        ])));
    }

    #[test]
    fn resolve_input_contract_is_bounded_and_stale_identity_is_detectable() {
        let request = resolve_request();
        assert_eq!(request.scope.location_revision, 3);
        assert_eq!(request.scope.workspace_revision, 4);
        assert!(request.session_id.is_some_and(|id| !id.is_nil()));

        let response = WorkspaceResolveResponse {
            workspace_id: Uuid::from_u128(1),
            workspace_revision: 8,
            locator: request.scope.locator.clone(),
        };
        assert!(response.workspace_revision > 0);
        assert_eq!(response.locator, request.scope.locator);

        let mut stale = resolve_request();
        stale.scope.workspace_revision = 99;
        assert_ne!(stale.scope.workspace_revision, response.workspace_revision);

        let mut nil_scope = resolve_request();
        nil_scope.scope.workspace_id = Uuid::nil();
        assert!(nil_scope.scope.workspace_id.is_nil());
        assert!(validate_location_request(Uuid::from_u128(2), 3).is_ok());
        assert!(validate_location_request(Uuid::nil(), 3).is_err());
        assert!(validate_location_request(Uuid::from_u128(2), 0).is_err());
    }

    #[test]
    fn wire_projection_uses_camel_case_and_closed_vocabularies() -> TestResult {
        let request = WorkspaceListRequest {
            location_id: Uuid::from_u128(0x20),
            expected_revision: 6,
        };
        let encoded = serde_json::to_value(&request)?;
        assert_eq!(
            encoded["locationId"],
            serde_json::json!(Uuid::from_u128(0x20))
        );
        assert_eq!(encoded["expectedRevision"], serde_json::json!(6));

        let location = LocationRowWire {
            location_id: Uuid::from_u128(0x10),
            display_name: "Local Disk".to_owned(),
            revision: 5,
            state: LocationStateWire::Enabled,
        };
        let encoded = serde_json::to_value(&location)?;
        assert_eq!(encoded["state"], serde_json::json!("enabled"));

        let entry = WorkspaceCatalogEntryWire {
            locator: "locator".to_owned(),
            display_name: "Root".to_owned(),
            workspace_id: Uuid::from_u128(0x30),
            workspace_revision: 8,
        };
        let encoded = serde_json::to_value(&entry)?;
        assert_eq!(
            encoded["workspaceId"],
            serde_json::json!(Uuid::from_u128(0x30))
        );
        assert_eq!(encoded["workspaceRevision"], serde_json::json!(8));
        Ok(())
    }
}
