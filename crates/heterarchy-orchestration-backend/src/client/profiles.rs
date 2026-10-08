//! Official bounded client for global Profile reads and mutations.

use std::collections::{HashMap, HashSet};

use heterarchy_orchestration_sessions::ProfileConfiguration;
use hyper::Method;
use hyper::header::HeaderValue;
use uuid::Uuid;

use crate::wire::AgentProfileResponse;
use crate::wire::WorkspaceQueryScope;
use crate::wire::profiles::{
    ProfileLifecycleRequest, ProfileMutationResponse, ProfileUpdateRequest, WorkspaceProfileWire,
    WorkspaceProfilesQueryRequest, WorkspaceProfilesQueryResponse,
};

use super::{
    ClientError, OrchestrationHttpClient, ReadbackTarget, canonical_identity, require_identity,
    valid_profile,
};

const MAX_PROFILE_LIST_LIMIT: u32 = 64;
const MAX_WORKSPACE_PROFILES: usize = 256;
const MAX_SESSION_MAPPINGS_PER_PROFILE: usize = 65;
const MAX_LOCATOR_BYTES: usize = 4_096;

impl OrchestrationHttpClient {
    /// Read the bounded source-fenced Profile projection for one Workspace.
    /// Session selectors authorize the source query but do not filter returned
    /// Session-to-Profile mappings.
    ///
    /// # Errors
    /// Returns invalid scope input, typed service rejection, bounded transport
    /// failures, or a response violating mapping/state bounds.
    pub async fn workspace_profiles(
        &self,
        authorization: &HeaderValue,
        request: &WorkspaceProfilesQueryRequest,
    ) -> Result<WorkspaceProfilesQueryResponse, ClientError> {
        validate_workspace_profiles_request(request)?;
        let path = format!(
            "/internal/v1/workspaces/{}/profiles/query",
            request.scope.workspace_id
        );
        let response: WorkspaceProfilesQueryResponse = self
            .call(authorization, Method::POST, path, Some(request), None)
            .await?;
        if !valid_workspace_profiles_response(&response, request) {
            return Err(ClientError::Protocol);
        }
        Ok(response)
    }

    /// Read a bounded newest-first Profile page.
    ///
    /// # Errors
    /// Returns invalid bounds, typed service rejection, bounded transport
    /// failures, or a response violating Profile identity/cardinality rules.
    pub async fn profile_list(
        &self,
        authorization: &HeaderValue,
        limit: u32,
    ) -> Result<Vec<AgentProfileResponse>, ClientError> {
        if !(1..=MAX_PROFILE_LIST_LIMIT).contains(&limit) {
            return Err(ClientError::InvalidRequest);
        }
        let response: Vec<AgentProfileResponse> = self
            .call::<_, ()>(
                authorization,
                Method::GET,
                format!("/internal/v1/agent-profiles?limit={limit}"),
                None,
                None,
            )
            .await?;
        if !valid_profile_list(&response, limit) {
            return Err(ClientError::Protocol);
        }
        Ok(response)
    }

    /// Apply one exact Profile configuration mutation.
    ///
    /// # Errors
    /// Returns invalid request/configuration input, typed rejection, or an
    /// uncertain mutation outcome carrying the stable request nonce.
    pub async fn update_profile(
        &self,
        authorization: &HeaderValue,
        profile_id: Uuid,
        request: &ProfileUpdateRequest,
    ) -> Result<ProfileMutationResponse, ClientError> {
        require_identity(profile_id)?;
        let nonce = validate_update_request(request)?;
        let configuration = profile_configuration(request)?;
        let expected_revision = positive_revision(request.expected_revision)?;
        let readback = ReadbackTarget::ProfileRequest(nonce);
        let response: ProfileMutationResponse = self
            .call(
                authorization,
                Method::PUT,
                format!("/internal/v1/agent-profiles/{profile_id}/configuration"),
                Some(request),
                Some(readback.clone()),
            )
            .await?;
        let next_revision = expected_revision
            .checked_add(1)
            .ok_or(ClientError::InvalidRequest)?;
        if !valid_update_response(
            &response,
            nonce,
            profile_id,
            expected_revision,
            next_revision,
            &configuration,
        ) {
            return Err(ClientError::UnknownOutcome { readback });
        }
        Ok(response)
    }

    /// Disable one Profile through the canonical mutation receipt owner.
    ///
    /// # Errors
    /// Returns invalid request input, typed rejection, or an uncertain outcome
    /// carrying the stable request nonce.
    pub async fn disable_profile(
        &self,
        authorization: &HeaderValue,
        profile_id: Uuid,
        request: &ProfileLifecycleRequest,
    ) -> Result<ProfileMutationResponse, ClientError> {
        self.lifecycle_profile(authorization, profile_id, request, "disable", "disabled")
            .await
    }

    /// Retire one Profile through the canonical mutation receipt owner.
    ///
    /// # Errors
    /// Returns invalid request input, typed rejection, or an uncertain outcome
    /// carrying the stable request nonce.
    pub async fn retire_profile(
        &self,
        authorization: &HeaderValue,
        profile_id: Uuid,
        request: &ProfileLifecycleRequest,
    ) -> Result<ProfileMutationResponse, ClientError> {
        self.lifecycle_profile(authorization, profile_id, request, "retire", "retired")
            .await
    }

    async fn lifecycle_profile(
        &self,
        authorization: &HeaderValue,
        profile_id: Uuid,
        request: &ProfileLifecycleRequest,
        kind: &str,
        lifecycle_state: &str,
    ) -> Result<ProfileMutationResponse, ClientError> {
        require_identity(profile_id)?;
        let nonce = validate_lifecycle_request(request)?;
        let expected_revision = positive_revision(request.expected_revision)?;
        let readback = ReadbackTarget::ProfileRequest(nonce);
        let response: ProfileMutationResponse = self
            .call(
                authorization,
                Method::POST,
                format!("/internal/v1/agent-profiles/{profile_id}/{kind}"),
                Some(request),
                Some(readback.clone()),
            )
            .await?;
        if !valid_lifecycle_response(
            &response,
            nonce,
            profile_id,
            expected_revision,
            kind,
            lifecycle_state,
        ) {
            return Err(ClientError::UnknownOutcome { readback });
        }
        Ok(response)
    }

    /// Read one immutable Profile mutation receipt by its exact nonce.
    ///
    /// # Errors
    /// Returns typed rejection, bounded transport failure, or a malformed
    /// receipt protocol error.
    pub async fn profile_mutation_receipt(
        &self,
        authorization: &HeaderValue,
        request_id: Uuid,
    ) -> Result<ProfileMutationResponse, ClientError> {
        require_identity(request_id)?;
        let response: ProfileMutationResponse = self
            .call::<_, ()>(
                authorization,
                Method::GET,
                format!("/internal/v1/profile-mutations/{request_id}"),
                None,
                None,
            )
            .await?;
        if !valid_profile_receipt(&response, request_id) {
            return Err(ClientError::Protocol);
        }
        Ok(response)
    }
}

fn validate_workspace_profiles_request(
    request: &WorkspaceProfilesQueryRequest,
) -> Result<(), ClientError> {
    validate_workspace_scope(&request.scope)?;
    if request.session_id.is_some_and(|id| id.is_nil()) {
        return Err(ClientError::InvalidRequest);
    }
    Ok(())
}

fn validate_workspace_scope(scope: &WorkspaceQueryScope) -> Result<(), ClientError> {
    if scope.workspace_id.is_nil()
        || scope.location_id.is_nil()
        || scope.location_revision <= 0
        || scope.workspace_revision <= 0
        || scope.locator.is_empty()
        || scope.locator.len() > MAX_LOCATOR_BYTES
        || scope.locator.chars().any(char::is_control)
    {
        return Err(ClientError::InvalidRequest);
    }
    Ok(())
}

fn valid_workspace_profiles_response(
    response: &WorkspaceProfilesQueryResponse,
    request: &WorkspaceProfilesQueryRequest,
) -> bool {
    if response.scope != request.scope
        || response.profiles.len() > MAX_WORKSPACE_PROFILES
        || response.session_profiles.len()
            > response
                .profiles
                .len()
                .saturating_mul(MAX_SESSION_MAPPINGS_PER_PROFILE)
    {
        return false;
    }
    let mut profile_ids = HashSet::with_capacity(response.profiles.len());
    if !response
        .profiles
        .iter()
        .all(|item| valid_workspace_profile(item) && profile_ids.insert(item.profile.profile_id))
    {
        return false;
    }
    let mut session_ids = HashSet::with_capacity(response.session_profiles.len());
    let mut profile_mapping_counts = HashMap::with_capacity(response.profiles.len());
    response.session_profiles.iter().all(|mapping| {
        let count = profile_mapping_counts
            .entry(mapping.profile_id)
            .or_insert(0_usize);
        *count += 1;
        !mapping.session_id.is_nil()
            && !mapping.profile_id.is_nil()
            && profile_ids.contains(&mapping.profile_id)
            && session_ids.insert(mapping.session_id)
            && *count <= MAX_SESSION_MAPPINGS_PER_PROFILE
    })
}

fn valid_workspace_profile(value: &WorkspaceProfileWire) -> bool {
    valid_profile_snapshot(&value.profile)
        && matches!(
            value.state.as_str(),
            "busy" | "online" | "offline" | "error" | "retired"
        )
}

fn validate_update_request(request: &ProfileUpdateRequest) -> Result<Uuid, ClientError> {
    let nonce = canonical_identity(&request.request_id)?;
    let _ = profile_configuration(request)?;
    positive_revision(request.expected_revision)?;
    Ok(nonce)
}

fn validate_lifecycle_request(request: &ProfileLifecycleRequest) -> Result<Uuid, ClientError> {
    let nonce = canonical_identity(&request.request_id)?;
    positive_revision(request.expected_revision)?;
    Ok(nonce)
}

fn positive_revision(value: i64) -> Result<i64, ClientError> {
    if value > 0 {
        Ok(value)
    } else {
        Err(ClientError::InvalidRequest)
    }
}

fn profile_configuration(
    request: &ProfileUpdateRequest,
) -> Result<ProfileConfiguration, ClientError> {
    ProfileConfiguration::new(
        request.display_name.clone(),
        request.preferred_model.clone(),
        request.preferred_reasoning.clone(),
        request.base_capabilities.clone(),
    )
    .map_err(|_| ClientError::InvalidRequest)
}

fn valid_profile_list(values: &[AgentProfileResponse], limit: u32) -> bool {
    let Ok(limit) = usize::try_from(limit) else {
        return false;
    };
    if values.len() > limit {
        return false;
    }
    let mut profile_ids = HashSet::with_capacity(values.len());
    let mut request_ids = HashSet::with_capacity(values.len());
    values.iter().all(|profile| {
        valid_profile_snapshot(profile)
            && profile_ids.insert(profile.profile_id)
            && request_ids.insert(profile.request_id)
    })
}

pub(super) fn valid_profile_snapshot(profile: &AgentProfileResponse) -> bool {
    valid_profile(profile)
        && profile.created_at_ms >= 0
        && profile.updated_at_ms >= profile.created_at_ms
        && profile.last_used_at_ms.is_none_or(|value| value >= 0)
}

fn valid_mutation_identity(
    response: &ProfileMutationResponse,
    request_id: Uuid,
    profile_id: Uuid,
    expected_revision: i64,
    kind: &str,
) -> bool {
    response.request_id == request_id
        && response.expected_revision == Some(expected_revision)
        && response.kind == kind
        && response.profile.profile_id == profile_id
        && valid_profile_snapshot(&response.profile)
}

fn valid_update_response(
    response: &ProfileMutationResponse,
    request_id: Uuid,
    profile_id: Uuid,
    expected_revision: i64,
    next_revision: i64,
    configuration: &ProfileConfiguration,
) -> bool {
    valid_mutation_identity(
        response,
        request_id,
        profile_id,
        expected_revision,
        "update",
    ) && response.profile.revision == next_revision
        && response.profile.display_name == configuration.display_name()
        && response.profile.preferred_model.as_deref() == configuration.preferred_model()
        && response.profile.preferred_reasoning.as_deref() == configuration.preferred_reasoning()
        && response.profile.base_capabilities == configuration.capabilities()
}

fn valid_lifecycle_response(
    response: &ProfileMutationResponse,
    request_id: Uuid,
    profile_id: Uuid,
    expected_revision: i64,
    kind: &str,
    lifecycle_state: &str,
) -> bool {
    let next_revision = expected_revision.checked_add(1);
    valid_mutation_identity(response, request_id, profile_id, expected_revision, kind)
        && response.profile.lifecycle_state == lifecycle_state
        && (response.profile.revision == expected_revision
            || next_revision.is_some_and(|revision| response.profile.revision == revision))
}

fn valid_profile_receipt(response: &ProfileMutationResponse, request_id: Uuid) -> bool {
    if response.replayed
        || response.request_id != request_id
        || !valid_profile_snapshot(&response.profile)
    {
        return false;
    }
    match response.kind.as_str() {
        "create" => {
            response.expected_revision.is_none()
                && response.profile.request_id == request_id
                && response.profile.lifecycle_state == "active"
                && response.profile.revision == 1
        }
        "update" => response.expected_revision.is_some_and(|revision| {
            revision > 0
                && revision.checked_add(1) == Some(response.profile.revision)
                && response.profile.lifecycle_state == "active"
        }),
        "disable" => valid_lifecycle_receipt(response, "disabled"),
        "retire" => valid_lifecycle_receipt(response, "retired"),
        _ => false,
    }
}

fn valid_lifecycle_receipt(response: &ProfileMutationResponse, lifecycle_state: &str) -> bool {
    let Some(expected_revision) = response.expected_revision else {
        return false;
    };
    expected_revision > 0
        && response.profile.lifecycle_state == lifecycle_state
        && (response.profile.revision == expected_revision
            || expected_revision
                .checked_add(1)
                .is_some_and(|revision| response.profile.revision == revision))
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use hyper::header::HeaderValue;

    use super::*;
    use crate::wire::profiles::WorkspaceSessionProfileWire;

    fn profile(lifecycle_state: &str, revision: i64) -> AgentProfileResponse {
        AgentProfileResponse {
            profile_id: Uuid::from_u128(1),
            request_id: Uuid::from_u128(2),
            stable_name: "stable".to_owned(),
            display_name: "Display".to_owned(),
            provider_family: "chatgpt".to_owned(),
            preferred_model: Some("gpt-5.6-luna".to_owned()),
            preferred_reasoning: Some("max".to_owned()),
            base_capabilities: vec!["backend".to_owned(), "rust".to_owned()],
            lifecycle_state: lifecycle_state.to_owned(),
            creation_origin: "user_manual".to_owned(),
            created_at_ms: 1,
            updated_at_ms: 2,
            last_used_at_ms: None,
            revision,
        }
    }

    fn workspace_scope() -> WorkspaceQueryScope {
        WorkspaceQueryScope {
            workspace_id: Uuid::from_u128(10),
            location_id: Uuid::from_u128(11),
            location_revision: 1,
            workspace_revision: 2,
            locator: "workspace".to_owned(),
        }
    }

    fn lifecycle_request() -> ProfileLifecycleRequest {
        ProfileLifecycleRequest {
            request_id: Uuid::from_u128(3).to_string(),
            expected_revision: 2,
        }
    }

    #[test]
    fn profile_list_rejects_duplicate_or_invalid_snapshots() {
        let value = profile("active", 1);
        assert!(valid_profile_list(std::slice::from_ref(&value), 1));
        assert!(!valid_profile_list(&[value.clone(), value], 2));

        let mut invalid = profile("active", 1);
        invalid.updated_at_ms = 0;
        assert!(!valid_profile_list(&[invalid], 1));
    }

    #[test]
    fn workspace_projection_binds_mappings_without_filtering_auth_selector() {
        let request = WorkspaceProfilesQueryRequest {
            scope: workspace_scope(),
            session_id: Some(Uuid::from_u128(99)),
        };
        let response = WorkspaceProfilesQueryResponse {
            scope: request.scope.clone(),
            profiles: vec![WorkspaceProfileWire {
                profile: profile("active", 1),
                state: "online".to_owned(),
            }],
            session_profiles: vec![WorkspaceSessionProfileWire {
                session_id: Uuid::from_u128(100),
                profile_id: Uuid::from_u128(1),
            }],
        };
        assert!(valid_workspace_profiles_response(&response, &request));

        let mut foreign = response.clone();
        foreign.session_profiles[0].profile_id = Uuid::from_u128(101);
        assert!(!valid_workspace_profiles_response(&foreign, &request));

        let mut duplicate = response.clone();
        duplicate
            .session_profiles
            .push(WorkspaceSessionProfileWire {
                session_id: Uuid::from_u128(100),
                profile_id: Uuid::from_u128(1),
            });
        assert!(!valid_workspace_profiles_response(&duplicate, &request));

        let mut invalid_state = response;
        invalid_state.profiles[0].state = "unknown".to_owned();
        assert!(!valid_workspace_profiles_response(&invalid_state, &request));
    }

    #[test]
    fn workspace_projection_accepts_owner_bounded_mapping_cardinality() {
        let request = WorkspaceProfilesQueryRequest {
            scope: workspace_scope(),
            session_id: None,
        };
        let profiles: Vec<WorkspaceProfileWire> = (0_u128..5)
            .map(|index| {
                let mut value = profile("active", 1);
                value.profile_id = Uuid::from_u128(index + 1);
                value.request_id = Uuid::from_u128(index + 10);
                WorkspaceProfileWire {
                    profile: value,
                    state: "online".to_owned(),
                }
            })
            .collect();
        let session_profiles: Vec<WorkspaceSessionProfileWire> = (0_u128..5)
            .flat_map(|profile_index| {
                (0_u128..65).map(move |session_index| WorkspaceSessionProfileWire {
                    session_id: Uuid::from_u128(1000 + profile_index * 65 + session_index),
                    profile_id: Uuid::from_u128(profile_index + 1),
                })
            })
            .collect();
        let response = WorkspaceProfilesQueryResponse {
            scope: request.scope.clone(),
            profiles,
            session_profiles,
        };
        assert!(valid_workspace_profiles_response(&response, &request));

        let mut over_profile = response.clone();
        over_profile
            .session_profiles
            .retain(|mapping| mapping.session_id != Uuid::from_u128(1_129));
        over_profile
            .session_profiles
            .push(WorkspaceSessionProfileWire {
                session_id: Uuid::from_u128(99_999),
                profile_id: Uuid::from_u128(1),
            });
        assert!(!valid_workspace_profiles_response(&over_profile, &request));
    }

    #[test]
    fn workspace_scope_reuses_bounded_identity_revision_and_locator_rules() {
        let request = WorkspaceProfilesQueryRequest {
            scope: workspace_scope(),
            session_id: None,
        };
        assert!(validate_workspace_profiles_request(&request).is_ok());

        let mut invalid = request.clone();
        invalid.scope.locator = "bad\nlocator".to_owned();
        assert_eq!(
            validate_workspace_profiles_request(&invalid),
            Err(ClientError::InvalidRequest)
        );
        invalid = request;
        invalid.scope.workspace_revision = 0;
        assert_eq!(
            validate_workspace_profiles_request(&invalid),
            Err(ClientError::InvalidRequest)
        );
    }

    #[test]
    fn lifecycle_noop_accepts_same_revision_and_fresh_transition() {
        let request_id = Uuid::from_u128(3);
        let profile_id = Uuid::from_u128(1);
        let same_revision = ProfileMutationResponse {
            request_id,
            expected_revision: Some(2),
            kind: "disable".to_owned(),
            profile: profile("disabled", 2),
            replayed: false,
        };
        assert!(valid_lifecycle_response(
            &same_revision,
            request_id,
            profile_id,
            2,
            "disable",
            "disabled"
        ));
        let fresh = ProfileMutationResponse {
            profile: profile("disabled", 3),
            ..same_revision
        };
        assert!(valid_lifecycle_response(
            &fresh, request_id, profile_id, 2, "disable", "disabled"
        ));
    }

    #[test]
    fn update_reuses_profile_configuration_and_nonce_validation() {
        let request = ProfileUpdateRequest {
            request_id: Uuid::from_u128(3).to_string(),
            expected_revision: 1,
            display_name: "Display".to_owned(),
            preferred_model: Some("gpt-5.6-luna".to_owned()),
            preferred_reasoning: Some("max".to_owned()),
            base_capabilities: vec!["backend".to_owned(), "rust".to_owned()],
        };
        assert!(validate_update_request(&request).is_ok());

        let mut duplicate = request.clone();
        duplicate.base_capabilities.push("rust".to_owned());
        assert_eq!(
            validate_update_request(&duplicate),
            Err(ClientError::InvalidRequest)
        );

        let mut noncanonical = request;
        noncanonical.request_id = Uuid::from_u128(0xA).to_string().to_uppercase();
        assert_eq!(
            validate_update_request(&noncanonical),
            Err(ClientError::InvalidRequest)
        );
        assert!(validate_lifecycle_request(&lifecycle_request()).is_ok());
    }

    #[test]
    fn receipt_requires_nonce_kind_and_lifecycle_consistency() {
        let request_id = Uuid::from_u128(3);
        let valid = ProfileMutationResponse {
            request_id,
            expected_revision: Some(2),
            kind: "retire".to_owned(),
            profile: profile("retired", 3),
            replayed: false,
        };
        assert!(valid_profile_receipt(&valid, request_id));
        let mut invalid = valid.clone();
        invalid.kind = "disable".to_owned();
        assert!(!valid_profile_receipt(&invalid, request_id));
        let mut invalid_revision = valid.clone();
        invalid_revision.profile.revision = 99;
        assert!(!valid_profile_receipt(&invalid_revision, request_id));
        let mut replayed = valid;
        replayed.replayed = true;
        assert!(!valid_profile_receipt(&replayed, request_id));
    }

    #[tokio::test]
    async fn lost_update_keeps_profile_nonce_readback() -> Result<(), Box<dyn std::error::Error>> {
        let client =
            OrchestrationHttpClient::new("127.0.0.1:1".parse()?, Duration::from_millis(50))?;
        let request = ProfileUpdateRequest {
            request_id: Uuid::from_u128(3).to_string(),
            expected_revision: 1,
            display_name: "Display".to_owned(),
            preferred_model: Some("gpt-5.6-luna".to_owned()),
            preferred_reasoning: Some("max".to_owned()),
            base_capabilities: vec!["backend".to_owned(), "rust".to_owned()],
        };
        assert_eq!(
            client
                .update_profile(
                    &HeaderValue::from_static("Bearer fixture"),
                    Uuid::from_u128(1),
                    &request,
                )
                .await
                .err(),
            Some(ClientError::UnknownOutcome {
                readback: ReadbackTarget::ProfileRequest(Uuid::from_u128(3))
            })
        );
        Ok(())
    }
}
