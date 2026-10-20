//! Typed observation and mutation adapter for Forge's binding-lifecycle HTTP
//! commands. Returned Operations observations are data readbacks, never Forge
//! effect capabilities.

use forge_backend::ops::{CommandOp, ScopeDto};
use forge_backend::{ForgeHttpClientError, ForgeHttpServiceCode};
use forge_control::ResolvedWorkspaceScope;
use forge_domain::{
    ActivityId, AgentBindingRejectionReason, AgentDeclaredRole, AgentDisplayName, AgentInstanceId,
    AgentSessionId, AgentTransportKind, AuthenticatedPrincipal, DirectTokenId, OAuthGrantId,
    TimestampMillis, WorkspaceId, WorkspaceRootFingerprint,
};
use heterarchy_orchestration_operations::{
    AgentBindingActivationRequest, AgentBindingReservationRequest, BindingActivationObservation,
    BindingRejectionObservation, BindingReservationObservation,
};
use serde::{Deserialize, Deserializer};
use serde_json::{Value, json};
use uuid::Uuid;

use crate::forge_commands::ForgeCommandHttpClient;

use super::forge_source::{self, ForgeSourceError};

/// Exact trusted Orchestration references selecting an existing Forge freeze
/// and its configured issuer. This is correlation data, not a Forge grant.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MembershipSourceReference {
    issuer_id: Uuid,
    destination_id: Uuid,
    migration_id: Uuid,
    freeze_revision: u64,
    manifest_digest: [u8; 32],
}

impl MembershipSourceReference {
    /// Construct a source reference from already-verified Operations metadata.
    ///
    /// # Errors
    /// Rejects nil identities, an unrepresentable freeze revision, a zero
    /// revision, or the zero manifest digest.
    pub fn new(
        issuer_id: Uuid,
        destination_id: Uuid,
        migration_id: Uuid,
        freeze_revision: u64,
        manifest_digest: [u8; 32],
    ) -> Result<Self, ForgeSourceError> {
        if issuer_id.is_nil()
            || destination_id.is_nil()
            || migration_id.is_nil()
            || freeze_revision == 0
            || i64::try_from(freeze_revision).is_err()
            || manifest_digest == [0; 32]
        {
            return Err(ForgeSourceError::InvalidScope);
        }
        Ok(Self {
            issuer_id,
            destination_id,
            migration_id,
            freeze_revision,
            manifest_digest,
        })
    }

    #[must_use]
    pub const fn issuer_id(self) -> Uuid {
        self.issuer_id
    }

    #[must_use]
    pub const fn destination_id(self) -> Uuid {
        self.destination_id
    }

    #[must_use]
    pub const fn migration_id(self) -> Uuid {
        self.migration_id
    }

    #[must_use]
    pub const fn freeze_revision(self) -> u64 {
        self.freeze_revision
    }

    #[must_use]
    pub const fn manifest_digest(self) -> [u8; 32] {
        self.manifest_digest
    }

    fn freeze_revision_i64(self) -> Result<i64, ForgeSourceError> {
        i64::try_from(self.freeze_revision).map_err(|_| ForgeSourceError::InvalidScope)
    }
}

/// Read-only projection of one delegated, frozen Forge source.
///
/// The freeze, manifest, and timestamps are validated against the exact
/// request reference before this value is returned. It contains no actor or
/// permission to mutate Forge state.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BindingDelegationObservation {
    workspace_id: WorkspaceId,
    migration_id: Uuid,
    destination_id: Uuid,
    freeze_revision: u64,
    manifest_digest: [u8; 32],
    freeze_created_at: TimestampMillis,
    freeze_updated_at: TimestampMillis,
    delegated_at: TimestampMillis,
}

impl BindingDelegationObservation {
    #[must_use]
    pub const fn workspace_id(self) -> WorkspaceId {
        self.workspace_id
    }

    #[must_use]
    pub const fn migration_id(self) -> Uuid {
        self.migration_id
    }

    #[must_use]
    pub const fn destination_id(self) -> Uuid {
        self.destination_id
    }

    #[must_use]
    pub const fn freeze_revision(self) -> u64 {
        self.freeze_revision
    }

    #[must_use]
    pub const fn manifest_digest(self) -> [u8; 32] {
        self.manifest_digest
    }

    #[must_use]
    pub const fn freeze_created_at(self) -> TimestampMillis {
        self.freeze_created_at
    }

    #[must_use]
    pub const fn freeze_updated_at(self) -> TimestampMillis {
        self.freeze_updated_at
    }

    #[must_use]
    pub const fn delegated_at(self) -> TimestampMillis {
        self.delegated_at
    }
}

/// Bounded typed HTTP client for the existing Forge binding-lifecycle command
/// family. Each method issues one command and returns its exact readback; this
/// adapter does not retry uncertain mutations.
#[must_use]
#[derive(Clone, Debug)]
pub struct ForgeMembershipHttpClient {
    commands: ForgeCommandHttpClient,
}

impl ForgeMembershipHttpClient {
    /// Reuse the host-configured, bounded Forge command transport.
    pub fn new(commands: ForgeCommandHttpClient) -> Self {
        Self { commands }
    }

    /// Read the exact current delegation for one trusted source reference.
    ///
    /// # Errors
    /// Rejects invalid identities, unavailable authority or inconsistent source readback.
    /// No command is retried automatically.
    pub async fn delegation(
        &self,
        scope: &ResolvedWorkspaceScope,
        reference: &MembershipSourceReference,
    ) -> Result<Option<BindingDelegationObservation>, ForgeSourceError> {
        let (wire_scope, _) = validated_request_scope(scope, *reference)?;
        let payload = json!({
            "scope": &wire_scope,
            "issuer_id": reference.issuer_id,
            "destination_id": reference.destination_id,
            "reference": {
                "migration_id": reference.migration_id,
                "expected_freeze_revision": reference.freeze_revision_i64()?,
                "manifest_digest": encode_digest(reference.manifest_digest),
            },
        });
        let result = self
            .commands
            .command(CommandOp::AgentOperationsDelegationGet, payload)
            .await
            .map_err(forge_source::map_command_error)?;
        parse_delegation_readback(result, &wire_scope, *reference)
    }

    /// Reserve one binding against the exact trusted source freeze.
    ///
    /// # Errors
    /// Rejects invalid identities, unavailable authority or inconsistent source readback.
    /// No command is retried automatically.
    pub async fn reserve(
        &self,
        scope: &ResolvedWorkspaceScope,
        reference: &MembershipSourceReference,
        reservation: &AgentBindingReservationRequest,
        observed_at: i64,
    ) -> Result<BindingReservationObservation, ForgeSourceError> {
        let (wire_scope, _) = validated_request_scope(scope, *reference)?;
        let observed_at = timestamp(observed_at, ForgeSourceError::InvalidScope)?;
        let payload = json!({
            "scope": &wire_scope,
            "issuer_id": reference.issuer_id,
            "destination_id": reference.destination_id,
            "reference": {
                "migration_id": reference.migration_id,
                "expected_freeze_revision": reference.freeze_revision_i64()?,
            },
            "reservation": reservation_json(reservation),
            "observed_at_ms": observed_at.get(),
        });
        let result = self
            .commands
            .command(CommandOp::AgentBindingReserve, payload)
            .await
            .map_err(forge_source::map_command_error)?;
        parse_reservation_readback(result, &wire_scope, *reference, Some(reservation))?
            .ok_or(ForgeSourceError::Protocol)
    }

    /// Read one reservation by the exact request identity and source tuple.
    ///
    /// # Errors
    /// Rejects invalid identities, unavailable authority or inconsistent source readback.
    /// No command is retried automatically.
    pub async fn reservation(
        &self,
        scope: &ResolvedWorkspaceScope,
        reference: &MembershipSourceReference,
        request: &AgentBindingReservationRequest,
    ) -> Result<Option<BindingReservationObservation>, ForgeSourceError> {
        let (wire_scope, _) = validated_request_scope(scope, *reference)?;
        let payload = json!({
            "scope": &wire_scope,
            "issuer_id": reference.issuer_id,
            "destination_id": reference.destination_id,
            "request_id": request.request_id(),
            "session_id": request.session_id().into_uuid(),
            "principal": principal_json(request.principal()),
        });
        let result = self
            .commands
            .command(CommandOp::AgentBindingReservationGet, payload)
            .await
            .map_err(forge_source::map_command_error)?;
        parse_reservation_readback(result, &wire_scope, *reference, Some(request))
    }

    /// Activate one committed binding through Forge's reference-based command.
    ///
    /// # Errors
    /// Rejects invalid identities, unavailable authority or inconsistent source readback.
    /// No command is retried automatically.
    pub async fn activate(
        &self,
        scope: &ResolvedWorkspaceScope,
        reference: &MembershipSourceReference,
        activation: &AgentBindingActivationRequest,
        session_id: Uuid,
        principal: AuthenticatedPrincipal,
        observed_at: i64,
    ) -> Result<BindingActivationObservation, ForgeSourceError> {
        let (wire_scope, _) = validated_request_scope(scope, *reference)?;
        if session_id.is_nil() || !valid_principal(principal) {
            return Err(ForgeSourceError::InvalidScope);
        }
        let observed_at = timestamp(observed_at, ForgeSourceError::InvalidScope)?;
        let payload = json!({
            "scope": &wire_scope,
            "issuer_id": reference.issuer_id,
            "destination_id": reference.destination_id,
            "reference": {
                "migration_id": reference.migration_id,
                "expected_freeze_revision": reference.freeze_revision_i64()?,
                "manifest_digest": encode_digest(reference.manifest_digest),
            },
            "activation": activation_json(activation),
            "session_id": session_id,
            "principal": principal_json(principal),
            "observed_at_ms": observed_at.get(),
        });
        let result = self
            .commands
            .command(CommandOp::AgentBindingActivate, payload)
            .await
            .map_err(forge_source::map_command_error)?;
        let projection = parse_activation_readback(result, &wire_scope, *reference)?
            .ok_or(ForgeSourceError::Protocol)?;
        validate_activation_mutation(&projection, activation, session_id, principal)?;
        activation_observation(projection, *reference, scope.workspace_id())
    }

    /// Read one activation by the exact reservation request identity.
    ///
    /// # Errors
    /// Rejects invalid identities, unavailable authority or inconsistent source readback.
    /// No command is retried automatically.
    pub async fn activation(
        &self,
        scope: &ResolvedWorkspaceScope,
        reference: &MembershipSourceReference,
        request: &AgentBindingReservationRequest,
    ) -> Result<Option<BindingActivationObservation>, ForgeSourceError> {
        let (wire_scope, _) = validated_request_scope(scope, *reference)?;
        let payload = json!({
            "scope": &wire_scope,
            "issuer_id": reference.issuer_id,
            "destination_id": reference.destination_id,
            "request_id": request.request_id(),
            "session_id": request.session_id().into_uuid(),
            "principal": principal_json(request.principal()),
        });
        let result = self
            .commands
            .command(CommandOp::AgentBindingActivationGet, payload)
            .await
            .map_err(forge_source::map_command_error)?;
        let Some(projection) = parse_activation_readback(result, &wire_scope, *reference)? else {
            return Ok(None);
        };
        validate_activation_query(&projection, request)?;
        activation_observation(projection, *reference, scope.workspace_id()).map(Some)
    }

    /// Record one terminal rejection from the stable O-owned attestation.
    ///
    /// # Errors
    /// Rejects invalid identities, unavailable authority or inconsistent source readback.
    /// No command is retried automatically.
    pub async fn reject(
        &self,
        scope: &ResolvedWorkspaceScope,
        reference: &MembershipSourceReference,
        binding: &AgentBindingReservationRequest,
        activation: &AgentBindingActivationRequest,
        observed_at: i64,
    ) -> Result<BindingRejectionObservation, ForgeSourceError> {
        let (wire_scope, _) = validated_request_scope(scope, *reference)?;
        if activation.request_id() != binding.request_id() {
            return Err(ForgeSourceError::InvalidScope);
        }
        let observed_at = timestamp(observed_at, ForgeSourceError::InvalidScope)?;
        let payload = json!({
            "scope": &wire_scope,
            "issuer_id": reference.issuer_id,
            "destination_id": reference.destination_id,
            "reference": {
                "migration_id": reference.migration_id,
                "expected_freeze_revision": reference.freeze_revision_i64()?,
                "manifest_digest": encode_digest(reference.manifest_digest),
            },
            "reservation": reservation_json(binding),
            "activation": activation_json(activation),
            "reason": "principal_transport_mismatch",
            "observed_at_ms": observed_at.get(),
        });
        let result = self
            .commands
            .command_with_request_id(binding.request_id(), CommandOp::AgentBindingReject, payload)
            .await
            .map_err(map_rejection_command_error)?;
        parse_rejection_mutation(
            result,
            &wire_scope,
            *reference,
            binding,
            activation,
            observed_at,
        )
    }

    /// Read one historical terminal rejection by its canonical caller intent.
    ///
    /// # Errors
    /// Rejects invalid identities, unavailable authority or inconsistent source readback.
    /// Absence never grants mutation retry authority.
    pub async fn rejection(
        &self,
        scope: &ResolvedWorkspaceScope,
        reference: &MembershipSourceReference,
        binding: &AgentBindingReservationRequest,
        request_digest: [u8; 32],
    ) -> Result<Option<BindingRejectionObservation>, ForgeSourceError> {
        let (wire_scope, _) = validated_request_scope(scope, *reference)?;
        if request_digest == [0; 32] {
            return Err(ForgeSourceError::InvalidScope);
        }
        let payload = json!({
            "scope": &wire_scope,
            "issuer_id": reference.issuer_id,
            "destination_id": reference.destination_id,
            "request_id": binding.request_id(),
            "request_digest": encode_digest(request_digest),
            "session_id": binding.session_id().into_uuid(),
            "principal": principal_json(binding.principal()),
        });
        let result = self
            .commands
            .command_with_request_id(
                binding.request_id(),
                CommandOp::AgentBindingRejectionGet,
                payload,
            )
            .await
            .map_err(map_rejection_command_error)?;
        parse_rejection_readback(result, &wire_scope, *reference, binding, request_digest)
    }
}

fn map_rejection_command_error(error: ForgeHttpClientError) -> ForgeSourceError {
    match error {
        ForgeHttpClientError::Service {
            status: 409,
            code: ForgeHttpServiceCode::ForgeUnavailable,
        } => ForgeSourceError::Stale,
        ForgeHttpClientError::Service {
            status: 202,
            code: ForgeHttpServiceCode::UnknownOutcome,
        } => ForgeSourceError::Unavailable,
        other => forge_source::map_command_error(other),
    }
}

fn validated_request_scope(
    expected: &ResolvedWorkspaceScope,
    reference: MembershipSourceReference,
) -> Result<(ScopeDto, String), ForgeSourceError> {
    let _revision = reference.freeze_revision_i64()?;
    let (scope, relative_root) = forge_source::validated_scope(expected)?;
    Ok((scope, relative_root))
}

fn reservation_json(request: &AgentBindingReservationRequest) -> Value {
    json!({
        "request_id": request.request_id(),
        "activity_id": request.activity_id().into_uuid(),
        "agent_id": request.agent_id().into_uuid(),
        "session_id": request.session_id().into_uuid(),
        "principal": principal_json(request.principal()),
        "requested_at_ms": request.requested_at().get(),
    })
}

fn activation_json(request: &AgentBindingActivationRequest) -> Value {
    json!({
        "request_id": request.request_id(),
        "membership_digest": encode_digest(request.membership_digest()),
        "display_name": request.display_name().as_str(),
        "declared_role": request.declared_role().as_str(),
        "transport": transport_name(request.transport()),
    })
}

fn principal_json(principal: AuthenticatedPrincipal) -> Value {
    match principal {
        AuthenticatedPrincipal::TrustedStdio => json!({ "kind": "trusted_stdio" }),
        AuthenticatedPrincipal::DirectToken(id) => json!({
            "kind": "direct_token",
            "token_id": id.into_uuid(),
        }),
        AuthenticatedPrincipal::OAuthGrant(id) => json!({
            "kind": "oauth_grant",
            "grant_id": id.into_uuid(),
        }),
    }
}

fn valid_principal(principal: AuthenticatedPrincipal) -> bool {
    match principal {
        AuthenticatedPrincipal::TrustedStdio => true,
        AuthenticatedPrincipal::DirectToken(id) => !id.into_uuid().is_nil(),
        AuthenticatedPrincipal::OAuthGrant(id) => !id.into_uuid().is_nil(),
    }
}

fn transport_name(transport: AgentTransportKind) -> &'static str {
    match transport {
        AgentTransportKind::Retired => "mcp",
        AgentTransportKind::Stdio => "stdio",
        AgentTransportKind::StreamableHttp => "streamable_http",
        AgentTransportKind::Other => "other",
    }
}

#[derive(Debug, Deserialize)]
#[serde(tag = "kind", deny_unknown_fields)]
enum PrincipalReadback {
    #[serde(rename = "trusted_stdio")]
    TrustedStdio,
    #[serde(rename = "direct_token")]
    DirectToken { token_id: String },
    #[serde(rename = "oauth_grant")]
    OAuthGrant { grant_id: String },
}

fn principal_from_readback(
    principal: &PrincipalReadback,
) -> Result<AuthenticatedPrincipal, ForgeSourceError> {
    match principal {
        PrincipalReadback::TrustedStdio => Ok(AuthenticatedPrincipal::TrustedStdio),
        PrincipalReadback::DirectToken { token_id } => Ok(AuthenticatedPrincipal::DirectToken(
            DirectTokenId::from_uuid(forge_source::canonical_uuid(token_id)?),
        )),
        PrincipalReadback::OAuthGrant { grant_id } => Ok(AuthenticatedPrincipal::OAuthGrant(
            OAuthGrantId::from_uuid(forge_source::canonical_uuid(grant_id)?),
        )),
    }
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum TransportReadback {
    Mcp,
    Stdio,
    StreamableHttp,
    Other,
}

impl From<TransportReadback> for AgentTransportKind {
    fn from(value: TransportReadback) -> Self {
        match value {
            TransportReadback::Mcp => Self::Retired,
            TransportReadback::Stdio => Self::Stdio,
            TransportReadback::StreamableHttp => Self::StreamableHttp,
            TransportReadback::Other => Self::Other,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DelegationReadback {
    scope: ScopeDto,
    destination_id: String,
    found: bool,
    retry_authorized: bool,
    request_equivalence_proven: bool,
    #[serde(default)]
    workspace_id: Option<String>,
    #[serde(default)]
    migration_id: Option<String>,
    #[serde(default)]
    freeze_destination_id: Option<String>,
    #[serde(default)]
    freeze_revision: Option<i64>,
    #[serde(default)]
    freeze_state: Option<FrozenStateReadback>,
    #[serde(default)]
    freeze_created_at_ms: Option<i64>,
    #[serde(default)]
    freeze_updated_at_ms: Option<i64>,
    #[serde(default)]
    manifest_digest: Option<String>,
    #[serde(default)]
    delegated_at_ms: Option<i64>,
}

#[derive(Debug, Clone, Copy, Deserialize)]
enum FrozenStateReadback {
    #[serde(rename = "frozen")]
    Frozen,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReservationReadback {
    scope: ScopeDto,
    destination_id: String,
    found: bool,
    retry_authorized: bool,
    request_equivalence_proven: bool,
    #[serde(default)]
    workspace_id: Option<String>,
    #[serde(default)]
    migration_id: Option<String>,
    #[serde(default)]
    freeze_revision: Option<i64>,
    #[serde(default)]
    request_id: Option<String>,
    #[serde(default)]
    activity_id: Option<String>,
    #[serde(default)]
    agent_id: Option<String>,
    #[serde(default)]
    session_id: Option<String>,
    #[serde(default)]
    principal: Option<PrincipalReadback>,
    #[serde(default)]
    requested_at_ms: Option<i64>,
    #[serde(default)]
    reserved_at_ms: Option<i64>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ActivationReadback {
    scope: ScopeDto,
    destination_id: String,
    found: bool,
    retry_authorized: bool,
    request_equivalence_proven: bool,
    #[serde(default)]
    workspace_id: Option<String>,
    #[serde(default)]
    migration_id: Option<String>,
    #[serde(default)]
    freeze_revision: Option<i64>,
    #[serde(default)]
    request_id: Option<String>,
    #[serde(default)]
    activity_id: Option<String>,
    #[serde(default)]
    agent_id: Option<String>,
    #[serde(default)]
    session_id: Option<String>,
    #[serde(default)]
    principal: Option<PrincipalReadback>,
    #[serde(default)]
    requested_at_ms: Option<i64>,
    #[serde(default)]
    reserved_at_ms: Option<i64>,
    #[serde(default)]
    membership_digest: Option<String>,
    #[serde(default)]
    display_name: Option<String>,
    #[serde(default)]
    declared_role: Option<String>,
    #[serde(default)]
    transport: Option<TransportReadback>,
    #[serde(default)]
    activated_at_ms: Option<i64>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RejectionMutationReadback {
    scope: ScopeDto,
    issuer_id: String,
    destination_id: String,
    status: RejectionMutationStatus,
    replayed: bool,
    receipt: RejectionReceiptReadback,
    retry_authorized: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
enum RejectionMutationStatus {
    Recorded,
    Replay,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RejectionGetReadback {
    scope: ScopeDto,
    issuer_id: String,
    destination_id: String,
    found: bool,
    status: RejectionReadStatus,
    receipt: Option<RejectionReceiptReadback>,
    retry_authorized: bool,
    request_equivalence_proven: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
enum RejectionReadStatus {
    HistoricalReceipt,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RejectionReceiptReadback {
    request_id: String,
    request_digest: String,
    workspace_id: String,
    activity_id: String,
    agent_id: String,
    session_id: String,
    principal: RejectionPrincipalReadback,
    migration_id: String,
    destination_id: String,
    expected_freeze_revision: u64,
    manifest_digest: String,
    issuer_id: String,
    workspace_root_fingerprint: String,
    binding_digest: String,
    delegation_digest: String,
    membership_digest: String,
    display_name: String,
    declared_role: String,
    transport: TransportReadback,
    reason: RejectionReasonReadback,
    reserved_at_ms: i64,
    requested_at_ms: i64,
    observed_at_ms: i64,
    rejected_at_ms: i64,
    receipt_digest: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RejectionPrincipalReadback {
    kind: RejectionPrincipalKind,
    #[serde(deserialize_with = "deserialize_required_option")]
    token_id: Option<String>,
    #[serde(deserialize_with = "deserialize_required_option")]
    grant_id: Option<String>,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum RejectionPrincipalKind {
    TrustedStdio,
    DirectToken,
    OauthGrant,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
enum RejectionReasonReadback {
    PrincipalTransportMismatch,
}

fn deserialize_required_option<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer)
}

#[derive(Debug, PartialEq, Eq)]
struct ReservationProjection {
    workspace_id: Uuid,
    migration_id: Uuid,
    freeze_revision: u64,
    request: AgentBindingReservationRequest,
    reserved_at: TimestampMillis,
}

#[derive(Debug, PartialEq, Eq)]
struct ActivationProjection {
    reservation: ReservationProjection,
    activation: AgentBindingActivationRequest,
    activated_at: TimestampMillis,
}

const DELEGATION_ROW_FIELDS: &[&str] = &[
    "workspace_id",
    "migration_id",
    "freeze_destination_id",
    "freeze_revision",
    "freeze_state",
    "freeze_created_at_ms",
    "freeze_updated_at_ms",
    "manifest_digest",
    "delegated_at_ms",
];

const RESERVATION_ROW_FIELDS: &[&str] = &[
    "workspace_id",
    "migration_id",
    "freeze_revision",
    "request_id",
    "activity_id",
    "agent_id",
    "session_id",
    "principal",
    "requested_at_ms",
    "reserved_at_ms",
];

const ACTIVATION_ROW_FIELDS: &[&str] = &[
    "workspace_id",
    "migration_id",
    "freeze_revision",
    "request_id",
    "activity_id",
    "agent_id",
    "session_id",
    "principal",
    "requested_at_ms",
    "reserved_at_ms",
    "membership_digest",
    "display_name",
    "declared_role",
    "transport",
    "activated_at_ms",
];

fn parse_delegation_readback(
    value: Value,
    expected_scope: &ScopeDto,
    reference: MembershipSourceReference,
) -> Result<Option<BindingDelegationObservation>, ForgeSourceError> {
    let found = found_flag(&value)?;
    validate_row_shape(&value, found, DELEGATION_ROW_FIELDS)?;
    let readback: DelegationReadback = forge_source::decode_result(value)?;
    validate_base(
        &readback.scope,
        &readback.destination_id,
        readback.retry_authorized,
        readback.request_equivalence_proven,
        expected_scope,
        reference.destination_id,
    )?;
    if !readback.found {
        return Ok(None);
    }

    let workspace_id = response_uuid(required(readback.workspace_id.as_deref())?)?;
    let migration_id = response_uuid(required(readback.migration_id.as_deref())?)?;
    let freeze_destination_id =
        response_uuid(required(readback.freeze_destination_id.as_deref())?)?;
    let freeze_revision = decode_freeze_revision(*required(readback.freeze_revision.as_ref())?)?;
    let _freeze_state = required(readback.freeze_state.as_ref())?;
    let freeze_created_at = timestamp(
        *required(readback.freeze_created_at_ms.as_ref())?,
        ForgeSourceError::Protocol,
    )?;
    let freeze_updated_at = timestamp(
        *required(readback.freeze_updated_at_ms.as_ref())?,
        ForgeSourceError::Protocol,
    )?;
    let manifest_digest = decode_digest(required(readback.manifest_digest.as_deref())?)?;
    let delegated_at = timestamp(
        *required(readback.delegated_at_ms.as_ref())?,
        ForgeSourceError::Protocol,
    )?;
    if workspace_id != expected_scope.workspace_id
        || migration_id != reference.migration_id
        || freeze_destination_id != reference.destination_id
        || freeze_revision != reference.freeze_revision
        || manifest_digest != reference.manifest_digest
        || freeze_created_at > freeze_updated_at
        || freeze_updated_at > delegated_at
    {
        return Err(ForgeSourceError::Protocol);
    }
    Ok(Some(BindingDelegationObservation {
        workspace_id: WorkspaceId::from_uuid(workspace_id),
        migration_id,
        destination_id: reference.destination_id,
        freeze_revision,
        manifest_digest,
        freeze_created_at,
        freeze_updated_at,
        delegated_at,
    }))
}

fn parse_reservation_readback(
    value: Value,
    expected_scope: &ScopeDto,
    reference: MembershipSourceReference,
    expected_request: Option<&AgentBindingReservationRequest>,
) -> Result<Option<BindingReservationObservation>, ForgeSourceError> {
    let found = found_flag(&value)?;
    validate_row_shape(&value, found, RESERVATION_ROW_FIELDS)?;
    let readback: ReservationReadback = forge_source::decode_result(value)?;
    validate_base(
        &readback.scope,
        &readback.destination_id,
        readback.retry_authorized,
        readback.request_equivalence_proven,
        expected_scope,
        reference.destination_id,
    )?;
    if !readback.found {
        return Ok(None);
    }
    let projection = reservation_projection(reservation_projection_input(&readback))?;
    validate_reservation_projection(&projection, expected_scope, reference)?;
    if expected_request.is_some_and(|expected| projection.request != *expected) {
        return Err(ForgeSourceError::Protocol);
    }
    reservation_observation(projection, reference).map(Some)
}

fn parse_activation_readback(
    value: Value,
    expected_scope: &ScopeDto,
    reference: MembershipSourceReference,
) -> Result<Option<ActivationProjection>, ForgeSourceError> {
    let found = found_flag(&value)?;
    validate_row_shape(&value, found, ACTIVATION_ROW_FIELDS)?;
    let readback: ActivationReadback = forge_source::decode_result(value)?;
    validate_base(
        &readback.scope,
        &readback.destination_id,
        readback.retry_authorized,
        readback.request_equivalence_proven,
        expected_scope,
        reference.destination_id,
    )?;
    if !readback.found {
        return Ok(None);
    }
    let reservation = reservation_projection(activation_reservation_projection_input(&readback))?;
    validate_reservation_projection(&reservation, expected_scope, reference)?;
    let activation_request = activation_request(&readback)?;
    if activation_request.request_id() != reservation.request.request_id() {
        return Err(ForgeSourceError::Protocol);
    }
    let activated_at = timestamp(
        *required(readback.activated_at_ms.as_ref())?,
        ForgeSourceError::Protocol,
    )?;
    Ok(Some(ActivationProjection {
        reservation,
        activation: activation_request,
        activated_at,
    }))
}

fn parse_rejection_mutation(
    value: Value,
    expected_scope: &ScopeDto,
    reference: MembershipSourceReference,
    binding: &AgentBindingReservationRequest,
    activation: &AgentBindingActivationRequest,
    observed_at: TimestampMillis,
) -> Result<BindingRejectionObservation, ForgeSourceError> {
    let readback: RejectionMutationReadback = forge_source::decode_result(value)?;
    validate_rejection_base(
        &readback.scope,
        &readback.issuer_id,
        &readback.destination_id,
        readback.retry_authorized,
        expected_scope,
        reference,
    )?;
    if (readback.status == RejectionMutationStatus::Replay) != readback.replayed {
        return Err(ForgeSourceError::Protocol);
    }
    rejection_observation(
        &readback.receipt,
        expected_scope,
        reference,
        binding,
        Some(activation),
        None,
        Some(observed_at),
    )
}

fn parse_rejection_readback(
    value: Value,
    expected_scope: &ScopeDto,
    reference: MembershipSourceReference,
    binding: &AgentBindingReservationRequest,
    request_digest: [u8; 32],
) -> Result<Option<BindingRejectionObservation>, ForgeSourceError> {
    if !value
        .as_object()
        .is_some_and(|object| object.contains_key("receipt"))
    {
        return Err(ForgeSourceError::Protocol);
    }
    let readback: RejectionGetReadback = forge_source::decode_result(value)?;
    validate_rejection_base(
        &readback.scope,
        &readback.issuer_id,
        &readback.destination_id,
        readback.retry_authorized,
        expected_scope,
        reference,
    )?;
    if readback.status != RejectionReadStatus::HistoricalReceipt
        || readback.request_equivalence_proven
        || readback.found != readback.receipt.is_some()
    {
        return Err(ForgeSourceError::Protocol);
    }
    readback
        .receipt
        .as_ref()
        .map(|receipt| {
            rejection_observation(
                receipt,
                expected_scope,
                reference,
                binding,
                None,
                Some(request_digest),
                None,
            )
        })
        .transpose()
}

#[allow(clippy::too_many_arguments)]
fn rejection_observation(
    receipt: &RejectionReceiptReadback,
    expected_scope: &ScopeDto,
    reference: MembershipSourceReference,
    binding: &AgentBindingReservationRequest,
    activation: Option<&AgentBindingActivationRequest>,
    expected_request_digest: Option<[u8; 32]>,
    expected_observed_at: Option<TimestampMillis>,
) -> Result<BindingRejectionObservation, ForgeSourceError> {
    let request_id = response_uuid(&receipt.request_id)?;
    let request_digest = decode_digest(&receipt.request_digest)?;
    let workspace_id = response_uuid(&receipt.workspace_id)?;
    let activity_id = response_uuid(&receipt.activity_id)?;
    let agent_id = response_uuid(&receipt.agent_id)?;
    let session_id = response_uuid(&receipt.session_id)?;
    let principal = rejection_principal(&receipt.principal)?;
    let migration_id = response_uuid(&receipt.migration_id)?;
    let destination_id = response_uuid(&receipt.destination_id)?;
    let manifest_digest = decode_digest(&receipt.manifest_digest)?;
    let issuer_id = response_uuid(&receipt.issuer_id)?;
    let membership_digest = decode_digest(&receipt.membership_digest)?;
    let observed_at = timestamp(receipt.observed_at_ms, ForgeSourceError::Protocol)?;
    if request_id != binding.request_id()
        || workspace_id != expected_scope.workspace_id
        || activity_id != binding.activity_id().into_uuid()
        || agent_id != binding.agent_id().into_uuid()
        || session_id != binding.session_id().into_uuid()
        || principal != binding.principal()
        || migration_id != reference.migration_id
        || destination_id != reference.destination_id
        || receipt.expected_freeze_revision != reference.freeze_revision
        || manifest_digest != reference.manifest_digest
        || issuer_id != reference.issuer_id
        || receipt.requested_at_ms != binding.requested_at().get()
        || expected_request_digest.is_some_and(|expected| expected != request_digest)
        || expected_observed_at.is_some_and(|expected| expected != observed_at)
        || activation.is_some_and(|expected| {
            expected.request_id() != request_id
                || expected.membership_digest() != membership_digest
                || expected.display_name().as_str() != receipt.display_name
                || expected.declared_role().as_str() != receipt.declared_role
                || expected.transport() != AgentTransportKind::from(receipt.transport)
        })
        || receipt.reason != RejectionReasonReadback::PrincipalTransportMismatch
    {
        return Err(ForgeSourceError::Protocol);
    }
    BindingRejectionObservation::new(
        request_id,
        WorkspaceId::from_uuid(workspace_id),
        ActivityId::from_uuid(activity_id),
        AgentInstanceId::from_uuid(agent_id),
        AgentSessionId::from_uuid(session_id),
        principal,
        migration_id,
        destination_id,
        receipt.expected_freeze_revision,
        manifest_digest,
        issuer_id,
        WorkspaceRootFingerprint::from_bytes(decode_digest16(&receipt.workspace_root_fingerprint)?),
        decode_digest(&receipt.binding_digest)?,
        decode_digest(&receipt.delegation_digest)?,
        membership_digest,
        AgentDisplayName::new(receipt.display_name.clone())
            .map_err(|_| ForgeSourceError::Protocol)?,
        AgentDeclaredRole::new(receipt.declared_role.clone())
            .map_err(|_| ForgeSourceError::Protocol)?,
        AgentTransportKind::from(receipt.transport),
        AgentBindingRejectionReason::PrincipalTransportMismatch,
        timestamp(receipt.reserved_at_ms, ForgeSourceError::Protocol)?,
        timestamp(receipt.requested_at_ms, ForgeSourceError::Protocol)?,
        observed_at,
        timestamp(receipt.rejected_at_ms, ForgeSourceError::Protocol)?,
        request_digest,
        decode_digest(&receipt.receipt_digest)?,
    )
    .map_err(|_| ForgeSourceError::Protocol)
}

fn validate_rejection_base(
    actual_scope: &ScopeDto,
    issuer_id: &str,
    destination_id: &str,
    retry_authorized: bool,
    expected_scope: &ScopeDto,
    reference: MembershipSourceReference,
) -> Result<(), ForgeSourceError> {
    if actual_scope != expected_scope
        || response_uuid(issuer_id)? != reference.issuer_id
        || response_uuid(destination_id)? != reference.destination_id
        || retry_authorized
    {
        return Err(ForgeSourceError::Protocol);
    }
    Ok(())
}

fn rejection_principal(
    value: &RejectionPrincipalReadback,
) -> Result<AuthenticatedPrincipal, ForgeSourceError> {
    match value.kind {
        RejectionPrincipalKind::TrustedStdio
            if value.token_id.is_none() && value.grant_id.is_none() =>
        {
            Ok(AuthenticatedPrincipal::TrustedStdio)
        }
        RejectionPrincipalKind::DirectToken if value.grant_id.is_none() => {
            Ok(AuthenticatedPrincipal::DirectToken(
                DirectTokenId::from_uuid(response_uuid(required(value.token_id.as_deref())?)?),
            ))
        }
        RejectionPrincipalKind::OauthGrant if value.token_id.is_none() => {
            Ok(AuthenticatedPrincipal::OAuthGrant(OAuthGrantId::from_uuid(
                response_uuid(required(value.grant_id.as_deref())?)?,
            )))
        }
        _ => Err(ForgeSourceError::Protocol),
    }
}

#[derive(Clone, Copy)]
struct ReservationProjectionInput<'a> {
    workspace_id: Option<&'a str>,
    migration_id: Option<&'a str>,
    freeze_revision: Option<i64>,
    request_id: Option<&'a str>,
    activity_id: Option<&'a str>,
    agent_id: Option<&'a str>,
    session_id: Option<&'a str>,
    principal: Option<&'a PrincipalReadback>,
    requested_at_ms: Option<i64>,
    reserved_at_ms: Option<i64>,
}

fn reservation_projection_input(readback: &ReservationReadback) -> ReservationProjectionInput<'_> {
    ReservationProjectionInput {
        workspace_id: readback.workspace_id.as_deref(),
        migration_id: readback.migration_id.as_deref(),
        freeze_revision: readback.freeze_revision,
        request_id: readback.request_id.as_deref(),
        activity_id: readback.activity_id.as_deref(),
        agent_id: readback.agent_id.as_deref(),
        session_id: readback.session_id.as_deref(),
        principal: readback.principal.as_ref(),
        requested_at_ms: readback.requested_at_ms,
        reserved_at_ms: readback.reserved_at_ms,
    }
}

fn activation_reservation_projection_input(
    readback: &ActivationReadback,
) -> ReservationProjectionInput<'_> {
    ReservationProjectionInput {
        workspace_id: readback.workspace_id.as_deref(),
        migration_id: readback.migration_id.as_deref(),
        freeze_revision: readback.freeze_revision,
        request_id: readback.request_id.as_deref(),
        activity_id: readback.activity_id.as_deref(),
        agent_id: readback.agent_id.as_deref(),
        session_id: readback.session_id.as_deref(),
        principal: readback.principal.as_ref(),
        requested_at_ms: readback.requested_at_ms,
        reserved_at_ms: readback.reserved_at_ms,
    }
}

fn reservation_projection(
    readback: ReservationProjectionInput<'_>,
) -> Result<ReservationProjection, ForgeSourceError> {
    let request = AgentBindingReservationRequest::new(
        response_uuid(required(readback.request_id)?)?,
        ActivityId::from_uuid(response_uuid(required(readback.activity_id)?)?),
        AgentInstanceId::from_uuid(response_uuid(required(readback.agent_id)?)?),
        AgentSessionId::from_uuid(response_uuid(required(readback.session_id)?)?),
        principal_from_readback(required(readback.principal)?)?,
        timestamp(
            *required(readback.requested_at_ms.as_ref())?,
            ForgeSourceError::Protocol,
        )?,
    )
    .map_err(|_| ForgeSourceError::Protocol)?;
    Ok(ReservationProjection {
        workspace_id: response_uuid(required(readback.workspace_id)?)?,
        migration_id: response_uuid(required(readback.migration_id)?)?,
        freeze_revision: decode_freeze_revision(*required(readback.freeze_revision.as_ref())?)?,
        request,
        reserved_at: timestamp(
            *required(readback.reserved_at_ms.as_ref())?,
            ForgeSourceError::Protocol,
        )?,
    })
}

fn activation_request(
    readback: &ActivationReadback,
) -> Result<AgentBindingActivationRequest, ForgeSourceError> {
    AgentBindingActivationRequest::new(
        response_uuid(required(readback.request_id.as_deref())?)?,
        decode_digest(required(readback.membership_digest.as_deref())?)?,
        AgentDisplayName::new(required(readback.display_name.as_deref())?.to_owned())
            .map_err(|_| ForgeSourceError::Protocol)?,
        AgentDeclaredRole::new(required(readback.declared_role.as_deref())?.to_owned())
            .map_err(|_| ForgeSourceError::Protocol)?,
        AgentTransportKind::from(readback.transport.ok_or(ForgeSourceError::Protocol)?),
    )
    .map_err(|_| ForgeSourceError::Protocol)
}

fn validate_reservation_projection(
    projection: &ReservationProjection,
    expected_scope: &ScopeDto,
    reference: MembershipSourceReference,
) -> Result<(), ForgeSourceError> {
    if projection.workspace_id != expected_scope.workspace_id
        || projection.migration_id != reference.migration_id
        || projection.freeze_revision != reference.freeze_revision
    {
        return Err(ForgeSourceError::Protocol);
    }
    Ok(())
}

fn validate_activation_mutation(
    projection: &ActivationProjection,
    expected_activation: &AgentBindingActivationRequest,
    expected_session_id: Uuid,
    expected_principal: AuthenticatedPrincipal,
) -> Result<(), ForgeSourceError> {
    if projection.activation != *expected_activation
        || projection.reservation.request.session_id().into_uuid() != expected_session_id
        || projection.reservation.request.principal() != expected_principal
    {
        return Err(ForgeSourceError::Protocol);
    }
    Ok(())
}

fn validate_activation_query(
    projection: &ActivationProjection,
    expected_request: &AgentBindingReservationRequest,
) -> Result<(), ForgeSourceError> {
    if projection.reservation.request != *expected_request {
        return Err(ForgeSourceError::Protocol);
    }
    Ok(())
}

fn reservation_observation(
    projection: ReservationProjection,
    reference: MembershipSourceReference,
) -> Result<BindingReservationObservation, ForgeSourceError> {
    BindingReservationObservation::new(
        projection.migration_id,
        reference.destination_id,
        WorkspaceId::from_uuid(projection.workspace_id),
        projection.freeze_revision,
        projection.request,
        projection.reserved_at,
    )
    .map_err(|_| ForgeSourceError::Protocol)
}

fn activation_observation(
    projection: ActivationProjection,
    reference: MembershipSourceReference,
    expected_workspace_id: WorkspaceId,
) -> Result<BindingActivationObservation, ForgeSourceError> {
    if projection.reservation.workspace_id != expected_workspace_id.into_uuid() {
        return Err(ForgeSourceError::Protocol);
    }
    let reservation = reservation_observation(projection.reservation, reference)?;
    BindingActivationObservation::new(
        reservation,
        projection.activation.membership_digest(),
        projection.activation.display_name().clone(),
        projection.activation.declared_role().clone(),
        projection.activation.transport(),
        projection.activated_at,
    )
    .map_err(|_| ForgeSourceError::Protocol)
}

fn validate_base(
    actual_scope: &ScopeDto,
    destination_id: &str,
    retry_authorized: bool,
    request_equivalence_proven: bool,
    expected_scope: &ScopeDto,
    expected_destination_id: Uuid,
) -> Result<(), ForgeSourceError> {
    if actual_scope != expected_scope
        || forge_source::canonical_uuid(destination_id)? != expected_destination_id
        || retry_authorized
        || request_equivalence_proven
    {
        return Err(ForgeSourceError::Protocol);
    }
    Ok(())
}

fn found_flag(value: &Value) -> Result<bool, ForgeSourceError> {
    value
        .get("found")
        .and_then(Value::as_bool)
        .ok_or(ForgeSourceError::Protocol)
}

fn validate_row_shape(value: &Value, found: bool, fields: &[&str]) -> Result<(), ForgeSourceError> {
    let object = value.as_object().ok_or(ForgeSourceError::Protocol)?;
    if fields
        .iter()
        .any(|field| object.contains_key(*field) != found)
    {
        return Err(ForgeSourceError::Protocol);
    }
    Ok(())
}

fn required<T: ?Sized>(value: Option<&T>) -> Result<&T, ForgeSourceError> {
    value.ok_or(ForgeSourceError::Protocol)
}

fn response_uuid(value: &str) -> Result<Uuid, ForgeSourceError> {
    forge_source::canonical_uuid(value)
}

fn decode_freeze_revision(value: i64) -> Result<u64, ForgeSourceError> {
    if value <= 0 {
        return Err(ForgeSourceError::Protocol);
    }
    u64::try_from(value).map_err(|_| ForgeSourceError::Protocol)
}

fn timestamp(value: i64, error: ForgeSourceError) -> Result<TimestampMillis, ForgeSourceError> {
    TimestampMillis::new(value).map_err(|_| error)
}

fn decode_digest(value: &str) -> Result<[u8; 32], ForgeSourceError> {
    if value.len() != 64
        || !value
            .as_bytes()
            .iter()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    {
        return Err(ForgeSourceError::Protocol);
    }
    let mut digest = [0_u8; 32];
    let (pairs, remainder) = value.as_bytes().as_chunks::<2>();
    if !remainder.is_empty() {
        return Err(ForgeSourceError::Protocol);
    }
    for (index, pair) in pairs.iter().enumerate() {
        let high = hex_nibble(pair[0]).ok_or(ForgeSourceError::Protocol)?;
        let low = hex_nibble(pair[1]).ok_or(ForgeSourceError::Protocol)?;
        digest[index] = (high << 4) | low;
    }
    Ok(digest)
}

fn decode_digest16(value: &str) -> Result<[u8; 16], ForgeSourceError> {
    if value.len() != 32
        || !value
            .as_bytes()
            .iter()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    {
        return Err(ForgeSourceError::Protocol);
    }
    let mut digest = [0_u8; 16];
    for (index, slot) in digest.iter_mut().enumerate() {
        let high = hex_nibble(value.as_bytes()[index * 2]).ok_or(ForgeSourceError::Protocol)?;
        let low = hex_nibble(value.as_bytes()[index * 2 + 1]).ok_or(ForgeSourceError::Protocol)?;
        *slot = (high << 4) | low;
    }
    if digest == [0; 16] {
        return Err(ForgeSourceError::Protocol);
    }
    Ok(digest)
}

fn encode_digest(value: [u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(value.len() * 2);
    for byte in value {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}

fn hex_nibble(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        _ => None,
    }
}

mod departure;
#[cfg(test)]
mod tests;
