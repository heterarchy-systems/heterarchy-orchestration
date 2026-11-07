//! Strict wire contracts for the one-shot Forge Home-source dispatch and its
//! query-only readback/reconciliation paths.

use std::{net::SocketAddr, str::FromStr};

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use forge_backend::{
    HomeTransferSourceRecord, HomeTransferSourceState, decode_home_transfer_source_request,
    encode_home_transfer_source_request,
};
use forge_domain::{
    ActivityId, AgentInstanceId, AgentSessionId, ForgeBackendId, HomeTransferSourceAction,
    HomeTransferSourceGrant, HomeTransferSourceRequest, OrchestrationIssuerId, TimestampMillis,
    WorkspaceId,
};
use heterarchy_orchestration_operations::{
    OperationalActor, TeamPermanentTransferSourceDispatchCommand,
    TeamPermanentTransferSourceDispatchReadback, TeamPermanentTransferSourceDispatchReceipt,
    TeamPermanentTransferSourceReceiptRecord, TeamPermanentTransferSourceWireIdentity,
    WorkspaceProjectScope,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use super::{
    TeamExpansionActorReceiptWire, TeamExpansionActorWire, valid_actor, valid_actor_receipt,
};

/// FHTR request envelopes are bounded by Forge's source-request codec.
pub const MAX_TEAM_PERMANENT_TRANSFER_SOURCE_REQUEST_ENVELOPE_BYTES: usize = 512 * 1024;
/// Canonical response cap shared by dispatch, readback, and reconciliation.
pub const MAX_TEAM_PERMANENT_TRANSFER_SOURCE_RESPONSE_BYTES: usize = 1024 * 1024;
/// FHTS snapshots are bounded independently before the whole JSON response cap.
pub const MAX_TEAM_PERMANENT_TRANSFER_SOURCE_SNAPSHOT_BYTES: usize = 1024 * 1024;
const MAX_SOURCE_HTTP_BODY_BYTES: usize = 64 * 1024;
const MAX_SOURCE_REQUEST_JSON_BYTES: usize = 64 * 1024;
const MAX_SOURCE_LOCATOR_BYTES: usize = 4 * 1024;

/// One exact first source attempt. Source routes, service pins, and grants are
/// server configuration and never arrive in this caller-authored request.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TeamPermanentTransferSourceDispatchActionWire {
    pub request_id: Uuid,
    pub preparation_id: Uuid,
    pub transfer_id: Uuid,
    pub expected_generation: u64,
    pub expected_workspace_leadership_revision: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TeamPermanentTransferSourceDispatchRequest {
    pub actor: TeamExpansionActorWire,
    pub command: TeamPermanentTransferSourceDispatchActionWire,
}

/// A current authorized Workspace actor reading the exact immutable Source
/// attempt. The current reader's revision is checked as its own scope, without
/// rewriting the original mutation's captured revision.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TeamPermanentTransferSourceReadbackRequest {
    pub workspace_id: Uuid,
    pub preparation_id: Uuid,
    pub current_actor: TeamExpansionActorWire,
}

/// Reconciliation is a distinct route over the same exact readback selector.
/// Its handler may issue the Source `Get` operation and record that fact, but
/// never the Source `Prepare` or `AbortUnstarted` mutation.
pub type TeamPermanentTransferSourceReconcileRequest = TeamPermanentTransferSourceReadbackRequest;

/// Whether this request created the immutable local dispatch row or observed
/// the exact existing attempt. This says nothing about Source readiness.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TeamPermanentTransferSourceDispatchDispositionWire {
    Created,
    Existing,
}

/// Canonical original attempt metadata. FHTR is kept as its exact base64
/// envelope; the full Operations preparation and credentials are omitted.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TeamPermanentTransferSourceDispatchRecordWire {
    pub workspace_id: Uuid,
    pub preparation_id: Uuid,
    pub transfer_id: Uuid,
    pub generation: u64,
    pub intent_digest: String,
    pub preparation_digest: String,
    pub expected_workspace_leadership_revision: i64,
    pub request_id: Uuid,
    pub actor: TeamExpansionActorReceiptWire,
    pub source_route: String,
    pub source_scope: TeamPermanentTransferSourceScopeWire,
    pub backend_id: Uuid,
    pub issuer_id: Uuid,
    pub service_destination_id: Uuid,
    pub source_request_envelope_base64: String,
    pub envelope_digest: String,
    pub grant_issued_at_ms: i64,
    pub grant_expires_at_ms: i64,
    pub wire_identity: TeamPermanentTransferSourceWireIdentityWire,
    pub recorded_at_ms: i64,
    /// Opaque persisted Operations payload digest; it is shape-checked only.
    pub payload_digest: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TeamPermanentTransferSourceScopeWire {
    pub workspace_id: Uuid,
    pub location_id: Uuid,
    pub location_revision: i64,
    pub workspace_revision: i64,
    pub locator: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TeamPermanentTransferSourceWireIdentityWire {
    pub transport_request_id: Uuid,
    pub body_sha256: String,
    pub body_byte_count: u32,
}

/// Original Source result fact and its canonical FHTS snapshot. `state` is the
/// source owner's immutable result; this DTO carries no readiness or cutover
/// conclusion.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TeamPermanentTransferSourceSnapshotWire {
    pub snapshot_base64: String,
    pub snapshot_sha256: String,
    pub workspace_id: Uuid,
    pub preparation_id: Uuid,
    pub transfer_id: Uuid,
    pub generation: u64,
    pub source_request_digest: String,
    pub issuer_id: Uuid,
    pub state: TeamPermanentTransferSourceStateWire,
    pub observed_at_ms: i64,
    pub grant_issued_at_ms: i64,
    pub grant_expires_at_ms: i64,
    pub migration_id: Uuid,
    pub freeze_revision: u64,
    pub delegation_digest: String,
    pub inventory_digest: Option<String>,
    pub outcome_digest: String,
    pub local_recorded_at_ms: i64,
    pub envelope_digest: String,
    /// Opaque persisted Operations payload digest; it is shape-checked only.
    pub payload_digest: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TeamPermanentTransferSourceStateWire {
    Prepared,
    AbortedUnstarted,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TeamPermanentTransferSourceDispatchResponse {
    pub request: TeamPermanentTransferSourceDispatchRequest,
    pub disposition: TeamPermanentTransferSourceDispatchDispositionWire,
    /// Required after successful dispatch handling, including exact replay.
    pub dispatch: TeamPermanentTransferSourceDispatchRecordWire,
    #[serde(deserialize_with = "super::deserialize_required_option")]
    pub source: Option<TeamPermanentTransferSourceSnapshotWire>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TeamPermanentTransferSourceReadbackResponse {
    pub request: TeamPermanentTransferSourceReadbackRequest,
    #[serde(deserialize_with = "super::deserialize_required_option")]
    pub dispatch: Option<TeamPermanentTransferSourceDispatchRecordWire>,
    #[serde(deserialize_with = "super::deserialize_required_option")]
    pub source: Option<TeamPermanentTransferSourceSnapshotWire>,
}

pub type TeamPermanentTransferSourceReconcileResponse = TeamPermanentTransferSourceReadbackResponse;

impl TeamPermanentTransferSourceDispatchResponse {
    /// Project one Operations dispatch and optional stored Source fact.
    /// Returns `None` if any owner-owned record fails consistency validation or
    /// the complete serialized response exceeds the shared wire bound.
    pub(crate) fn from_operations(
        request: &TeamPermanentTransferSourceDispatchRequest,
        receipt: &TeamPermanentTransferSourceDispatchReceipt,
        disposition: TeamPermanentTransferSourceDispatchDispositionWire,
        source: Option<&TeamPermanentTransferSourceReceiptRecord>,
    ) -> Option<Self> {
        let source = match source {
            Some(source) => Some(TeamPermanentTransferSourceSnapshotWire::from_operations(
                source,
            )?),
            None => None,
        };
        let response = Self {
            request: request.clone(),
            disposition,
            dispatch: TeamPermanentTransferSourceDispatchRecordWire::from_operations(receipt)?,
            source,
        };
        valid_team_permanent_transfer_source_dispatch_response(&response, request)
            .then_some(response)
    }
}

impl TeamPermanentTransferSourceReadbackResponse {
    /// Project one exact Operations readback, including a local attempt with no
    /// recorded Source result. Absence of dispatch forbids a Source snapshot.
    pub(crate) fn from_operations(
        request: &TeamPermanentTransferSourceReadbackRequest,
        readback: Option<&TeamPermanentTransferSourceDispatchReadback>,
    ) -> Option<Self> {
        let (dispatch, source) = match readback {
            None => (None, None),
            Some(readback) => {
                let dispatch = Some(
                    TeamPermanentTransferSourceDispatchRecordWire::from_operations(
                        readback.dispatch(),
                    )?,
                );
                let source = match readback.original_source_record() {
                    Some(source) => Some(TeamPermanentTransferSourceSnapshotWire::from_operations(
                        source,
                    )?),
                    None => None,
                };
                (dispatch, source)
            }
        };
        let response = Self {
            request: request.clone(),
            dispatch,
            source,
        };
        valid_team_permanent_transfer_source_readback_response(&response, request)
            .then_some(response)
    }
}

impl TeamPermanentTransferSourceDispatchRecordWire {
    pub(crate) fn from_operations(
        receipt: &TeamPermanentTransferSourceDispatchReceipt,
    ) -> Option<Self> {
        let actor = TeamExpansionActorReceiptWire {
            activity_id: receipt.actor().activity_id().into_uuid(),
            agent_id: receipt.actor().agent_id().into_uuid(),
            session_id: receipt.actor().session_id().into_uuid(),
            participation_id: receipt.actor().participation_id().into_uuid(),
            profile_id: receipt.actor().profile_id(),
            participant_id: receipt.actor().participant_id(),
            display_name: receipt.actor().display_name().to_owned(),
        };
        let source_scope =
            TeamPermanentTransferSourceScopeWire::from_operations(receipt.source_scope());
        let wire_identity = TeamPermanentTransferSourceWireIdentityWire {
            transport_request_id: receipt.wire_identity().transport_request_id(),
            body_sha256: lower_hex(&receipt.wire_identity().body_sha256()),
            body_byte_count: receipt.wire_identity().body_byte_count(),
        };
        let value = Self {
            workspace_id: receipt.workspace_id().into_uuid(),
            preparation_id: receipt.preparation_id(),
            transfer_id: receipt.transfer_id(),
            generation: receipt.generation(),
            intent_digest: lower_hex(&receipt.intent_digest()),
            preparation_digest: lower_hex(&receipt.preparation_digest()),
            expected_workspace_leadership_revision: receipt
                .expected_workspace_leadership_revision(),
            request_id: receipt.request_id(),
            actor,
            source_route: receipt.source_route().to_string(),
            source_scope,
            backend_id: receipt.backend_id().into_uuid(),
            issuer_id: receipt.issuer_id().into_uuid(),
            service_destination_id: receipt.service_destination_id(),
            source_request_envelope_base64: URL_SAFE_NO_PAD
                .encode(receipt.source_request_envelope()),
            envelope_digest: lower_hex(receipt.envelope_digest().as_bytes()),
            grant_issued_at_ms: receipt.grant().issued_at().get(),
            grant_expires_at_ms: receipt.grant().expires_at().get(),
            wire_identity,
            recorded_at_ms: receipt.recorded_at().get(),
            payload_digest: lower_hex(&receipt.payload_digest()),
        };
        valid_team_permanent_transfer_source_dispatch_record(&value).then_some(value)
    }
}

impl TeamPermanentTransferSourceScopeWire {
    fn from_operations(scope: &WorkspaceProjectScope) -> Self {
        Self {
            workspace_id: scope.workspace_id(),
            location_id: scope.location_id(),
            location_revision: scope.location_revision(),
            workspace_revision: scope.workspace_revision(),
            locator: scope.locator().to_owned(),
        }
    }

    fn from_actor_scope(scope: &crate::wire::WorkspaceQueryScope) -> Self {
        Self {
            workspace_id: scope.workspace_id,
            location_id: scope.location_id,
            location_revision: scope.location_revision,
            workspace_revision: scope.workspace_revision,
            locator: scope.locator.clone(),
        }
    }

    fn to_operations(&self) -> Option<WorkspaceProjectScope> {
        WorkspaceProjectScope::new(
            self.workspace_id,
            self.location_id,
            self.location_revision,
            self.workspace_revision,
            self.locator.clone(),
        )
        .ok()
    }

    fn matches_actor_scope(&self, scope: &crate::wire::WorkspaceQueryScope) -> bool {
        self == &Self::from_actor_scope(scope)
    }
}

impl TeamPermanentTransferSourceSnapshotWire {
    fn from_operations(receipt: &TeamPermanentTransferSourceReceiptRecord) -> Option<Self> {
        let source = receipt.source_record();
        let snapshot = receipt.snapshot_bytes();
        if snapshot.is_empty() || snapshot.len() > MAX_TEAM_PERMANENT_TRANSFER_SOURCE_SNAPSHOT_BYTES
        {
            return None;
        }
        let snapshot_digest: [u8; 32] = Sha256::digest(snapshot).into();
        if snapshot_digest != receipt.snapshot_sha256() {
            return None;
        }
        Some(Self {
            snapshot_base64: URL_SAFE_NO_PAD.encode(snapshot),
            snapshot_sha256: lower_hex(&snapshot_digest),
            workspace_id: source.request().scope().workspace_id().into_uuid(),
            preparation_id: source.request().witness().preparation_id(),
            transfer_id: source.request().subject().transfer_id(),
            generation: source.request().subject().transfer_generation(),
            source_request_digest: lower_hex(source.request().request_digest().as_bytes()),
            issuer_id: source.issuer().into_uuid(),
            state: match source.state() {
                HomeTransferSourceState::Prepared => TeamPermanentTransferSourceStateWire::Prepared,
                HomeTransferSourceState::AbortedUnstarted => {
                    TeamPermanentTransferSourceStateWire::AbortedUnstarted
                }
            },
            observed_at_ms: source.observed_at().get(),
            grant_issued_at_ms: source.grant_issued_at().get(),
            grant_expires_at_ms: source.grant_expires_at().get(),
            migration_id: source.migration_id(),
            freeze_revision: source.freeze_revision(),
            delegation_digest: lower_hex(source.delegation_digest()),
            inventory_digest: source.inventory_digest().map(lower_hex),
            outcome_digest: lower_hex(source.outcome_digest()),
            local_recorded_at_ms: receipt.recorded_at().get(),
            envelope_digest: lower_hex(receipt.envelope_digest().as_bytes()),
            payload_digest: lower_hex(&receipt.payload_digest()),
        })
    }
}

/// Validate the exact original Source dispatch request before the server
/// derives any Source pins or creates an Operations attempt.
#[must_use]
pub fn valid_team_permanent_transfer_source_dispatch_request(
    request: &TeamPermanentTransferSourceDispatchRequest,
) -> bool {
    valid_actor(&request.actor)
        && request.actor.scope.locator.len() <= MAX_SOURCE_LOCATOR_BYTES
        && scope_from_actor(&request.actor).is_some()
        && !request.command.request_id.is_nil()
        && !request.command.preparation_id.is_nil()
        && !request.command.transfer_id.is_nil()
        && request.command.expected_generation > 0
        && i64::try_from(request.command.expected_generation).is_ok()
        && request.command.expected_workspace_leadership_revision > 0
        && json_within_limit(request, MAX_SOURCE_REQUEST_JSON_BYTES)
}

#[must_use]
pub fn valid_team_permanent_transfer_source_readback_request(
    request: &TeamPermanentTransferSourceReadbackRequest,
) -> bool {
    !request.workspace_id.is_nil()
        && !request.preparation_id.is_nil()
        && valid_actor(&request.current_actor)
        && request.current_actor.scope.workspace_id == request.workspace_id
        && request.current_actor.scope.locator.len() <= MAX_SOURCE_LOCATOR_BYTES
        && scope_from_actor(&request.current_actor).is_some()
        && json_within_limit(request, MAX_SOURCE_REQUEST_JSON_BYTES)
}

#[must_use]
pub fn valid_team_permanent_transfer_source_reconcile_request(
    request: &TeamPermanentTransferSourceReconcileRequest,
) -> bool {
    valid_team_permanent_transfer_source_readback_request(request)
}

#[must_use]
pub fn valid_team_permanent_transfer_source_dispatch_response(
    response: &TeamPermanentTransferSourceDispatchResponse,
    request: &TeamPermanentTransferSourceDispatchRequest,
) -> bool {
    &response.request == request
        && valid_team_permanent_transfer_source_dispatch_request(request)
        && valid_team_permanent_transfer_source_dispatch_record_for_request(
            &response.dispatch,
            request,
        )
        && response.source.as_ref().is_none_or(|source| {
            valid_team_permanent_transfer_source_snapshot(source, &response.dispatch)
        })
        && json_within_limit(response, MAX_TEAM_PERMANENT_TRANSFER_SOURCE_RESPONSE_BYTES)
}

#[must_use]
pub fn valid_team_permanent_transfer_source_readback_response(
    response: &TeamPermanentTransferSourceReadbackResponse,
    request: &TeamPermanentTransferSourceReadbackRequest,
) -> bool {
    &response.request == request
        && valid_team_permanent_transfer_source_readback_request(request)
        && valid_readback_result(
            response.dispatch.as_ref(),
            response.source.as_ref(),
            request,
        )
        && json_within_limit(response, MAX_TEAM_PERMANENT_TRANSFER_SOURCE_RESPONSE_BYTES)
}

#[must_use]
pub fn valid_team_permanent_transfer_source_reconcile_response(
    response: &TeamPermanentTransferSourceReconcileResponse,
    request: &TeamPermanentTransferSourceReconcileRequest,
) -> bool {
    valid_team_permanent_transfer_source_readback_response(response, request)
}

/// Compare an immutable local dispatch with the exact original public request.
/// The current reader may be different and may have a newer Workspace revision.
#[must_use]
pub fn valid_team_permanent_transfer_source_dispatch_recovery(
    original: &TeamPermanentTransferSourceDispatchRequest,
    stored: &TeamPermanentTransferSourceDispatchRecordWire,
) -> bool {
    valid_team_permanent_transfer_source_dispatch_request(original)
        && valid_team_permanent_transfer_source_dispatch_record_for_request(stored, original)
}

#[must_use]
pub fn valid_team_permanent_transfer_source_dispatch_record(
    record: &TeamPermanentTransferSourceDispatchRecordWire,
) -> bool {
    let original = reconstructed_dispatch_request(record);
    valid_team_permanent_transfer_source_dispatch_record_for_request(record, &original)
}

fn valid_team_permanent_transfer_source_dispatch_record_for_request(
    record: &TeamPermanentTransferSourceDispatchRecordWire,
    request: &TeamPermanentTransferSourceDispatchRequest,
) -> bool {
    if !valid_team_permanent_transfer_source_dispatch_request(request)
        || record.workspace_id != request.actor.scope.workspace_id
        || record.preparation_id != request.command.preparation_id
        || record.transfer_id != request.command.transfer_id
        || record.generation != request.command.expected_generation
        || record.expected_workspace_leadership_revision
            != request.command.expected_workspace_leadership_revision
        || record.request_id != request.command.request_id
        || record.actor.activity_id != request.actor.activity_id
        || record.actor.session_id != request.actor.session_id
        || record.actor.agent_id
            != heterarchy_orchestration_runtime::leadership::member_agent_id(
                request.actor.scope.workspace_id,
                request.actor.activity_id,
                request.actor.session_id,
            )
            .into_uuid()
        || !record
            .source_scope
            .matches_actor_scope(&request.actor.scope)
        || !valid_actor_receipt(&record.actor, record.workspace_id)
    {
        return false;
    }
    valid_source_request_and_command(record, request)
}

fn reconstructed_dispatch_request(
    record: &TeamPermanentTransferSourceDispatchRecordWire,
) -> TeamPermanentTransferSourceDispatchRequest {
    TeamPermanentTransferSourceDispatchRequest {
        actor: TeamExpansionActorWire {
            scope: crate::wire::WorkspaceQueryScope {
                workspace_id: record.source_scope.workspace_id,
                location_id: record.source_scope.location_id,
                location_revision: record.source_scope.location_revision,
                workspace_revision: record.source_scope.workspace_revision,
                locator: record.source_scope.locator.clone(),
            },
            activity_id: record.actor.activity_id,
            session_id: record.actor.session_id,
        },
        command: TeamPermanentTransferSourceDispatchActionWire {
            request_id: record.request_id,
            preparation_id: record.preparation_id,
            transfer_id: record.transfer_id,
            expected_generation: record.generation,
            expected_workspace_leadership_revision: record.expected_workspace_leadership_revision,
        },
    }
}

fn valid_readback_result(
    dispatch: Option<&TeamPermanentTransferSourceDispatchRecordWire>,
    source: Option<&TeamPermanentTransferSourceSnapshotWire>,
    request: &TeamPermanentTransferSourceReadbackRequest,
) -> bool {
    match (dispatch, source) {
        (None, None) => true,
        (None, Some(_)) => false,
        (Some(dispatch), source) => {
            dispatch.workspace_id == request.workspace_id
                && dispatch.preparation_id == request.preparation_id
                && valid_team_permanent_transfer_source_dispatch_record(dispatch)
                && source.is_none_or(|source| {
                    valid_team_permanent_transfer_source_snapshot(source, dispatch)
                })
        }
    }
}

fn valid_team_permanent_transfer_source_snapshot(
    snapshot: &TeamPermanentTransferSourceSnapshotWire,
    dispatch: &TeamPermanentTransferSourceDispatchRecordWire,
) -> bool {
    if snapshot.snapshot_base64.is_empty()
        || snapshot.snapshot_base64.len() > MAX_TEAM_PERMANENT_TRANSFER_SOURCE_RESPONSE_BYTES
        || snapshot.local_recorded_at_ms < dispatch.recorded_at_ms
        || snapshot.envelope_digest != dispatch.envelope_digest
        || !valid_digest(&snapshot.snapshot_sha256)
        || !valid_digest(&snapshot.source_request_digest)
        || !valid_digest(&snapshot.delegation_digest)
        || !valid_digest(&snapshot.outcome_digest)
        || !valid_digest(&snapshot.payload_digest)
    {
        return false;
    }
    let Some(snapshot_bytes) = decode_base64(
        &snapshot.snapshot_base64,
        MAX_TEAM_PERMANENT_TRANSFER_SOURCE_SNAPSHOT_BYTES,
    ) else {
        return false;
    };
    let Some(expected_snapshot_digest) = decode_digest(&snapshot.snapshot_sha256) else {
        return false;
    };
    let actual_snapshot_digest: [u8; 32] = Sha256::digest(&snapshot_bytes).into();
    if actual_snapshot_digest != expected_snapshot_digest {
        return false;
    }
    let Ok(source_record) = HomeTransferSourceRecord::from_snapshot_bytes(&snapshot_bytes) else {
        return false;
    };
    let Ok(canonical_snapshot) = source_record.to_snapshot_bytes() else {
        return false;
    };
    if canonical_snapshot != snapshot_bytes {
        return false;
    }
    let Some(source_request) = source_request_from_dispatch(dispatch) else {
        return false;
    };
    let snapshot_inventory_digest = match snapshot.inventory_digest.as_deref() {
        Some(digest) => {
            let Some(digest) = decode_digest(digest) else {
                return false;
            };
            if digest == [0; 32] {
                return false;
            }
            Some(digest)
        }
        None => None,
    };
    snapshot.workspace_id == source_record.request().scope().workspace_id().into_uuid()
        && snapshot.workspace_id == dispatch.workspace_id
        && snapshot.preparation_id == source_record.request().witness().preparation_id()
        && snapshot.preparation_id == dispatch.preparation_id
        && snapshot.transfer_id == source_record.request().subject().transfer_id()
        && snapshot.transfer_id == dispatch.transfer_id
        && snapshot.generation == source_record.request().subject().transfer_generation()
        && snapshot.generation == dispatch.generation
        && snapshot.source_request_digest
            == lower_hex(source_record.request().request_digest().as_bytes())
        && source_record.request() == &source_request
        && snapshot.issuer_id == source_record.issuer().into_uuid()
        && snapshot.issuer_id == dispatch.issuer_id
        && snapshot.state == state_wire(source_record.state())
        && snapshot.observed_at_ms == source_record.observed_at().get()
        && snapshot.grant_issued_at_ms == source_record.grant_issued_at().get()
        && snapshot.grant_expires_at_ms == source_record.grant_expires_at().get()
        && snapshot.migration_id == source_record.migration_id()
        && snapshot.freeze_revision == source_record.freeze_revision()
        && snapshot.delegation_digest == lower_hex(source_record.delegation_digest())
        && snapshot_inventory_digest == source_record.inventory_digest().copied()
        && snapshot.outcome_digest == lower_hex(source_record.outcome_digest())
}

fn state_wire(state: HomeTransferSourceState) -> TeamPermanentTransferSourceStateWire {
    match state {
        HomeTransferSourceState::Prepared => TeamPermanentTransferSourceStateWire::Prepared,
        HomeTransferSourceState::AbortedUnstarted => {
            TeamPermanentTransferSourceStateWire::AbortedUnstarted
        }
    }
}

fn source_request_from_dispatch(
    dispatch: &TeamPermanentTransferSourceDispatchRecordWire,
) -> Option<HomeTransferSourceRequest> {
    let bytes = decode_base64(
        &dispatch.source_request_envelope_base64,
        MAX_TEAM_PERMANENT_TRANSFER_SOURCE_REQUEST_ENVELOPE_BYTES,
    )?;
    let request = decode_home_transfer_source_request(&bytes).ok()?;
    if encode_home_transfer_source_request(&request)
        .ok()?
        .as_slice()
        != bytes.as_slice()
    {
        return None;
    }
    Some(request)
}

fn valid_source_request_and_command(
    dispatch: &TeamPermanentTransferSourceDispatchRecordWire,
    request: &TeamPermanentTransferSourceDispatchRequest,
) -> bool {
    if !valid_source_dispatch_metadata(dispatch, request) {
        return false;
    }
    let Some(source_scope) = source_scope_from_dispatch(dispatch, request) else {
        return false;
    };
    let Some(source_request) = source_request_from_dispatch(dispatch) else {
        return false;
    };
    source_request_matches_dispatch(dispatch, request, &source_request)
        && reconstructed_command_matches_envelope(dispatch, source_scope, source_request)
}

fn valid_source_dispatch_metadata(
    dispatch: &TeamPermanentTransferSourceDispatchRecordWire,
    request: &TeamPermanentTransferSourceDispatchRequest,
) -> bool {
    dispatch.workspace_id == request.actor.scope.workspace_id
        && !dispatch.workspace_id.is_nil()
        && !dispatch.preparation_id.is_nil()
        && !dispatch.transfer_id.is_nil()
        && dispatch.generation > 0
        && i64::try_from(dispatch.generation).is_ok()
        && dispatch.expected_workspace_leadership_revision > 0
        && !dispatch.request_id.is_nil()
        && !dispatch.backend_id.is_nil()
        && !dispatch.issuer_id.is_nil()
        && !dispatch.service_destination_id.is_nil()
        && dispatch.recorded_at_ms >= 0
        && dispatch.grant_issued_at_ms >= 0
        && dispatch.grant_expires_at_ms > dispatch.grant_issued_at_ms
        && dispatch.recorded_at_ms >= dispatch.grant_issued_at_ms
        && dispatch.recorded_at_ms < dispatch.grant_expires_at_ms
        && valid_digest(&dispatch.intent_digest)
        && valid_digest(&dispatch.preparation_digest)
        && valid_digest(&dispatch.envelope_digest)
        && valid_digest(&dispatch.payload_digest)
        && valid_actor_receipt(&dispatch.actor, dispatch.workspace_id)
        && dispatch.actor.activity_id == request.actor.activity_id
        && dispatch.actor.session_id == request.actor.session_id
        && dispatch.actor.agent_id
            == heterarchy_orchestration_runtime::leadership::member_agent_id(
                request.actor.scope.workspace_id,
                request.actor.activity_id,
                request.actor.session_id,
            )
            .into_uuid()
        && !dispatch.source_route.is_empty()
        && dispatch.source_route.len() <= 128
}

fn source_scope_from_dispatch(
    dispatch: &TeamPermanentTransferSourceDispatchRecordWire,
    request: &TeamPermanentTransferSourceDispatchRequest,
) -> Option<WorkspaceProjectScope> {
    let source_scope = dispatch.source_scope.to_operations()?;
    (source_scope.workspace_id() == dispatch.workspace_id
        && source_scope.workspace_id() == request.actor.scope.workspace_id
        && dispatch
            .source_scope
            .matches_actor_scope(&request.actor.scope))
    .then_some(source_scope)
}

fn source_request_matches_dispatch(
    dispatch: &TeamPermanentTransferSourceDispatchRecordWire,
    request: &TeamPermanentTransferSourceDispatchRequest,
    source_request: &HomeTransferSourceRequest,
) -> bool {
    let scope = source_request.scope();
    let subject = source_request.subject();
    let witness = source_request.witness();
    scope.workspace_id().into_uuid() == dispatch.workspace_id
        && scope.workspace_revision().get() == dispatch.source_scope.workspace_revision
        && scope.backend_id().into_uuid() == dispatch.backend_id
        && scope.destination_id() == dispatch.service_destination_id
        && subject.transfer_id() == dispatch.transfer_id
        && subject.transfer_generation() == dispatch.generation
        && witness.preparation_id() == dispatch.preparation_id
        && lower_hex(witness.intent_digest()) == dispatch.intent_digest
        && lower_hex(witness.preparation_digest()) == dispatch.preparation_digest
        && dispatch.actor.activity_id == request.actor.activity_id
        && dispatch.actor.session_id == request.actor.session_id
}

fn reconstructed_command_matches_envelope(
    dispatch: &TeamPermanentTransferSourceDispatchRecordWire,
    source_scope: WorkspaceProjectScope,
    source_request: HomeTransferSourceRequest,
) -> bool {
    let Ok(route) = SocketAddr::from_str(&dispatch.source_route) else {
        return false;
    };
    let Ok(body_byte_count) = usize::try_from(dispatch.wire_identity.body_byte_count) else {
        return false;
    };
    if !route.ip().is_loopback()
        || route.port() == 0
        || route.to_string() != dispatch.source_route
        || dispatch.wire_identity.transport_request_id.is_nil()
        || body_byte_count == 0
        || body_byte_count > MAX_SOURCE_HTTP_BODY_BYTES
        || !valid_digest(&dispatch.wire_identity.body_sha256)
    {
        return false;
    }
    let Some(envelope_digest) = decode_digest(&dispatch.envelope_digest) else {
        return false;
    };
    let Some(grant_issued_at) = TimestampMillis::new(dispatch.grant_issued_at_ms).ok() else {
        return false;
    };
    let Some(grant_expires_at) = TimestampMillis::new(dispatch.grant_expires_at_ms).ok() else {
        return false;
    };
    let Ok(grant) = HomeTransferSourceGrant::new(
        OrchestrationIssuerId::from_uuid(dispatch.issuer_id),
        HomeTransferSourceAction::Prepare,
        source_request.request_digest(),
        grant_issued_at,
        grant_expires_at,
    ) else {
        return false;
    };
    let Some(body_sha256) = decode_digest(&dispatch.wire_identity.body_sha256) else {
        return false;
    };
    let Ok(wire_identity) = TeamPermanentTransferSourceWireIdentity::new(
        dispatch.wire_identity.transport_request_id,
        body_sha256,
        body_byte_count,
    ) else {
        return false;
    };
    let actor = OperationalActor::Participant {
        activity_id: ActivityId::from_uuid(dispatch.actor.activity_id),
        agent_id: AgentInstanceId::from_uuid(dispatch.actor.agent_id),
        session_id: AgentSessionId::from_uuid(dispatch.actor.session_id),
    };
    let Ok(command) = TeamPermanentTransferSourceDispatchCommand::new(
        WorkspaceId::from_uuid(dispatch.workspace_id),
        dispatch.preparation_id,
        dispatch.transfer_id,
        dispatch.generation,
        dispatch.expected_workspace_leadership_revision,
        dispatch.request_id,
        actor,
        route,
        source_scope,
        ForgeBackendId::from_uuid(dispatch.backend_id),
        OrchestrationIssuerId::from_uuid(dispatch.issuer_id),
        dispatch.service_destination_id,
        source_request,
        grant,
        wire_identity,
    ) else {
        return false;
    };
    command.envelope_digest().as_bytes() == &envelope_digest
}

fn scope_from_actor(actor: &TeamExpansionActorWire) -> Option<WorkspaceProjectScope> {
    WorkspaceProjectScope::new(
        actor.scope.workspace_id,
        actor.scope.location_id,
        actor.scope.location_revision,
        actor.scope.workspace_revision,
        actor.scope.locator.clone(),
    )
    .ok()
}

fn valid_digest(value: &str) -> bool {
    decode_digest(value).is_some_and(|digest| digest != [0; 32])
}

fn decode_digest(value: &str) -> Option<[u8; 32]> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return None;
    }
    let mut digest = [0; 32];
    for (index, pair) in value.as_bytes().as_chunks::<2>().0.iter().enumerate() {
        digest[index] = (hex_nibble(pair[0])? << 4) | hex_nibble(pair[1])?;
    }
    Some(digest)
}

fn hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        _ => None,
    }
}

fn lower_hex(bytes: &[u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(64);
    for byte in bytes {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0F)]));
    }
    output
}

fn decode_base64(value: &str, maximum: usize) -> Option<Vec<u8>> {
    if value.is_empty() || value.len() > MAX_TEAM_PERMANENT_TRANSFER_SOURCE_RESPONSE_BYTES {
        return None;
    }
    let decoded = URL_SAFE_NO_PAD.decode(value).ok()?;
    if decoded.is_empty() || decoded.len() > maximum || URL_SAFE_NO_PAD.encode(&decoded) != value {
        return None;
    }
    Some(decoded)
}

fn json_within_limit<T: Serialize>(value: &T, maximum: usize) -> bool {
    serde_json::to_vec(value).is_ok_and(|json| json.len() <= maximum)
}

#[cfg(test)]
/// Shared canonical dispatch fixture for sibling client-boundary tests.
///
/// # Errors
///
/// Returns an error if the canonical fixture cannot be built.
pub fn test_dispatch_fixture_parts() -> Result<
    (
        TeamPermanentTransferSourceDispatchRequest,
        TeamPermanentTransferSourceDispatchRecordWire,
    ),
    Box<dyn std::error::Error>,
> {
    tests::dispatch_fixture_parts()
}

#[cfg(test)]
mod tests {
    use base64::Engine;
    use forge_backend::{HomeTransferSourceRecordParts, HomeTransferSourceState};
    use forge_domain::{
        AuthenticatedPrincipal, HomeTransferSourceBinding, HomeTransferSourceScope,
        HomeTransferSourceSubject, HomeTransferSourceWitness, WorkspaceRevision,
    };
    use heterarchy_orchestration_operations::{
        OperationalActor, TeamPermanentTransferSourceDispatchCommand,
        TeamPermanentTransferSourceWireIdentity, WorkspaceProjectScope,
    };

    use super::*;

    type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

    struct Fixture {
        request: TeamPermanentTransferSourceDispatchRequest,
        source_request: HomeTransferSourceRequest,
        dispatch: TeamPermanentTransferSourceDispatchRecordWire,
    }

    fn id(value: u128) -> Uuid {
        Uuid::from_u128(value)
    }

    fn alter_first_hex(value: &mut String) {
        let replacement = if value.as_bytes().first() == Some(&b'0') {
            "1"
        } else {
            "0"
        };
        value.replace_range(0..1, replacement);
    }

    fn actor(workspace_revision: i64) -> TeamExpansionActorWire {
        TeamExpansionActorWire {
            scope: crate::wire::WorkspaceQueryScope {
                workspace_id: id(100),
                location_id: id(101),
                location_revision: 3,
                workspace_revision,
                locator: "project://unit-test".to_owned(),
            },
            activity_id: id(102),
            session_id: id(103),
        }
    }

    fn source_request() -> TestResult<HomeTransferSourceRequest> {
        let workspace_id = WorkspaceId::from_uuid(id(100));
        let session_id = AgentSessionId::from_uuid(id(206));
        let scope = HomeTransferSourceScope::new(
            workspace_id,
            WorkspaceRevision::new(10)?,
            ForgeBackendId::from_uuid(id(201)),
            id(202),
        )?;
        let subject =
            HomeTransferSourceSubject::new(id(106), 2, id(203), id(204), id(205), id(207), 4, 5)?;
        let witness = HomeTransferSourceWitness::new(id(105), [0x21; 32], [0x22; 32])?;
        let binding = HomeTransferSourceBinding::new(
            ActivityId::from_uuid(id(208)),
            AgentInstanceId::from_uuid(id(209)),
            session_id,
            AuthenticatedPrincipal::TrustedStdio,
        )?;
        Ok(HomeTransferSourceRequest::new(
            witness,
            scope,
            subject,
            vec![session_id],
            vec![binding],
        )?)
    }

    fn fixture() -> TestResult<Fixture> {
        let actor = actor(10);
        let request = TeamPermanentTransferSourceDispatchRequest {
            actor: actor.clone(),
            command: TeamPermanentTransferSourceDispatchActionWire {
                request_id: id(104),
                preparation_id: id(105),
                transfer_id: id(106),
                expected_generation: 2,
                expected_workspace_leadership_revision: 7,
            },
        };
        let source_request = source_request()?;
        let route: SocketAddr = "127.0.0.1:8061".parse()?;
        let source_scope = WorkspaceProjectScope::new(
            actor.scope.workspace_id,
            actor.scope.location_id,
            actor.scope.location_revision,
            actor.scope.workspace_revision,
            actor.scope.locator.clone(),
        )?;
        let actor_id = heterarchy_orchestration_runtime::leadership::member_agent_id(
            actor.scope.workspace_id,
            actor.activity_id,
            actor.session_id,
        );
        let source_issuer = OrchestrationIssuerId::from_uuid(id(203));
        let grant = HomeTransferSourceGrant::new(
            source_issuer,
            HomeTransferSourceAction::Prepare,
            source_request.request_digest(),
            TimestampMillis::new(300)?,
            TimestampMillis::new(400)?,
        )?;
        let wire_identity = TeamPermanentTransferSourceWireIdentity::new(id(210), [0x33; 32], 128)?;
        let command = TeamPermanentTransferSourceDispatchCommand::new(
            workspace_id(),
            id(105),
            id(106),
            2,
            7,
            id(104),
            OperationalActor::Participant {
                activity_id: ActivityId::from_uuid(actor.activity_id),
                agent_id: actor_id,
                session_id: AgentSessionId::from_uuid(actor.session_id),
            },
            route,
            source_scope.clone(),
            ForgeBackendId::from_uuid(id(201)),
            source_issuer,
            id(202),
            source_request.clone(),
            grant,
            wire_identity,
        )?;
        let source_request_envelope =
            forge_backend::encode_home_transfer_source_request(&source_request)?;
        let dispatch = TeamPermanentTransferSourceDispatchRecordWire {
            workspace_id: id(100),
            preparation_id: id(105),
            transfer_id: id(106),
            generation: 2,
            intent_digest: lower_hex(source_request.witness().intent_digest()),
            preparation_digest: lower_hex(source_request.witness().preparation_digest()),
            expected_workspace_leadership_revision: 7,
            request_id: id(104),
            actor: TeamExpansionActorReceiptWire {
                activity_id: actor.activity_id,
                agent_id: actor_id.into_uuid(),
                session_id: actor.session_id,
                participation_id: id(211),
                profile_id: id(212),
                participant_id: id(213),
                display_name: "Transfer operator".to_owned(),
            },
            source_route: route.to_string(),
            source_scope: TeamPermanentTransferSourceScopeWire::from_operations(&source_scope),
            backend_id: id(201),
            issuer_id: source_issuer.into_uuid(),
            service_destination_id: id(202),
            source_request_envelope_base64: URL_SAFE_NO_PAD.encode(&source_request_envelope),
            envelope_digest: lower_hex(command.envelope_digest().as_bytes()),
            grant_issued_at_ms: 300,
            grant_expires_at_ms: 400,
            wire_identity: TeamPermanentTransferSourceWireIdentityWire {
                transport_request_id: id(210),
                body_sha256: lower_hex(&[0x33; 32]),
                body_byte_count: 128,
            },
            recorded_at_ms: 320,
            payload_digest: lower_hex(&[0x44; 32]),
        };
        Ok(Fixture {
            request,
            source_request,
            dispatch,
        })
    }

    pub(super) fn dispatch_fixture_parts() -> TestResult<(
        TeamPermanentTransferSourceDispatchRequest,
        TeamPermanentTransferSourceDispatchRecordWire,
    )> {
        let fixture = fixture()?;
        Ok((fixture.request, fixture.dispatch))
    }

    fn workspace_id() -> WorkspaceId {
        WorkspaceId::from_uuid(id(100))
    }

    fn aborted_source_record(fixture: &Fixture) -> TestResult<HomeTransferSourceRecord> {
        let mut parts = HomeTransferSourceRecordParts {
            request: fixture.source_request.clone(),
            issuer: OrchestrationIssuerId::from_uuid(fixture.dispatch.issuer_id),
            state: HomeTransferSourceState::AbortedUnstarted,
            observed_at: TimestampMillis::new(150)?,
            grant_issued_at: TimestampMillis::new(100)?,
            grant_expires_at: TimestampMillis::new(200)?,
            migration_id: id(214),
            freeze_revision: 1,
            delegation_digest: [0x55; 32],
            inventory_payload: None,
            inventory_digest: None,
            outcome_digest: [0; 32],
        };
        parts.outcome_digest = source_outcome_digest(&parts);
        Ok(HomeTransferSourceRecord::from_parts(parts)?)
    }

    fn source_outcome_digest(parts: &HomeTransferSourceRecordParts) -> [u8; 32] {
        let mut digest = Sha256::new();
        digest.update(b"forge.home-transfer.source.outcome.v1\0");
        digest.update(parts.request.request_digest().as_bytes());
        digest.update(parts.issuer.into_uuid().as_bytes());
        digest.update(1_i64.to_be_bytes());
        digest.update(parts.observed_at.get().to_be_bytes());
        digest.update(parts.grant_issued_at.get().to_be_bytes());
        digest.update(parts.grant_expires_at.get().to_be_bytes());
        digest.update(parts.migration_id.as_bytes());
        digest.update(parts.freeze_revision.to_be_bytes());
        digest.update(parts.delegation_digest);
        digest.update([0]);
        digest.finalize().into()
    }

    fn snapshot_wire(
        fixture: &Fixture,
        source_record: &HomeTransferSourceRecord,
    ) -> TestResult<TeamPermanentTransferSourceSnapshotWire> {
        let snapshot = source_record.to_snapshot_bytes()?;
        let snapshot_sha256: [u8; 32] = Sha256::digest(&snapshot).into();
        Ok(TeamPermanentTransferSourceSnapshotWire {
            snapshot_base64: URL_SAFE_NO_PAD.encode(&snapshot),
            snapshot_sha256: lower_hex(&snapshot_sha256),
            workspace_id: source_record.request().scope().workspace_id().into_uuid(),
            preparation_id: source_record.request().witness().preparation_id(),
            transfer_id: source_record.request().subject().transfer_id(),
            generation: source_record.request().subject().transfer_generation(),
            source_request_digest: lower_hex(source_record.request().request_digest().as_bytes()),
            issuer_id: source_record.issuer().into_uuid(),
            state: state_wire(source_record.state()),
            observed_at_ms: source_record.observed_at().get(),
            grant_issued_at_ms: source_record.grant_issued_at().get(),
            grant_expires_at_ms: source_record.grant_expires_at().get(),
            migration_id: source_record.migration_id(),
            freeze_revision: source_record.freeze_revision(),
            delegation_digest: lower_hex(source_record.delegation_digest()),
            inventory_digest: source_record.inventory_digest().map(lower_hex),
            outcome_digest: lower_hex(source_record.outcome_digest()),
            local_recorded_at_ms: 330,
            envelope_digest: fixture.dispatch.envelope_digest.clone(),
            payload_digest: lower_hex(&[0x66; 32]),
        })
    }

    #[test]
    fn dispatch_requires_exact_original_ids_pins_and_recomputed_envelope() -> TestResult {
        let fixture = fixture()?;
        let response = TeamPermanentTransferSourceDispatchResponse {
            request: fixture.request.clone(),
            disposition: TeamPermanentTransferSourceDispatchDispositionWire::Created,
            dispatch: fixture.dispatch.clone(),
            source: None,
        };
        assert!(valid_team_permanent_transfer_source_dispatch_response(
            &response,
            &fixture.request
        ));

        let mut changed_request = response.clone();
        changed_request.request.command.transfer_id = id(999);
        assert!(!valid_team_permanent_transfer_source_dispatch_response(
            &changed_request,
            &fixture.request
        ));

        let mut changed_workspace_pin = response.clone();
        changed_workspace_pin
            .dispatch
            .source_scope
            .workspace_revision += 1;
        assert!(!valid_team_permanent_transfer_source_dispatch_response(
            &changed_workspace_pin,
            &fixture.request
        ));

        let mut changed_envelope = response.clone();
        alter_first_hex(&mut changed_envelope.dispatch.envelope_digest);
        assert!(!valid_team_permanent_transfer_source_dispatch_response(
            &changed_envelope,
            &fixture.request
        ));

        let mut uppercase_digest = response.clone();
        uppercase_digest.dispatch.payload_digest = "A".repeat(64);
        assert!(!valid_team_permanent_transfer_source_dispatch_response(
            &uppercase_digest,
            &fixture.request
        ));

        let mut noncanonical_request = response;
        noncanonical_request
            .dispatch
            .source_request_envelope_base64
            .push('=');
        assert!(!valid_team_permanent_transfer_source_dispatch_response(
            &noncanonical_request,
            &fixture.request
        ));
        Ok(())
    }

    #[test]
    fn source_snapshot_preserves_original_window_and_allows_new_reader_revision() -> TestResult {
        let fixture = fixture()?;
        let source_record = aborted_source_record(&fixture)?;
        let snapshot = snapshot_wire(&fixture, &source_record)?;
        // The local dispatch grant window (300..400) deliberately differs from
        // the immutable Source record's original window (100..200).
        assert_ne!(
            fixture.dispatch.grant_issued_at_ms,
            snapshot.grant_issued_at_ms
        );
        assert_ne!(
            fixture.dispatch.grant_expires_at_ms,
            snapshot.grant_expires_at_ms
        );
        let mut current_actor = fixture.request.actor.clone();
        current_actor.scope.workspace_revision = 11;
        let request = TeamPermanentTransferSourceReadbackRequest {
            workspace_id: id(100),
            preparation_id: id(105),
            current_actor,
        };
        let response = TeamPermanentTransferSourceReadbackResponse {
            request: request.clone(),
            dispatch: Some(fixture.dispatch.clone()),
            source: Some(snapshot),
        };
        assert!(valid_team_permanent_transfer_source_readback_response(
            &response, &request
        ));
        assert!(valid_team_permanent_transfer_source_dispatch_recovery(
            &fixture.request,
            response.dispatch.as_ref().ok_or("dispatch missing")?,
        ));

        let mut wrong_hash = response.clone();
        alter_first_hex(
            &mut wrong_hash
                .source
                .as_mut()
                .ok_or("snapshot missing")?
                .snapshot_sha256,
        );
        assert!(!valid_team_permanent_transfer_source_readback_response(
            &wrong_hash,
            &request
        ));

        let mut wrong_snapshot = response.clone();
        let encoded = &mut wrong_snapshot
            .source
            .as_mut()
            .ok_or("snapshot missing")?
            .snapshot_base64;
        let replacement = if encoded.as_bytes().first() == Some(&b'A') {
            "B"
        } else {
            "A"
        };
        encoded.replace_range(0..1, replacement);
        assert!(!valid_team_permanent_transfer_source_readback_response(
            &wrong_snapshot,
            &request
        ));

        let mut wrong_state = response;
        wrong_state.source.as_mut().ok_or("snapshot missing")?.state =
            TeamPermanentTransferSourceStateWire::Prepared;
        assert!(!valid_team_permanent_transfer_source_readback_response(
            &wrong_state,
            &request
        ));
        Ok(())
    }

    #[test]
    fn no_dispatch_means_no_source_snapshot_and_payload_caps_are_combined() -> TestResult {
        let fixture = fixture()?;
        let mut current_actor = fixture.request.actor.clone();
        current_actor.scope.workspace_revision = 11;
        let request = TeamPermanentTransferSourceReadbackRequest {
            workspace_id: id(100),
            preparation_id: id(105),
            current_actor,
        };
        let no_attempt = TeamPermanentTransferSourceReadbackResponse {
            request: request.clone(),
            dispatch: None,
            source: None,
        };
        assert!(valid_team_permanent_transfer_source_readback_response(
            &no_attempt,
            &request
        ));
        let orphan_source = TeamPermanentTransferSourceReadbackResponse {
            request: request.clone(),
            dispatch: None,
            source: Some(TeamPermanentTransferSourceSnapshotWire {
                snapshot_base64: "A".to_owned(),
                snapshot_sha256: "00".repeat(32),
                workspace_id: id(100),
                preparation_id: id(105),
                transfer_id: id(106),
                generation: 2,
                source_request_digest: "11".repeat(32),
                issuer_id: id(203),
                state: TeamPermanentTransferSourceStateWire::AbortedUnstarted,
                observed_at_ms: 2,
                grant_issued_at_ms: 1,
                grant_expires_at_ms: 3,
                migration_id: id(214),
                freeze_revision: 1,
                delegation_digest: "22".repeat(32),
                inventory_digest: None,
                outcome_digest: "33".repeat(32),
                local_recorded_at_ms: 330,
                envelope_digest: "44".repeat(32),
                payload_digest: "55".repeat(32),
            }),
        };
        assert!(!valid_team_permanent_transfer_source_readback_response(
            &orphan_source,
            &request
        ));

        let mut oversized = TeamPermanentTransferSourceDispatchResponse {
            request: fixture.request.clone(),
            disposition: TeamPermanentTransferSourceDispatchDispositionWire::Created,
            dispatch: fixture.dispatch,
            source: None,
        };
        oversized.dispatch.source_request_envelope_base64 =
            "A".repeat(MAX_TEAM_PERMANENT_TRANSFER_SOURCE_RESPONSE_BYTES + 1);
        assert!(!valid_team_permanent_transfer_source_dispatch_response(
            &oversized,
            &fixture.request
        ));
        Ok(())
    }
}
