//! Checked Operations-owned Home acceptance records.

use forge_domain::{
    AgentAssignmentId, AgentAssignmentRevision, AgentAssignmentState, AgentSessionId,
    AgentSessionRevision, AuthenticatedPrincipal, ForgeBackendId, HomeHandoffAcceptanceGrant,
    HomeHandoffTakeRequest, HomeHandoffWorkAuthority, OrchestrationIssuerId, TimestampMillis,
    WorkspaceId,
};
use forge_persistence::{HomeHandoffAcceptanceOutcomeKind, HomeHandoffProposalRecord};
use heterarchy_orchestration_sessions::{
    AgentProfileId, AgentProfileRevision, HomeAcceptanceProfilePinReference,
    HomeAcceptanceSessionHoldReference,
};
use uuid::Uuid;

use crate::{NodeTeamBindingScope, OperationsError, WorkspaceParticipationId};

/// The configured Source identity trusted by the native Runtime composition.
/// Values copied from a take request are checked against this identity and the
/// actual stored Prepared Source receipt; they never select the owner.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HomeAcceptanceSourceIdentity {
    issuer: OrchestrationIssuerId,
    backend: ForgeBackendId,
    destination: Uuid,
}

impl HomeAcceptanceSourceIdentity {
    /// Construct the configured private Source owner tuple.
    ///
    /// # Errors
    /// Rejects nil owner identities.
    pub fn new(
        issuer: OrchestrationIssuerId,
        backend: ForgeBackendId,
        destination: Uuid,
    ) -> Result<Self, OperationsError> {
        if issuer.into_uuid().is_nil() || backend.into_uuid().is_nil() || destination.is_nil() {
            return Err(OperationsError::Bounds("Home acceptance Source identity"));
        }
        Ok(Self {
            issuer,
            backend,
            destination,
        })
    }

    pub const fn issuer(self) -> OrchestrationIssuerId {
        self.issuer
    }

    pub const fn backend(self) -> ForgeBackendId {
        self.backend
    }

    #[must_use]
    pub const fn destination(self) -> Uuid {
        self.destination
    }
}

/// Exact staged Source take intent, or a proposal-less terminal-only tombstone.
///
/// The proposal is returned as checked persisted data. It authenticates no
/// Source read; Runtime must fetch the live proposal through its configured
/// authenticated Source client before staging or admission.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HomeAcceptanceIntentReceipt {
    identity: HomeAcceptanceSourceIdentity,
    request: HomeHandoffTakeRequest,
    proposal: Option<HomeHandoffProposalRecord>,
    predecessor_acceptance_id: Option<Uuid>,
    predecessor_source_digest: Option<[u8; 32]>,
    intent_digest: [u8; 32],
    recorded_at: TimestampMillis,
    replayed: bool,
}

impl HomeAcceptanceIntentReceipt {
    #[allow(
        clippy::too_many_arguments,
        reason = "one immutable staged or terminal-only intent identity"
    )]
    pub(crate) const fn from_parts(
        identity: HomeAcceptanceSourceIdentity,
        request: HomeHandoffTakeRequest,
        proposal: Option<&HomeHandoffProposalRecord>,
        predecessor_acceptance_id: Option<Uuid>,
        predecessor_source_digest: Option<[u8; 32]>,
        intent_digest: [u8; 32],
        recorded_at: TimestampMillis,
        replayed: bool,
    ) -> Self {
        Self {
            identity,
            request,
            proposal: proposal.copied(),
            predecessor_acceptance_id,
            predecessor_source_digest,
            intent_digest,
            recorded_at,
            replayed,
        }
    }

    pub const fn identity(&self) -> HomeAcceptanceSourceIdentity {
        self.identity
    }

    #[must_use]
    pub const fn acceptance_id(&self) -> Uuid {
        self.request.acceptance_id()
    }

    #[must_use]
    pub const fn workspace_id(&self) -> WorkspaceId {
        self.request.scope().workspace_id()
    }

    pub const fn request(&self) -> HomeHandoffTakeRequest {
        self.request
    }

    #[must_use]
    pub const fn proposal(&self) -> Option<HomeHandoffProposalRecord> {
        self.proposal
    }

    #[must_use]
    pub const fn predecessor_acceptance_id(&self) -> Option<Uuid> {
        self.predecessor_acceptance_id
    }

    #[must_use]
    pub const fn predecessor_source_digest(&self) -> Option<[u8; 32]> {
        self.predecessor_source_digest
    }

    #[must_use]
    pub const fn intent_digest(&self) -> [u8; 32] {
        self.intent_digest
    }

    #[must_use]
    pub const fn recorded_at(&self) -> TimestampMillis {
        self.recorded_at
    }

    #[must_use]
    pub const fn replayed(&self) -> bool {
        self.replayed
    }
}

/// Durable admission receipt and the exact Source grant returned after commit.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HomeAcceptanceAdmissionReceipt {
    intent: HomeAcceptanceIntentReceipt,
    receipt_id: Uuid,
    receipt_digest: [u8; 32],
    moving_profile_id: Uuid,
    recipient_profile_id: AgentProfileId,
    recipient_participant_id: Uuid,
    recipient_agent_id: forge_domain::AgentInstanceId,
    recipient_session_id: AgentSessionId,
    recipient_participation_id: WorkspaceParticipationId,
    recipient_participation_revision: u64,
    recipient_membership_revision: u64,
    current_node_binding_revision: Option<u64>,
    current_node_binding_digest: [u8; 32],
    session_hold: HomeAcceptanceSessionHoldObservation,
    profile_pin: HomeAcceptanceProfilePinObservation,
    work_authority: HomeHandoffWorkAuthority,
    grant: HomeHandoffAcceptanceGrant,
    admitted_at: TimestampMillis,
    replayed: bool,
}

impl HomeAcceptanceAdmissionReceipt {
    #[allow(
        clippy::too_many_arguments,
        reason = "one checked constructor captures the complete immutable admission tuple"
    )]
    pub(crate) fn from_parts(
        intent: &HomeAcceptanceIntentReceipt,
        receipt_id: Uuid,
        receipt_digest: [u8; 32],
        moving_profile_id: Uuid,
        recipient_profile_id: AgentProfileId,
        recipient_participant_id: Uuid,
        recipient_agent_id: forge_domain::AgentInstanceId,
        recipient_session_id: AgentSessionId,
        recipient_participation_id: WorkspaceParticipationId,
        recipient_participation_revision: u64,
        recipient_membership_revision: u64,
        current_node_binding_revision: Option<u64>,
        current_node_binding_digest: [u8; 32],
        session_hold: HomeAcceptanceSessionHoldObservation,
        profile_pin: HomeAcceptanceProfilePinObservation,
        work_authority: HomeHandoffWorkAuthority,
        grant: &HomeHandoffAcceptanceGrant,
        admitted_at: TimestampMillis,
        replayed: bool,
    ) -> Self {
        Self {
            intent: *intent,
            receipt_id,
            receipt_digest,
            moving_profile_id,
            recipient_profile_id,
            recipient_participant_id,
            recipient_agent_id,
            recipient_session_id,
            recipient_participation_id,
            recipient_participation_revision,
            recipient_membership_revision,
            current_node_binding_revision,
            current_node_binding_digest,
            session_hold,
            profile_pin,
            work_authority,
            grant: *grant,
            admitted_at,
            replayed,
        }
    }

    pub const fn intent(&self) -> &HomeAcceptanceIntentReceipt {
        &self.intent
    }

    #[must_use]
    pub const fn acceptance_id(&self) -> Uuid {
        self.intent.acceptance_id()
    }

    #[must_use]
    pub const fn receipt_id(&self) -> Uuid {
        self.receipt_id
    }

    #[must_use]
    pub const fn receipt_digest(&self) -> [u8; 32] {
        self.receipt_digest
    }

    #[must_use]
    pub const fn moving_profile_id(&self) -> Uuid {
        self.moving_profile_id
    }

    #[must_use]
    pub const fn recipient_profile_id(&self) -> AgentProfileId {
        self.recipient_profile_id
    }

    #[must_use]
    pub const fn recipient_participant_id(&self) -> Uuid {
        self.recipient_participant_id
    }

    #[must_use]
    pub const fn recipient_agent_id(&self) -> forge_domain::AgentInstanceId {
        self.recipient_agent_id
    }

    pub const fn recipient_session_id(&self) -> AgentSessionId {
        self.recipient_session_id
    }

    #[must_use]
    pub const fn recipient_participation_id(&self) -> WorkspaceParticipationId {
        self.recipient_participation_id
    }

    #[must_use]
    pub const fn recipient_participation_revision(&self) -> u64 {
        self.recipient_participation_revision
    }

    #[must_use]
    pub const fn recipient_membership_revision(&self) -> u64 {
        self.recipient_membership_revision
    }

    #[must_use]
    pub const fn current_node_binding_revision(&self) -> Option<u64> {
        self.current_node_binding_revision
    }

    #[must_use]
    pub const fn current_node_binding_digest(&self) -> [u8; 32] {
        self.current_node_binding_digest
    }

    pub const fn session_hold(&self) -> HomeAcceptanceSessionHoldObservation {
        self.session_hold
    }

    pub const fn profile_pin(&self) -> HomeAcceptanceProfilePinObservation {
        self.profile_pin
    }

    pub const fn work_authority(&self) -> HomeHandoffWorkAuthority {
        self.work_authority
    }

    pub const fn grant(&self) -> HomeHandoffAcceptanceGrant {
        self.grant
    }

    #[must_use]
    pub const fn admitted_at(&self) -> TimestampMillis {
        self.admitted_at
    }

    #[must_use]
    pub const fn replayed(&self) -> bool {
        self.replayed
    }
}

/// Admission may replay the original receipt or observe an earlier absorbing
/// Source abort. A resolution never recreates an admission.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(
    clippy::large_enum_variant,
    reason = "the accepted owner API returns the exact durable receipt by value; boxing would change its outcome shape"
)]
pub enum HomeAcceptanceAdmissionOutcome {
    Admitted(HomeAcceptanceAdmissionReceipt),
    Resolved(HomeAcceptanceResolutionReceipt),
}

/// Absorbing Operations readback bound to the exact Source terminal payload.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HomeAcceptanceResolutionReceipt {
    identity: HomeAcceptanceSourceIdentity,
    request: HomeHandoffTakeRequest,
    resolution_id: Uuid,
    resolution_digest: [u8; 32],
    source_payload_digest: [u8; 32],
    outcome: HomeHandoffAcceptanceOutcomeKind,
    admission: Option<HomeAcceptanceAdmissionReceipt>,
    resolved_at: TimestampMillis,
    replayed: bool,
}

impl HomeAcceptanceResolutionReceipt {
    #[allow(
        clippy::too_many_arguments,
        reason = "one checked constructor captures an absorbing Source resolution"
    )]
    pub(crate) const fn from_parts(
        identity: HomeAcceptanceSourceIdentity,
        request: HomeHandoffTakeRequest,
        resolution_id: Uuid,
        resolution_digest: [u8; 32],
        source_payload_digest: [u8; 32],
        outcome: HomeHandoffAcceptanceOutcomeKind,
        admission: Option<&HomeAcceptanceAdmissionReceipt>,
        resolved_at: TimestampMillis,
        replayed: bool,
    ) -> Self {
        Self {
            identity,
            request,
            resolution_id,
            resolution_digest,
            source_payload_digest,
            outcome,
            admission: admission.copied(),
            resolved_at,
            replayed,
        }
    }

    pub const fn identity(&self) -> HomeAcceptanceSourceIdentity {
        self.identity
    }

    #[must_use]
    pub const fn acceptance_id(&self) -> Uuid {
        self.request.acceptance_id()
    }

    pub const fn request(&self) -> HomeHandoffTakeRequest {
        self.request
    }

    #[must_use]
    pub const fn resolution_id(&self) -> Uuid {
        self.resolution_id
    }

    #[must_use]
    pub const fn resolution_digest(&self) -> [u8; 32] {
        self.resolution_digest
    }

    #[must_use]
    pub const fn source_payload_digest(&self) -> [u8; 32] {
        self.source_payload_digest
    }

    pub const fn outcome(&self) -> HomeHandoffAcceptanceOutcomeKind {
        self.outcome
    }

    #[must_use]
    pub const fn admission(&self) -> Option<HomeAcceptanceAdmissionReceipt> {
        self.admission
    }

    #[must_use]
    pub const fn resolved_at(&self) -> TimestampMillis {
        self.resolved_at
    }

    #[must_use]
    pub const fn replayed(&self) -> bool {
        self.replayed
    }
}

/// Operations' immutable observation of the exact Session hold consumed by
/// admission. It is a persisted comparison value, not a Session-owner credential.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HomeAcceptanceSessionHoldObservation {
    acceptance_id: Uuid,
    request_digest: [u8; 32],
    session_id: AgentSessionId,
    principal: AuthenticatedPrincipal,
    session_revision: AgentSessionRevision,
    acquired_at: TimestampMillis,
}

impl HomeAcceptanceSessionHoldObservation {
    pub(crate) const fn from_owner(value: HomeAcceptanceSessionHoldReference) -> Self {
        Self {
            acceptance_id: value.acceptance_id(),
            request_digest: value.request_digest(),
            session_id: value.session_id(),
            principal: value.principal(),
            session_revision: value.session_revision(),
            acquired_at: value.acquired_at(),
        }
    }

    pub(crate) const fn from_parts(
        acceptance_id: Uuid,
        request_digest: [u8; 32],
        session_id: AgentSessionId,
        principal: AuthenticatedPrincipal,
        session_revision: AgentSessionRevision,
        acquired_at: TimestampMillis,
    ) -> Self {
        Self {
            acceptance_id,
            request_digest,
            session_id,
            principal,
            session_revision,
            acquired_at,
        }
    }

    #[must_use]
    pub const fn acceptance_id(self) -> Uuid {
        self.acceptance_id
    }

    #[must_use]
    pub const fn request_digest(self) -> [u8; 32] {
        self.request_digest
    }

    pub const fn session_id(self) -> AgentSessionId {
        self.session_id
    }

    pub const fn principal(self) -> AuthenticatedPrincipal {
        self.principal
    }

    pub const fn session_revision(self) -> AgentSessionRevision {
        self.session_revision
    }

    #[must_use]
    pub const fn acquired_at(self) -> TimestampMillis {
        self.acquired_at
    }
}

/// Operations' immutable observation of the exact Profile pin consumed by
/// admission. It is not proof that the Profile catalog remains bound or live.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HomeAcceptanceProfilePinObservation {
    profile_id: AgentProfileId,
    profile_revision: AgentProfileRevision,
    logical_session_request_id: Uuid,
    linked_profile_revision: AgentProfileRevision,
    linked_session_revision: AgentSessionRevision,
    acquired_at: TimestampMillis,
}

impl HomeAcceptanceProfilePinObservation {
    pub(crate) const fn from_owner(value: HomeAcceptanceProfilePinReference) -> Self {
        Self {
            profile_id: value.profile_id(),
            profile_revision: value.profile_revision(),
            logical_session_request_id: value.logical_session_request_id(),
            linked_profile_revision: value.linked_profile_revision(),
            linked_session_revision: value.linked_session_revision(),
            acquired_at: value.acquired_at(),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) const fn from_parts(
        profile_id: AgentProfileId,
        profile_revision: AgentProfileRevision,
        logical_session_request_id: Uuid,
        linked_profile_revision: AgentProfileRevision,
        linked_session_revision: AgentSessionRevision,
        acquired_at: TimestampMillis,
    ) -> Self {
        Self {
            profile_id,
            profile_revision,
            logical_session_request_id,
            linked_profile_revision,
            linked_session_revision,
            acquired_at,
        }
    }

    #[must_use]
    pub const fn profile_id(self) -> AgentProfileId {
        self.profile_id
    }

    #[must_use]
    pub const fn profile_revision(self) -> AgentProfileRevision {
        self.profile_revision
    }

    #[must_use]
    pub const fn logical_session_request_id(self) -> Uuid {
        self.logical_session_request_id
    }

    #[must_use]
    pub const fn linked_profile_revision(self) -> AgentProfileRevision {
        self.linked_profile_revision
    }

    pub const fn linked_session_revision(self) -> AgentSessionRevision {
        self.linked_session_revision
    }

    #[must_use]
    pub const fn acquired_at(self) -> TimestampMillis {
        self.acquired_at
    }
}

/// Durable state of the Home successor's distinct Node work reservation.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HomeAcceptanceWorkReservationState {
    Pending,
    Active,
    Aborted,
    Terminal,
}

impl HomeAcceptanceWorkReservationState {
    pub(crate) const fn encode(self) -> i64 {
        match self {
            Self::Pending => 0,
            Self::Active => 1,
            Self::Aborted => 2,
            Self::Terminal => 3,
        }
    }

    pub(crate) fn decode(value: i64) -> Result<Self, OperationsError> {
        match value {
            0 => Ok(Self::Pending),
            1 => Ok(Self::Active),
            2 => Ok(Self::Aborted),
            3 => Ok(Self::Terminal),
            _ => Err(OperationsError::Corrupt("Home work reservation state")),
        }
    }
}

/// Immutable successor lineage and current active-work reservation readback.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HomeAcceptanceWorkReservationReceipt {
    acceptance_id: Uuid,
    workspace_id: WorkspaceId,
    node_scope: NodeTeamBindingScope,
    root_assignment_id: AgentAssignmentId,
    successor_assignment_id: AgentAssignmentId,
    team_id: Option<Uuid>,
    state: HomeAcceptanceWorkReservationState,
    created_at: TimestampMillis,
    updated_at: TimestampMillis,
    terminal_source_digest: Option<[u8; 32]>,
    terminal_assignment_state: Option<AgentAssignmentState>,
    terminal_assignment_revision: Option<AgentAssignmentRevision>,
    terminal_assignment_cancellation_requested: Option<bool>,
    terminal_assignment_digest: Option<[u8; 32]>,
    terminal_assignment_updated_at: Option<TimestampMillis>,
    replayed: bool,
}

impl HomeAcceptanceWorkReservationReceipt {
    #[allow(
        clippy::too_many_arguments,
        reason = "one decoded work reservation projection preserves the complete terminal evidence"
    )]
    pub(crate) const fn from_parts(
        acceptance_id: Uuid,
        workspace_id: WorkspaceId,
        node_scope: NodeTeamBindingScope,
        root_assignment_id: AgentAssignmentId,
        successor_assignment_id: AgentAssignmentId,
        team_id: Option<Uuid>,
        state: HomeAcceptanceWorkReservationState,
        created_at: TimestampMillis,
        updated_at: TimestampMillis,
        terminal_source_digest: Option<[u8; 32]>,
        terminal_assignment_state: Option<AgentAssignmentState>,
        terminal_assignment_revision: Option<AgentAssignmentRevision>,
        terminal_assignment_cancellation_requested: Option<bool>,
        terminal_assignment_digest: Option<[u8; 32]>,
        terminal_assignment_updated_at: Option<TimestampMillis>,
        replayed: bool,
    ) -> Self {
        Self {
            acceptance_id,
            workspace_id,
            node_scope,
            root_assignment_id,
            successor_assignment_id,
            team_id,
            state,
            created_at,
            updated_at,
            terminal_source_digest,
            terminal_assignment_state,
            terminal_assignment_revision,
            terminal_assignment_cancellation_requested,
            terminal_assignment_digest,
            terminal_assignment_updated_at,
            replayed,
        }
    }

    #[must_use]
    pub const fn acceptance_id(&self) -> Uuid {
        self.acceptance_id
    }

    #[must_use]
    pub const fn workspace_id(&self) -> WorkspaceId {
        self.workspace_id
    }

    pub const fn node_scope(&self) -> NodeTeamBindingScope {
        self.node_scope
    }

    #[must_use]
    pub const fn root_assignment_id(&self) -> AgentAssignmentId {
        self.root_assignment_id
    }

    #[must_use]
    pub const fn successor_assignment_id(&self) -> AgentAssignmentId {
        self.successor_assignment_id
    }

    #[must_use]
    pub const fn team_id(&self) -> Option<Uuid> {
        self.team_id
    }

    pub const fn state(&self) -> HomeAcceptanceWorkReservationState {
        self.state
    }

    #[must_use]
    pub const fn created_at(&self) -> TimestampMillis {
        self.created_at
    }

    #[must_use]
    pub const fn updated_at(&self) -> TimestampMillis {
        self.updated_at
    }

    #[must_use]
    pub const fn terminal_source_digest(&self) -> Option<[u8; 32]> {
        self.terminal_source_digest
    }

    #[must_use]
    pub const fn terminal_assignment_state(&self) -> Option<AgentAssignmentState> {
        self.terminal_assignment_state
    }

    #[must_use]
    pub const fn terminal_assignment_revision(&self) -> Option<AgentAssignmentRevision> {
        self.terminal_assignment_revision
    }

    #[must_use]
    pub const fn terminal_assignment_cancellation_requested(&self) -> Option<bool> {
        self.terminal_assignment_cancellation_requested
    }

    #[must_use]
    pub const fn terminal_assignment_digest(&self) -> Option<[u8; 32]> {
        self.terminal_assignment_digest
    }

    #[must_use]
    pub const fn terminal_assignment_updated_at(&self) -> Option<TimestampMillis> {
        self.terminal_assignment_updated_at
    }

    #[must_use]
    pub const fn replayed(&self) -> bool {
        self.replayed
    }
}

/// Coherent exact readback used for process-restart recovery.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HomeAcceptanceRecord {
    intent: HomeAcceptanceIntentReceipt,
    admission: Option<HomeAcceptanceAdmissionReceipt>,
    resolution: Option<HomeAcceptanceResolutionReceipt>,
    work_reservation: Option<HomeAcceptanceWorkReservationReceipt>,
}

impl HomeAcceptanceRecord {
    pub(crate) fn from_parts(
        intent: &HomeAcceptanceIntentReceipt,
        admission: Option<&HomeAcceptanceAdmissionReceipt>,
        resolution: Option<&HomeAcceptanceResolutionReceipt>,
        work_reservation: Option<&HomeAcceptanceWorkReservationReceipt>,
    ) -> Self {
        Self {
            intent: *intent,
            admission: admission.copied(),
            resolution: resolution.cloned(),
            work_reservation: work_reservation.cloned(),
        }
    }

    pub const fn intent(&self) -> &HomeAcceptanceIntentReceipt {
        &self.intent
    }

    #[must_use]
    pub const fn admission(&self) -> Option<HomeAcceptanceAdmissionReceipt> {
        self.admission
    }

    #[must_use]
    pub const fn resolution(&self) -> Option<&HomeAcceptanceResolutionReceipt> {
        self.resolution.as_ref()
    }

    #[must_use]
    pub const fn work_reservation(&self) -> Option<&HomeAcceptanceWorkReservationReceipt> {
        self.work_reservation.as_ref()
    }
}

/// Bounded unresolved Operations Home records in acceptance-ID order.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HomeAcceptancePendingPage {
    records: Vec<HomeAcceptanceRecord>,
    next_after: Option<Uuid>,
}

impl HomeAcceptancePendingPage {
    pub fn records(&self) -> &[HomeAcceptanceRecord] {
        &self.records
    }

    #[must_use]
    pub const fn next_after(&self) -> Option<Uuid> {
        self.next_after
    }

    pub(crate) const fn from_parts(
        records: Vec<HomeAcceptanceRecord>,
        next_after: Option<Uuid>,
    ) -> Self {
        Self {
            records,
            next_after,
        }
    }
}
