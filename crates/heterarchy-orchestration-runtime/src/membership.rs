//! Transport-neutral owner for Profile-linked Workspace membership stages.
//!
//! Source validation and the Forge reservation/activation calls stay in the
//! host composition. Runtime owns the existing Operations and Profile journal
//! transitions and accepts original typed Forge receipts or checked, read-only
//! source observations.

mod identity;

use forge_domain::{
    AgentDeclaredRole, AgentDisplayName, AgentSessionId, AgentSessionState, AgentTransportKind,
    AuthenticatedPrincipal, RegisteredAgentSession, TimestampMillis, WorkspaceId,
};
use heterarchy_orchestration_operations::{
    AgentBindingActivation, AgentBindingReservation, AgentBindingReservationRequest,
    BindingActivationObservation, BindingRejectionAttemptDisposition,
    BindingRejectionAttemptRequest, BindingRejectionAttemptResolution, BindingRejectionObservation,
    BindingReservationObservation, MembershipCommitReceipt, OperationsCatalog, OperationsError,
    RegistrationActivationReceipt, RegistrationBindingRejectionReceipt, RegistrationPhase,
    RegistrationProfileScope, RegistrationProgress, RegistrationRequest,
    ResolveBindingRejectionAttempt, WorkspaceParticipationId,
    WorkspaceParticipationReplacementActivationReceipt,
    WorkspaceParticipationReplacementCommitReceipt, WorkspaceParticipationReplacementProgress,
    WorkspaceParticipationSessionExpectation, WorkspaceParticipationSessionRebindCommand,
};
use heterarchy_orchestration_sessions::AgentProfileLogicalSessionLink;
use uuid::Uuid;

use crate::{OrchestrationRuntime, RuntimeError};

/// Exact Activity membership target. The caller's Session and principal are
/// validated separately by each stage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MembershipTarget {
    pub workspace_id: Uuid,
    pub activity_id: Uuid,
    pub session_id: Uuid,
}

impl MembershipTarget {
    /// Construct one non-nil Workspace/Activity/Session target.
    ///
    /// # Errors
    /// Returns invalid input when any identity is nil.
    pub fn new(
        workspace_id: Uuid,
        activity_id: Uuid,
        session_id: Uuid,
    ) -> Result<Self, RuntimeError> {
        let target = Self {
            workspace_id,
            activity_id,
            session_id,
        };
        validate_target(target)?;
        Ok(target)
    }
}

/// Derive the stable membership request identity shared with MCP and Backend
/// adapters. The order and domain are a durable replay contract.
#[must_use]
pub fn member_registration_request_id(
    workspace_id: Uuid,
    activity_id: Uuid,
    session_id: Uuid,
) -> Uuid {
    identity::request_id(workspace_id, activity_id, session_id)
}

/// Metadata for one normal or Profile-replacement registration proposal.
#[derive(Debug, Clone)]
pub struct MemberRegisterCommand {
    pub target: MembershipTarget,
    pub principal: AuthenticatedPrincipal,
    pub display_name: String,
    pub declared_role: String,
    pub transport: AgentTransportKind,
    pub migration_id: Uuid,
    pub now_ms: i64,
}

#[derive(Debug, Clone, Copy)]
struct MemberRegistrationReplayContext {
    target: MembershipTarget,
    migration_id: Uuid,
    profile_link: Option<AgentProfileLogicalSessionLink>,
    expected_participant_id: Uuid,
    native_participant_id: Uuid,
    principal: AuthenticatedPrincipal,
    transport: AgentTransportKind,
}

/// Full stored registration proposal, tagged with its owning journal.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MemberProgress {
    Normal(Box<RegistrationProgress>),
    Replacement(Box<WorkspaceParticipationReplacementProgress>),
}

impl MemberProgress {
    pub fn request(&self) -> &RegistrationRequest {
        match self {
            Self::Normal(value) => value.request(),
            Self::Replacement(value) => value.request(),
        }
    }

    pub fn phase(&self) -> RegistrationPhase {
        match self {
            Self::Normal(value) => value.phase(),
            Self::Replacement(value) => value.phase(),
        }
    }

    #[must_use]
    pub fn workspace_id(&self) -> WorkspaceId {
        match self {
            Self::Normal(value) => value.workspace_id(),
            Self::Replacement(value) => value.workspace_id(),
        }
    }

    #[must_use]
    pub fn participant_id(&self) -> Uuid {
        match self {
            Self::Normal(value) => value.participant_id(),
            Self::Replacement(value) => value.participant_id(),
        }
    }

    #[must_use]
    pub fn migration_id(&self) -> Uuid {
        match self {
            Self::Normal(value) => value.migration_id(),
            Self::Replacement(value) => value.migration_id(),
        }
    }

    #[must_use]
    pub fn reserved_at(&self) -> Option<TimestampMillis> {
        match self {
            Self::Normal(value) => value.reserved_at(),
            Self::Replacement(value) => value.reserved_at(),
        }
    }
}

/// Exact stored logical membership commit, without flattening replacement data.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MemberCommit {
    Normal(MembershipCommitReceipt),
    Replacement(WorkspaceParticipationReplacementCommitReceipt),
}

impl MemberCommit {
    #[must_use]
    pub const fn request_id(&self) -> Uuid {
        match self {
            Self::Normal(value) => value.request_id(),
            Self::Replacement(value) => value.request_id(),
        }
    }

    #[must_use]
    pub const fn workspace_id(&self) -> WorkspaceId {
        match self {
            Self::Normal(value) => value.workspace_id(),
            Self::Replacement(value) => value.workspace_id(),
        }
    }

    #[must_use]
    pub const fn participant_id(&self) -> Uuid {
        match self {
            Self::Normal(value) => value.participant_id(),
            Self::Replacement(value) => value.participant_id(),
        }
    }

    pub const fn binding(&self) -> &AgentBindingReservationRequest {
        match self {
            Self::Normal(value) => value.binding(),
            Self::Replacement(value) => value.binding(),
        }
    }

    #[must_use]
    pub const fn commit_digest(&self) -> [u8; 32] {
        match self {
            Self::Normal(value) => value.commit_digest(),
            Self::Replacement(value) => value.commit_digest(),
        }
    }

    #[must_use]
    pub const fn committed_at(&self) -> TimestampMillis {
        match self {
            Self::Normal(value) => value.committed_at(),
            Self::Replacement(value) => value.committed_at(),
        }
    }

    #[must_use]
    pub const fn joined_event_id(&self) -> Uuid {
        match self {
            Self::Normal(value) => value.joined_event_id(),
            Self::Replacement(value) => value.joined_event_id(),
        }
    }
}

/// Exact stored Forge activation witness, tagged with its owning journal.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MemberActivation {
    Normal(RegistrationActivationReceipt),
    Replacement(WorkspaceParticipationReplacementActivationReceipt),
}

impl MemberActivation {
    #[must_use]
    pub const fn request_id(&self) -> Uuid {
        match self {
            Self::Normal(value) => value.request_id(),
            Self::Replacement(value) => value.request_id(),
        }
    }

    #[must_use]
    pub const fn workspace_id(&self) -> WorkspaceId {
        match self {
            Self::Normal(value) => value.workspace_id(),
            Self::Replacement(value) => value.workspace_id(),
        }
    }

    #[must_use]
    pub const fn participant_id(&self) -> Uuid {
        match self {
            Self::Normal(value) => value.participant_id(),
            Self::Replacement(value) => value.participant_id(),
        }
    }

    pub const fn binding(&self) -> &AgentBindingReservationRequest {
        match self {
            Self::Normal(value) => value.binding(),
            Self::Replacement(value) => value.binding(),
        }
    }

    #[must_use]
    pub const fn membership_digest(&self) -> [u8; 32] {
        match self {
            Self::Normal(value) => value.membership_digest(),
            Self::Replacement(value) => value.membership_digest(),
        }
    }

    #[must_use]
    pub const fn activated_at(&self) -> TimestampMillis {
        match self {
            Self::Normal(value) => value.activated_at(),
            Self::Replacement(value) => value.activated_at(),
        }
    }
}

/// Stored commit and, only when already Ready, its original Forge witness.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemberActivationPlan {
    progress: MemberProgress,
    commit: MemberCommit,
    activation: Option<MemberActivation>,
}

/// Exact stored registration/commit input and optional terminal rejection.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemberBindingRejectionPlan {
    progress: MemberProgress,
    commit: MemberCommit,
    attempt: Option<BindingRejectionAttemptRequest>,
    rejection: Option<RegistrationBindingRejectionReceipt>,
}

/// Exact historical journal readback, independent of current participation.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemberRegistrationReadback {
    pub progress: Option<MemberProgress>,
    pub commit: Option<MemberCommit>,
    pub activation: Option<MemberActivation>,
}

impl MemberActivationPlan {
    pub const fn progress(&self) -> &MemberProgress {
        &self.progress
    }

    pub const fn commit(&self) -> &MemberCommit {
        &self.commit
    }
    #[must_use]
    pub const fn activation(&self) -> Option<&MemberActivation> {
        self.activation.as_ref()
    }
}

impl MemberBindingRejectionPlan {
    pub const fn progress(&self) -> &MemberProgress {
        &self.progress
    }
    pub const fn commit(&self) -> &MemberCommit {
        &self.commit
    }
    #[must_use = "the optional immutable Source attempt must be inspected"]
    pub const fn attempt(&self) -> Option<&BindingRejectionAttemptRequest> {
        self.attempt.as_ref()
    }
    #[must_use = "the optional terminal rejection must be inspected"]
    pub const fn rejection(&self) -> Option<&RegistrationBindingRejectionReceipt> {
        self.rejection.as_ref()
    }
}

impl OrchestrationRuntime {
    /// Prepare or replay one Profile-linked or native HTTP Session registration.
    ///
    /// # Errors
    /// Rejects an unowned/closed Session, unsupported unlinked transport, stale
    /// registration, invalid metadata, or durable catalog failure.
    pub fn prepare_member_registration(
        &self,
        command: MemberRegisterCommand,
    ) -> Result<MemberProgress, RuntimeError> {
        let target = command.target;
        validate_target(target)?;
        if command.migration_id.is_nil() {
            return Err(RuntimeError::InvalidInput);
        }
        let now = timestamp(command.now_ms)?;
        let verified_session = self.open_member_session(target, command.principal)?;
        let session_id = AgentSessionId::from_uuid(target.session_id);
        let profile_link = {
            let profiles = self.profile_catalog_guard()?;
            profiles.logical_session_link(session_id)?
        };
        let (native_participant, profile_participant) =
            member_registration_participant_ids(target, profile_link, command.transport)?;
        let request_id = member_registration_request_id(
            target.workspace_id,
            target.activity_id,
            target.session_id,
        );
        let display_name =
            AgentDisplayName::new(command.display_name).map_err(|_| RuntimeError::InvalidInput)?;
        let declared_role = AgentDeclaredRole::new(command.declared_role)
            .map_err(|_| RuntimeError::InvalidInput)?;
        let workspace_id = WorkspaceId::from_uuid(target.workspace_id);
        let session_id = AgentSessionId::from_uuid(target.session_id);
        let replay = MemberRegistrationReplayContext {
            target,
            migration_id: command.migration_id,
            profile_link,
            expected_participant_id: profile_participant,
            native_participant_id: native_participant,
            principal: command.principal,
            transport: command.transport,
        };

        self.require_open_session(target.session_id, command.principal)?;
        let mut catalog = self.catalog()?;
        if let Some(progress) = read_progress(&catalog, request_id)? {
            return existing_member_registration(
                &mut catalog,
                replay,
                &display_name,
                &declared_role,
                progress,
            );
        }

        validate_new_registration_transport(command.principal, command.transport)?;

        let replacement = if let Some(profile_link) = profile_link {
            catalog
                .workspace_participation_session_expectation(
                    workspace_id,
                    profile_link.profile_id(),
                )?
                .filter(|value| value.current_session_id() != session_id)
        } else {
            if let Some(identity) =
                catalog.logical_identity_for_session(workspace_id, session_id)?
                && identity.participant_id() != native_participant
            {
                return Err(RuntimeError::Conflict);
            }
            None
        };
        let participant_id = replacement
            .as_ref()
            .map_or(profile_participant, |value| value.participant_id());
        let request = identity::registration_request(
            target,
            participant_id,
            command.principal,
            display_name,
            declared_role,
            command.transport,
            now,
        )?;
        let progress = if let Some(profile_link) = profile_link {
            prepare_new_member_registration(
                &mut catalog,
                command.migration_id,
                target,
                profile_link,
                &verified_session,
                replacement,
                &request,
            )?
        } else {
            prepare_native_member_registration(
                &mut catalog,
                command.migration_id,
                target,
                &request,
            )?
        };
        if progress.participant_id() != participant_id {
            return Err(RuntimeError::Conflict);
        }
        Ok(progress)
    }

    /// Record the original Forge reservation witness against its exact proposal.
    ///
    /// # Errors
    /// Rejects a stale or foreign witness, an unowned/closed Session, or catalog failure.
    pub fn record_member_reservation(
        &self,
        target: MembershipTarget,
        principal: AuthenticatedPrincipal,
        reservation: &AgentBindingReservation,
    ) -> Result<MemberProgress, RuntimeError> {
        let observation = BindingReservationObservation::from_native(reservation)?;
        self.record_member_reservation_observation(target, principal, &observation)
    }

    /// Record an authenticated source HTTP readback through the same journal core.
    ///
    /// # Errors
    /// Rejects wrong target, closed/foreign Session or source/payload/replay disagreement.
    pub fn record_member_reservation_observation(
        &self,
        target: MembershipTarget,
        principal: AuthenticatedPrincipal,
        reservation: &BindingReservationObservation,
    ) -> Result<MemberProgress, RuntimeError> {
        validate_target(target)?;
        let _session = self.open_member_session(target, principal)?;
        let request_id = reservation.request().request_id();
        let progress = self
            .member_registration(target, principal, request_id)?
            .ok_or(RuntimeError::Conflict)?;
        validate_progress_target(&progress, target, request_id)?;
        self.require_open_session(target.session_id, principal)?;
        let mut catalog = self.catalog()?;
        let progress = match progress {
            MemberProgress::Normal(_) => MemberProgress::Normal(Box::new(
                catalog.record_registration_reservation_observation(reservation)?,
            )),
            MemberProgress::Replacement(_) => MemberProgress::Replacement(Box::new(
                catalog.record_workspace_participation_replacement_reservation_observation(
                    reservation,
                )?,
            )),
        };
        validate_progress_target(&progress, target, request_id)?;
        Ok(progress)
    }

    /// Commit one reservation or replay its exact stored membership receipt.
    ///
    /// Profile replacement commits require the original empty-recipient contract.
    ///
    /// # Errors
    /// Rejects an unowned/closed Session, wrong request/scope, stale phase, or catalog failure.
    pub fn confirm_member_registration(
        &self,
        target: MembershipTarget,
        principal: AuthenticatedPrincipal,
        request_id: Uuid,
        recipients: &[Uuid],
        now_ms: i64,
    ) -> Result<MemberCommit, RuntimeError> {
        validate_target(target)?;
        if request_id.is_nil() {
            return Err(RuntimeError::InvalidInput);
        }
        let observed_at = timestamp(now_ms)?;
        let _session = self.open_member_session(target, principal)?;
        let progress = self
            .member_registration(target, principal, request_id)?
            .ok_or(RuntimeError::Conflict)?;
        validate_progress_target(&progress, target, request_id)?;
        match progress {
            MemberProgress::Normal(progress) => {
                let commit = match progress.phase() {
                    RegistrationPhase::Reserved
                    | RegistrationPhase::MembershipCommitted
                    | RegistrationPhase::Ready => {
                        self.require_open_session(target.session_id, principal)?;
                        let mut catalog = self.catalog()?;
                        catalog.commit_registration_membership(
                            request_id,
                            recipients,
                            observed_at,
                        )?
                    }
                    RegistrationPhase::Pending => return Err(RuntimeError::Conflict),
                };
                let commit = MemberCommit::Normal(commit);
                if progress.phase() != RegistrationPhase::Ready {
                    self.ensure_profile_participation_anchor(
                        target,
                        request_id,
                        commit.participant_id(),
                        commit.committed_at(),
                    )?;
                }
                Ok(commit)
            }
            MemberProgress::Replacement(progress) => confirm_replacement_member_registration(
                self,
                target,
                principal,
                request_id,
                recipients,
                observed_at,
                progress.as_ref(),
            ),
        }
    }

    /// Build activation input only from durable commit evidence; Ready returns
    /// its original witness and never fabricates a replacement witness.
    ///
    /// # Errors
    /// Rejects an unowned/closed Session, foreign target, or a registration
    /// that has not committed membership.
    pub fn member_activation_plan(
        &self,
        target: MembershipTarget,
        principal: AuthenticatedPrincipal,
        request_id: Uuid,
    ) -> Result<MemberActivationPlan, RuntimeError> {
        validate_target(target)?;
        if request_id.is_nil() {
            return Err(RuntimeError::InvalidInput);
        }
        let _session = self.open_member_session(target, principal)?;
        let catalog = self.catalog()?;
        let progress = read_progress(&catalog, request_id)?.ok_or(RuntimeError::Conflict)?;
        validate_progress_target(&progress, target, request_id)?;
        if catalog
            .registration_binding_rejection(request_id)?
            .is_some()
        {
            return Err(RuntimeError::Conflict);
        }
        let (commit, activation) = match &progress {
            MemberProgress::Normal(value) => {
                let commit = catalog
                    .membership_commit(request_id)?
                    .ok_or(RuntimeError::Conflict)?;
                let activation = catalog.registration_activation(request_id)?;
                match value.phase() {
                    RegistrationPhase::MembershipCommitted if activation.is_none() => {}
                    RegistrationPhase::Ready if activation.is_some() => {}
                    _ => return Err(RuntimeError::Conflict),
                }
                (
                    MemberCommit::Normal(commit),
                    activation.map(MemberActivation::Normal),
                )
            }
            MemberProgress::Replacement(value) => {
                let commit = catalog
                    .workspace_participation_replacement_commit(request_id)?
                    .ok_or(RuntimeError::Conflict)?;
                let activation =
                    catalog.workspace_participation_replacement_activation(request_id)?;
                match value.phase() {
                    RegistrationPhase::MembershipCommitted if activation.is_none() => {}
                    RegistrationPhase::Ready if activation.is_some() => {}
                    _ => return Err(RuntimeError::Conflict),
                }
                (
                    MemberCommit::Replacement(commit),
                    activation.map(MemberActivation::Replacement),
                )
            }
        };
        validate_commit_target(&commit, target, request_id)?;
        if let Some(activation) = &activation {
            validate_activation_target(activation, target, request_id, &commit)?;
        }
        Ok(MemberActivationPlan {
            progress,
            commit,
            activation,
        })
    }

    /// Build the terminal rejection input from the exact durable registration.
    ///
    /// # Errors
    /// Rejects foreign Sessions, replacement journals, missing commits, Ready
    /// registrations, or conflicting durable evidence. Closed owners may use
    /// the plan only for historical terminal readback/reconciliation.
    pub fn member_binding_rejection_plan(
        &self,
        target: MembershipTarget,
        principal: AuthenticatedPrincipal,
        request_id: Uuid,
    ) -> Result<MemberBindingRejectionPlan, RuntimeError> {
        validate_target(target)?;
        if request_id.is_nil() {
            return Err(RuntimeError::InvalidInput);
        }
        let _session = self.resolve_owned_session(target.session_id, principal)?;
        let catalog = self.catalog()?;
        let progress = read_progress(&catalog, request_id)?.ok_or(RuntimeError::Conflict)?;
        validate_progress_target(&progress, target, request_id)?;
        let MemberProgress::Normal(_) = &progress else {
            return Err(RuntimeError::BindingRejectionTargetUnsupported);
        };
        if progress.phase() != RegistrationPhase::MembershipCommitted {
            return Err(RuntimeError::Conflict);
        }
        let commit = catalog
            .membership_commit(request_id)?
            .map(MemberCommit::Normal)
            .ok_or(RuntimeError::Conflict)?;
        validate_commit_target(&commit, target, request_id)?;
        if catalog.registration_activation(request_id)?.is_some() {
            return Err(RuntimeError::Conflict);
        }
        let rejection = catalog.registration_binding_rejection(request_id)?;
        let attempt = catalog.registration_binding_rejection_attempt(request_id)?;
        if rejection.is_some() && attempt.is_none() {
            return Err(RuntimeError::Conflict);
        }
        Ok(MemberBindingRejectionPlan {
            progress,
            commit,
            attempt,
            rejection,
        })
    }

    /// Durably freeze the first exact Source rejection dispatch intent.
    ///
    /// # Errors
    /// Rejects foreign/closed Sessions, changed source intent, unsupported
    /// replacement registrations, or conflicting terminal evidence.
    pub fn prepare_member_binding_rejection_attempt(
        &self,
        target: MembershipTarget,
        principal: AuthenticatedPrincipal,
        request: &BindingRejectionAttemptRequest,
    ) -> Result<BindingRejectionAttemptDisposition, RuntimeError> {
        self.require_operator(principal)?;
        let plan = self.member_binding_rejection_plan(target, principal, request.request_id())?;
        if plan.progress().request().binding().principal() != principal {
            return Err(RuntimeError::Forbidden);
        }
        self.require_open_session(target.session_id, principal)?;
        let mut catalog = self.catalog()?;
        catalog
            .prepare_registration_binding_rejection_attempt(request)
            .map_err(Into::into)
    }

    /// Persist or replay the trusted resolution of one legacy uncertain attempt.
    ///
    /// # Errors
    /// Rejects non-operators, untrusted artifacts, foreign Sessions, terminal
    /// evidence, changed attempt identity, or persistence failure.
    pub fn resolve_member_binding_rejection_attempt(
        &self,
        target: MembershipTarget,
        principal: AuthenticatedPrincipal,
        request: &ResolveBindingRejectionAttempt,
    ) -> Result<BindingRejectionAttemptResolution, RuntimeError> {
        self.require_operator(principal)?;
        let trusted_deployment = self
            .trusted_binding_rejection_legacy_deployments
            .iter()
            .copied()
            .find(|deployment| {
                deployment.artifact_digest() == request.expected_legacy_client_artifact_digest
            })
            .ok_or(RuntimeError::Forbidden)?;
        let plan = self.member_binding_rejection_plan(target, principal, request.request_id)?;
        if plan.attempt().is_none()
            || plan.rejection().is_some()
            || request.operator_principal != principal
        {
            return Err(RuntimeError::Conflict);
        }
        let mut catalog = self.catalog()?;
        catalog
            .resolve_registration_binding_rejection_attempt(request, trusted_deployment)
            .map_err(Into::into)
    }

    /// Consume one trusted resolution exactly once before Source dispatch.
    ///
    /// # Errors
    /// Rejects non-operators, closed or foreign Sessions, corrupt durable
    /// evidence, untrusted artifacts, repeated consumption, or persistence
    /// failure.
    pub fn consume_member_binding_rejection_resolution(
        &self,
        target: MembershipTarget,
        principal: AuthenticatedPrincipal,
        request_id: Uuid,
        consumed_at: TimestampMillis,
    ) -> Result<BindingRejectionAttemptDisposition, RuntimeError> {
        self.require_operator(principal)?;
        self.require_open_session(target.session_id, principal)?;
        let _plan = self.member_binding_rejection_plan(target, principal, request_id)?;
        let mut catalog = self.catalog()?;
        let Some(resolution) =
            catalog.registration_binding_rejection_attempt_resolution(request_id)?
        else {
            return Ok(BindingRejectionAttemptDisposition::ReadbackRequired);
        };
        let trusted_deployment = self
            .trusted_binding_rejection_legacy_deployments
            .iter()
            .copied()
            .find(|deployment| {
                deployment.artifact_digest() == resolution.legacy_client_artifact_digest()
            })
            .ok_or(RuntimeError::Forbidden)?;
        catalog
            .consume_registration_binding_rejection_resolution(
                request_id,
                principal,
                trusted_deployment,
                consumed_at,
            )
            .map_err(Into::into)
    }

    /// Record one authenticated Source terminal rejection observation.
    ///
    /// # Errors
    /// Rejects foreign Sessions, changed registration intent, activation
    /// conflicts, or mismatched Source evidence. An already-attempted Source
    /// receipt may be journalled after Session close.
    pub fn record_member_binding_rejection_observation(
        &self,
        target: MembershipTarget,
        principal: AuthenticatedPrincipal,
        rejection: &BindingRejectionObservation,
    ) -> Result<RegistrationBindingRejectionReceipt, RuntimeError> {
        validate_target(target)?;
        self.require_operator(principal)?;
        let _session = self.resolve_owned_session(target.session_id, principal)?;
        let request_id = rejection.request_id();
        let plan = self.member_binding_rejection_plan(target, principal, request_id)?;
        if plan.progress().request().binding().principal() != principal {
            return Err(RuntimeError::Forbidden);
        }
        let mut catalog = self.catalog()?;
        catalog
            .record_registration_binding_rejection_observation(rejection)
            .map_err(Into::into)
    }

    /// Read historical terminal rejection evidence for its authenticated owner.
    ///
    /// Closed Sessions remain readable; this method performs no mutation and
    /// grants no activation or successor authority.
    ///
    /// # Errors
    /// Rejects foreign identities or inconsistent durable evidence.
    pub fn member_binding_rejection_readback(
        &self,
        target: MembershipTarget,
        principal: AuthenticatedPrincipal,
        request_id: Uuid,
    ) -> Result<Option<RegistrationBindingRejectionReceipt>, RuntimeError> {
        validate_target(target)?;
        if request_id.is_nil() {
            return Err(RuntimeError::InvalidInput);
        }
        let _session = self.resolve_owned_session(target.session_id, principal)?;
        let catalog = self.catalog()?;
        let progress = read_progress(&catalog, request_id)?;
        if let Some(progress) = progress {
            validate_progress_target(&progress, target, request_id)?;
        }
        catalog
            .registration_binding_rejection(request_id)
            .map_err(Into::into)
    }

    /// Record the original Forge activation witness; only Operations can make
    /// the registration Ready.
    ///
    /// # Errors
    /// Rejects a foreign target/witness, an unowned/closed Session, stale phase,
    /// or catalog failure.
    pub fn record_member_activation(
        &self,
        target: MembershipTarget,
        principal: AuthenticatedPrincipal,
        activation: &AgentBindingActivation,
    ) -> Result<MemberActivation, RuntimeError> {
        let observation = BindingActivationObservation::from_native(activation)?;
        self.record_member_activation_observation(target, principal, &observation)
    }

    /// Journal the authenticated HTTP activation readback without constructing a Forge capability.
    ///
    /// # Errors
    /// Rejects wrong target, closed/foreign Session or source/digest/replay disagreement.
    pub fn record_member_activation_observation(
        &self,
        target: MembershipTarget,
        principal: AuthenticatedPrincipal,
        activation: &BindingActivationObservation,
    ) -> Result<MemberActivation, RuntimeError> {
        validate_target(target)?;
        let _session = self.open_member_session(target, principal)?;
        let request_id = activation.request_id();
        let progress = self
            .member_registration(target, principal, request_id)?
            .ok_or(RuntimeError::Conflict)?;
        validate_progress_target(&progress, target, request_id)?;
        let commit = {
            let catalog = self.catalog()?;
            match &progress {
                MemberProgress::Normal(_) => catalog
                    .membership_commit(request_id)?
                    .map(MemberCommit::Normal)
                    .ok_or(RuntimeError::Conflict)?,
                MemberProgress::Replacement(_) => catalog
                    .workspace_participation_replacement_commit(request_id)?
                    .map(MemberCommit::Replacement)
                    .ok_or(RuntimeError::Conflict)?,
            }
        };
        validate_commit_target(&commit, target, request_id)?;
        if !matches!(
            progress.phase(),
            RegistrationPhase::MembershipCommitted | RegistrationPhase::Ready
        ) {
            return Err(RuntimeError::Conflict);
        }
        self.require_open_session(target.session_id, principal)?;
        let mut catalog = self.catalog()?;
        let receipt = match progress {
            MemberProgress::Normal(_) => MemberActivation::Normal(
                catalog.record_registration_activation_observation(activation)?,
            ),
            MemberProgress::Replacement(_) => MemberActivation::Replacement(
                catalog.record_workspace_participation_replacement_activation_observation(
                    activation,
                )?,
            ),
        };
        validate_activation_target(&receipt, target, request_id, &commit)?;
        Ok(receipt)
    }

    /// Read one exact registration journal for the authenticated open Session.
    ///
    /// # Errors
    /// Rejects invalid identities or an unowned/closed Session.
    pub fn member_registration(
        &self,
        target: MembershipTarget,
        principal: AuthenticatedPrincipal,
        request_id: Uuid,
    ) -> Result<Option<MemberProgress>, RuntimeError> {
        validate_target(target)?;
        if request_id.is_nil() {
            return Err(RuntimeError::InvalidInput);
        }
        let _session = self.open_member_session(target, principal)?;
        let catalog = self.catalog()?;
        let progress = read_progress(&catalog, request_id)?;
        if let Some(progress) = &progress {
            validate_progress_target(progress, target, request_id)?;
        }
        Ok(progress)
    }

    /// Read immutable registration and activation evidence for the Session owner.
    ///
    /// A historical Ready receipt does not grant a current effect binding.
    ///
    /// # Errors
    /// Rejects invalid, foreign or closed Sessions and inconsistent journal evidence.
    pub fn member_registration_readback(
        &self,
        target: MembershipTarget,
        principal: AuthenticatedPrincipal,
        request_id: Uuid,
    ) -> Result<MemberRegistrationReadback, RuntimeError> {
        validate_target(target)?;
        if request_id.is_nil() {
            return Err(RuntimeError::InvalidInput);
        }
        let _session = self.open_member_session(target, principal)?;
        let catalog = self.catalog()?;
        let normal = catalog.registration_audit_progress(request_id)?;
        let replacement = catalog.workspace_participation_replacement_progress(request_id)?;
        let (progress, commit, activation) = match (normal, replacement) {
            (Some(_), Some(_)) => return Err(RuntimeError::Conflict),
            (Some(progress), None) => (
                Some(MemberProgress::Normal(Box::new(progress))),
                catalog
                    .membership_commit(request_id)?
                    .map(MemberCommit::Normal),
                catalog
                    .registration_activation(request_id)?
                    .map(MemberActivation::Normal),
            ),
            (None, Some(progress)) => (
                Some(MemberProgress::Replacement(Box::new(progress))),
                catalog
                    .workspace_participation_replacement_commit(request_id)?
                    .map(MemberCommit::Replacement),
                catalog
                    .workspace_participation_replacement_activation(request_id)?
                    .map(MemberActivation::Replacement),
            ),
            (None, None) => (None, None, None),
        };
        if let Some(progress) = &progress {
            validate_progress_target(progress, target, request_id)?;
        }
        if let Some(commit) = &commit {
            validate_commit_target(commit, target, request_id)?;
            if let Some(activation) = &activation {
                validate_activation_target(activation, target, request_id, commit)?;
            }
        } else if activation.is_some() {
            return Err(RuntimeError::Conflict);
        }
        Ok(MemberRegistrationReadback {
            progress,
            commit,
            activation,
        })
    }

    fn open_member_session(
        &self,
        target: MembershipTarget,
        principal: AuthenticatedPrincipal,
    ) -> Result<RegisteredAgentSession, RuntimeError> {
        self.resolve_owned_session(target.session_id, principal)
            .and_then(|session| {
                if session.state() == AgentSessionState::Open {
                    Ok(session)
                } else {
                    Err(RuntimeError::Forbidden)
                }
            })
    }

    fn ensure_profile_participation_anchor(
        &self,
        target: MembershipTarget,
        request_id: Uuid,
        expected_participant_id: Uuid,
        mapped_at: TimestampMillis,
    ) -> Result<(), RuntimeError> {
        let session_id = AgentSessionId::from_uuid(target.session_id);
        let workspace_id = WorkspaceId::from_uuid(target.workspace_id);
        let registration_scope = {
            let catalog = self.catalog()?;
            catalog.registration_profile_scope(request_id)?
        };
        let Some(registration_scope) = registration_scope else {
            if expected_participant_id
                != identity::native_session_participation_id(target.workspace_id, target.session_id)
            {
                return Err(OperationsError::Conflict(
                    "unattributed membership participant identity",
                )
                .into());
            }
            return Ok(());
        };
        let profile_link = {
            let profiles = self.profile_catalog_guard()?;
            profiles.logical_session_link(session_id)?
        }
        .ok_or(OperationsError::Conflict(
            "Profile registration link missing during membership confirm",
        ))?;
        if !registration_profile_scope_matches(registration_scope, target, request_id, profile_link)
            || expected_participant_id
                != identity::profile_participation_id(
                    target.workspace_id,
                    profile_link.profile_id().into_uuid(),
                )
        {
            return Err(OperationsError::Conflict(
                "Profile registration identity changed before membership confirm",
            )
            .into());
        }
        let participation_id = identity::profile_participation_id(
            target.workspace_id,
            profile_link.profile_id().into_uuid(),
        );
        let mapping_request_id =
            identity::profile_mapping_request_id(target.workspace_id, target.session_id);
        let mut catalog = self.catalog()?;
        let identity = catalog
            .logical_identity_for_session(workspace_id, session_id)?
            .ok_or(OperationsError::Conflict(
                "Profile-mapped logical member missing after membership commit",
            ))?;
        if identity.participant_id() != expected_participant_id {
            return Err(OperationsError::Conflict(
                "Profile-mapped participant differs from membership commit",
            )
            .into());
        }
        if let Some(participation) =
            catalog.workspace_participation(workspace_id, profile_link.profile_id())?
        {
            let initial_mapping = participation
                .session_mappings()
                .iter()
                .find(|mapping| mapping.session_id() == session_id);
            let rebound = if initial_mapping.is_none() {
                catalog
                    .workspace_participation_session_rebind_for_session(workspace_id, session_id)?
            } else {
                None
            };
            let mapped_participant = initial_mapping
                .map(|mapping| mapping.participant_id())
                .or_else(|| {
                    rebound.map(
                        heterarchy_orchestration_operations::WorkspaceParticipationSessionRebindReceipt::participant_id,
                    )
                });
            if mapped_participant != Some(expected_participant_id) {
                return Err(OperationsError::Conflict(
                    "Profile-mapped Session differs from existing WorkspaceParticipation",
                )
                .into());
            }
            return Ok(());
        }
        let participation_id = WorkspaceParticipationId::from_uuid(participation_id)?;
        let _mapped = catalog.map_workspace_participation(
            mapping_request_id,
            participation_id,
            profile_link,
            &identity,
            identity.membership_revision(),
            mapped_at,
        )?;
        Ok(())
    }
}

fn validate_new_registration_transport(
    principal: AuthenticatedPrincipal,
    transport: AgentTransportKind,
) -> Result<(), RuntimeError> {
    if principal.is_token_or_grant_authenticated() && !transport.is_retired_or_stream() {
        return Err(RuntimeError::InvalidInput);
    }
    Ok(())
}

fn member_registration_participant_ids(
    target: MembershipTarget,
    profile_link: Option<AgentProfileLogicalSessionLink>,
    transport: AgentTransportKind,
) -> Result<(Uuid, Uuid), RuntimeError> {
    let native_participant =
        identity::native_session_participation_id(target.workspace_id, target.session_id);
    if profile_link.is_none() && transport != AgentTransportKind::StreamableHttp {
        return Err(RuntimeError::InvalidInput);
    }
    let participant = profile_link.map_or(native_participant, |link| {
        identity::profile_participation_id(target.workspace_id, link.profile_id().into_uuid())
    });
    Ok((native_participant, participant))
}

fn existing_member_registration(
    catalog: &mut OperationsCatalog,
    replay: MemberRegistrationReplayContext,
    display_name: &AgentDisplayName,
    declared_role: &AgentDeclaredRole,
    progress: MemberProgress,
) -> Result<MemberProgress, RuntimeError> {
    let target = replay.target;
    let migration_id = replay.migration_id;
    let profile_link = replay.profile_link;
    let request_id = progress.request().binding().request_id();
    validate_progress_target(&progress, target, request_id)?;
    let request = progress.request();
    if request.display_name() != display_name
        || request.declared_role() != declared_role
        || request.transport() != replay.transport
        || request.binding().principal() != replay.principal
    {
        return Err(RuntimeError::Conflict);
    }
    if profile_link.is_none()
        && let Some(identity) = catalog.logical_identity_for_session(
            WorkspaceId::from_uuid(target.workspace_id),
            AgentSessionId::from_uuid(target.session_id),
        )?
        && identity.participant_id() != replay.native_participant_id
    {
        return Err(RuntimeError::Conflict);
    }
    match progress {
        MemberProgress::Normal(existing) => {
            if existing.participant_id() != replay.expected_participant_id {
                return Err(RuntimeError::Conflict);
            }
            let request = existing.request().clone();
            let progress = if let Some(profile_link) = profile_link {
                let scope = catalog.registration_profile_scope(request_id)?;
                if !scope.is_some_and(|scope| {
                    registration_profile_scope_matches(scope, target, request_id, profile_link)
                }) {
                    return Err(RuntimeError::Conflict);
                }
                catalog.prepare_profile_registration(migration_id, &request, profile_link)?
            } else {
                if catalog.registration_profile_scope(request_id)?.is_some() {
                    return Err(RuntimeError::Conflict);
                }
                catalog.prepare_registration(migration_id, &request)?
            };
            let progress = MemberProgress::Normal(Box::new(progress));
            validate_progress_target(&progress, target, request_id)?;
            Ok(progress)
        }
        MemberProgress::Replacement(progress) => {
            let Some(profile_link) = profile_link else {
                return Err(RuntimeError::Conflict);
            };
            if progress.migration_id() != migration_id
                || progress.profile_id() != profile_link.profile_id()
            {
                return Err(RuntimeError::Conflict);
            }
            Ok(MemberProgress::Replacement(progress))
        }
    }
}

fn registration_profile_scope_matches(
    scope: RegistrationProfileScope,
    target: MembershipTarget,
    request_id: Uuid,
    profile_link: AgentProfileLogicalSessionLink,
) -> bool {
    scope.request_id() == request_id
        && scope.workspace_id().into_uuid() == target.workspace_id
        && scope.profile_id() == profile_link.profile_id()
        && scope.profile_link_request_id() == profile_link.request_id()
        && scope.profile_revision() == profile_link.profile_revision()
        && scope.session_id() == profile_link.session_id()
        && scope.session_id().into_uuid() == target.session_id
        && scope.session_revision() == profile_link.session_revision()
}

fn prepare_native_member_registration(
    catalog: &mut OperationsCatalog,
    migration_id: Uuid,
    target: MembershipTarget,
    request: &RegistrationRequest,
) -> Result<MemberProgress, RuntimeError> {
    let request_id =
        member_registration_request_id(target.workspace_id, target.activity_id, target.session_id);
    let progress = MemberProgress::Normal(Box::new(
        catalog.prepare_registration(migration_id, request)?,
    ));
    validate_progress_target(&progress, target, request_id)?;
    Ok(progress)
}

fn prepare_new_member_registration(
    catalog: &mut OperationsCatalog,
    migration_id: Uuid,
    target: MembershipTarget,
    profile_link: AgentProfileLogicalSessionLink,
    verified_session: &RegisteredAgentSession,
    replacement: Option<WorkspaceParticipationSessionExpectation>,
    request: &RegistrationRequest,
) -> Result<MemberProgress, RuntimeError> {
    let request_id =
        member_registration_request_id(target.workspace_id, target.activity_id, target.session_id);
    let progress = if let Some(replacement) = replacement {
        MemberProgress::Replacement(Box::new(
            catalog.prepare_workspace_participation_replacement(
                migration_id,
                replacement,
                profile_link,
                verified_session,
                request,
            )?,
        ))
    } else {
        MemberProgress::Normal(Box::new(catalog.prepare_profile_registration(
            migration_id,
            request,
            profile_link,
        )?))
    };
    validate_progress_target(&progress, target, request_id)?;
    Ok(progress)
}

fn confirm_replacement_member_registration(
    runtime: &OrchestrationRuntime,
    target: MembershipTarget,
    principal: AuthenticatedPrincipal,
    request_id: Uuid,
    recipients: &[Uuid],
    observed_at: TimestampMillis,
    progress: &WorkspaceParticipationReplacementProgress,
) -> Result<MemberCommit, RuntimeError> {
    if !recipients.is_empty() {
        return Err(RuntimeError::Conflict);
    }
    match progress.phase() {
        RegistrationPhase::Reserved => {
            let session = runtime.open_member_session(target, principal)?;
            let session_id = AgentSessionId::from_uuid(target.session_id);
            let profile_link = {
                let profiles = runtime.profile_catalog_guard()?;
                profiles.logical_session_link(session_id)?
            }
            .ok_or(RuntimeError::Conflict)?;
            if profile_link.profile_id() != progress.profile_id() {
                return Err(RuntimeError::Conflict);
            }
            let rebind_request_id =
                identity::profile_rebind_request_id(target.workspace_id, target.session_id);
            runtime.require_open_session(target.session_id, principal)?;
            let rebind = {
                let mut catalog = runtime.catalog()?;
                if let Some(receipt) = catalog.workspace_participation_session_rebind_for_session(
                    progress.workspace_id(),
                    session_id,
                )? {
                    receipt
                } else {
                    let command = WorkspaceParticipationSessionRebindCommand::new(
                        rebind_request_id,
                        progress.workspace_id(),
                        progress.previous_session_id(),
                        progress.expected_participation_revision(),
                        observed_at,
                    )?;
                    catalog.rebind_workspace_participation_session(
                        command,
                        profile_link,
                        &session,
                    )?
                }
            };
            let mut catalog = runtime.catalog()?;
            let commit = catalog.commit_workspace_participation_replacement(
                request_id,
                rebind,
                &[],
                observed_at,
            )?;
            Ok(MemberCommit::Replacement(commit))
        }
        RegistrationPhase::MembershipCommitted | RegistrationPhase::Ready => {
            let catalog = runtime.catalog()?;
            catalog
                .workspace_participation_replacement_commit(request_id)?
                .map(MemberCommit::Replacement)
                .ok_or(RuntimeError::Conflict)
        }
        RegistrationPhase::Pending => Err(RuntimeError::Conflict),
    }
}

fn read_progress(
    catalog: &OperationsCatalog,
    request_id: Uuid,
) -> Result<Option<MemberProgress>, RuntimeError> {
    let normal = catalog.registration_progress(request_id)?;
    let replacement = catalog.workspace_participation_replacement_progress(request_id)?;
    match (normal, replacement) {
        (Some(_), Some(_)) => Err(RuntimeError::persistence(OperationsError::Corrupt(
            "member request exists in both registration journals",
        ))),
        (Some(progress), None) => Ok(Some(MemberProgress::Normal(Box::new(progress)))),
        (None, Some(progress)) => Ok(Some(MemberProgress::Replacement(Box::new(progress)))),
        (None, None) => Ok(None),
    }
}

fn validate_target(target: MembershipTarget) -> Result<(), RuntimeError> {
    if target.workspace_id.is_nil() || target.activity_id.is_nil() || target.session_id.is_nil() {
        return Err(RuntimeError::InvalidInput);
    }
    Ok(())
}

fn validate_progress_target(
    progress: &MemberProgress,
    target: MembershipTarget,
    request_id: Uuid,
) -> Result<(), RuntimeError> {
    let binding = progress.request().binding();
    if progress.workspace_id().into_uuid() != target.workspace_id
        || binding.request_id() != request_id
        || binding.activity_id().into_uuid() != target.activity_id
        || binding.session_id().into_uuid() != target.session_id
    {
        return Err(RuntimeError::Conflict);
    }
    Ok(())
}

fn validate_commit_target(
    commit: &MemberCommit,
    target: MembershipTarget,
    request_id: Uuid,
) -> Result<(), RuntimeError> {
    let binding = commit.binding();
    if commit.request_id() != request_id
        || commit.workspace_id().into_uuid() != target.workspace_id
        || binding.activity_id().into_uuid() != target.activity_id
        || binding.session_id().into_uuid() != target.session_id
    {
        return Err(RuntimeError::Conflict);
    }
    Ok(())
}

fn validate_activation_target(
    activation: &MemberActivation,
    target: MembershipTarget,
    request_id: Uuid,
    commit: &MemberCommit,
) -> Result<(), RuntimeError> {
    let binding = activation.binding();
    if activation.request_id() != request_id
        || activation.workspace_id().into_uuid() != target.workspace_id
        || binding.activity_id().into_uuid() != target.activity_id
        || binding.session_id().into_uuid() != target.session_id
        || activation.participant_id() != commit.participant_id()
        || activation.membership_digest() != commit.commit_digest()
    {
        return Err(RuntimeError::Conflict);
    }
    Ok(())
}

fn timestamp(value: i64) -> Result<TimestampMillis, RuntimeError> {
    TimestampMillis::new(value).map_err(|_| RuntimeError::InvalidInput)
}

#[cfg(test)]
mod tests {
    use std::fs;

    use forge_domain::{DirectTokenId, OAuthGrantId};

    use super::*;

    #[test]
    fn unlinked_streamable_session_is_not_rejected_for_missing_profile_link()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = std::env::temp_dir().join(format!(
            "orchestration-native-membership-{}",
            Uuid::now_v7()
        ));
        fs::create_dir(&root)?;
        let runtime = OrchestrationRuntime::open(&root)?;
        let principal =
            AuthenticatedPrincipal::DirectToken(DirectTokenId::from_uuid(Uuid::from_u128(0xCA11)));
        let session = runtime.open_session(Uuid::from_u128(0xCA12), principal, 1)?;
        let target = MembershipTarget::new(
            Uuid::from_u128(0xCA13),
            Uuid::from_u128(0xCA14),
            session.session.session_id,
        )?;
        assert!(
            runtime
                .profile_catalog_guard()?
                .logical_session_link(AgentSessionId::from_uuid(session.session.session_id))?
                .is_none()
        );

        let invalid = runtime.prepare_member_registration(MemberRegisterCommand {
            target,
            principal,
            display_name: " ".to_owned(),
            declared_role: "coordinator".to_owned(),
            transport: AgentTransportKind::StreamableHttp,
            migration_id: Uuid::from_u128(0xCA15),
            now_ms: 2,
        });
        assert!(matches!(invalid, Err(RuntimeError::InvalidInput)));

        let result = runtime.prepare_member_registration(MemberRegisterCommand {
            target,
            principal,
            display_name: "Codex Main".to_owned(),
            declared_role: "coordinator".to_owned(),
            transport: AgentTransportKind::StreamableHttp,
            migration_id: Uuid::from_u128(0xCA15),
            now_ms: 2,
        });
        assert!(
            matches!(result, Err(RuntimeError::Conflict)),
            "the valid unlinked request still requires a Verified source import: {result:?}"
        );

        drop(runtime);
        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn unlinked_session_rejects_every_transport_except_streamable_http()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = std::env::temp_dir().join(format!(
            "orchestration-native-membership-transport-{}",
            Uuid::now_v7()
        ));
        fs::create_dir(&root)?;
        let runtime = OrchestrationRuntime::open(&root)?;
        let principal =
            AuthenticatedPrincipal::DirectToken(DirectTokenId::from_uuid(Uuid::from_u128(0xCA21)));
        let session = runtime.open_session(Uuid::from_u128(0xCA22), principal, 1)?;
        let target = MembershipTarget::new(
            Uuid::from_u128(0xCA23),
            Uuid::from_u128(0xCA24),
            session.session.session_id,
        )?;

        for transport in [
            AgentTransportKind::Retired,
            AgentTransportKind::Stdio,
            AgentTransportKind::Other,
        ] {
            assert!(matches!(
                runtime.prepare_member_registration(MemberRegisterCommand {
                    target,
                    principal,
                    display_name: "Codex Main".to_owned(),
                    declared_role: "coordinator".to_owned(),
                    transport,
                    migration_id: Uuid::from_u128(0xCA25),
                    now_ms: 2,
                }),
                Err(RuntimeError::InvalidInput)
            ));
        }

        drop(runtime);
        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn native_registration_still_requires_the_exact_open_session_owner()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = std::env::temp_dir().join(format!(
            "orchestration-native-membership-owner-{}",
            Uuid::now_v7()
        ));
        fs::create_dir(&root)?;
        let runtime = OrchestrationRuntime::open(&root)?;
        let owner =
            AuthenticatedPrincipal::DirectToken(DirectTokenId::from_uuid(Uuid::from_u128(0xCA31)));
        let foreign =
            AuthenticatedPrincipal::DirectToken(DirectTokenId::from_uuid(Uuid::from_u128(0xCA32)));
        let session = runtime.open_session(Uuid::from_u128(0xCA33), owner, 1)?;
        let target = MembershipTarget::new(
            Uuid::from_u128(0xCA34),
            Uuid::from_u128(0xCA35),
            session.session.session_id,
        )?;
        let command = |principal| MemberRegisterCommand {
            target,
            principal,
            display_name: "Codex Main".to_owned(),
            declared_role: "coordinator".to_owned(),
            transport: AgentTransportKind::StreamableHttp,
            migration_id: Uuid::from_u128(0xCA36),
            now_ms: 2,
        };
        assert!(matches!(
            runtime.prepare_member_registration(command(foreign)),
            Err(RuntimeError::Forbidden)
        ));
        let _closed = runtime.close_session(
            session.session.session_id,
            owner,
            session.session.revision,
            3,
        )?;
        assert!(matches!(
            runtime.prepare_member_registration(command(owner)),
            Err(RuntimeError::Forbidden)
        ));

        drop(runtime);
        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn missing_profile_scope_requires_the_canonical_native_participant_identity()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = std::env::temp_dir().join(format!(
            "orchestration-native-membership-anchor-{}",
            Uuid::now_v7()
        ));
        fs::create_dir(&root)?;
        let runtime = OrchestrationRuntime::open(&root)?;
        let target = MembershipTarget::new(
            Uuid::from_u128(0xCA41),
            Uuid::from_u128(0xCA42),
            Uuid::from_u128(0xCA43),
        )?;
        let request_id = member_registration_request_id(
            target.workspace_id,
            target.activity_id,
            target.session_id,
        );
        let native_participant =
            identity::native_session_participation_id(target.workspace_id, target.session_id);
        assert!(
            runtime
                .ensure_profile_participation_anchor(
                    target,
                    request_id,
                    native_participant,
                    TimestampMillis::new(1)?,
                )
                .is_ok()
        );
        assert!(matches!(
            runtime.ensure_profile_participation_anchor(
                target,
                request_id,
                Uuid::from_u128(0xCA44),
                TimestampMillis::new(1)?,
            ),
            Err(RuntimeError::Conflict)
        ));

        drop(runtime);
        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn new_registration_transport_matches_source_activation_predicate() {
        let token =
            AuthenticatedPrincipal::DirectToken(DirectTokenId::from_uuid(Uuid::from_u128(1)));
        let grant = AuthenticatedPrincipal::OAuthGrant(OAuthGrantId::from_uuid(Uuid::from_u128(2)));
        for principal in [token, grant] {
            assert!(
                validate_new_registration_transport(principal, AgentTransportKind::StreamableHttp)
                    .is_ok()
            );
            assert!(
                validate_new_registration_transport(principal, AgentTransportKind::Retired).is_ok()
            );
            for transport in [AgentTransportKind::Other, AgentTransportKind::Stdio] {
                assert!(matches!(
                    validate_new_registration_transport(principal, transport),
                    Err(RuntimeError::InvalidInput)
                ));
            }
        }
        for transport in [
            AgentTransportKind::Other,
            AgentTransportKind::Stdio,
            AgentTransportKind::Retired,
            AgentTransportKind::StreamableHttp,
        ] {
            assert!(
                validate_new_registration_transport(
                    AuthenticatedPrincipal::TrustedStdio,
                    transport
                )
                .is_ok()
            );
        }
    }

    #[test]
    fn rejection_readback_authenticates_closed_session_after_restart()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = std::env::temp_dir().join(format!(
            "orchestration-rejection-closed-session-{}",
            Uuid::now_v7()
        ));
        fs::create_dir(&root)?;
        let principal =
            AuthenticatedPrincipal::DirectToken(DirectTokenId::from_uuid(Uuid::from_u128(0xCA01)));
        let runtime = OrchestrationRuntime::open(&root)?;
        let opened = runtime.open_session(Uuid::from_u128(0xCA02), principal, 1)?;
        let session_id = opened.session.session_id;
        let _closed = runtime.close_session(session_id, principal, opened.session.revision, 2)?;
        drop(runtime);

        let reopened = OrchestrationRuntime::open(&root)?;
        let target =
            MembershipTarget::new(Uuid::from_u128(0xCA03), Uuid::from_u128(0xCA04), session_id)?;
        assert!(
            reopened
                .member_binding_rejection_readback(target, principal, Uuid::from_u128(0xCA05),)?
                .is_none()
        );
        let foreign =
            AuthenticatedPrincipal::DirectToken(DirectTokenId::from_uuid(Uuid::from_u128(0xCA06)));
        assert!(matches!(
            reopened.member_binding_rejection_readback(target, foreign, Uuid::from_u128(0xCA05),),
            Err(RuntimeError::Forbidden)
        ));
        drop(reopened);
        fs::remove_dir_all(root)?;
        Ok(())
    }
}
