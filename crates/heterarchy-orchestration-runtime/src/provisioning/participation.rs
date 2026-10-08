//! Runtime-owned join and activation for confirmed `AgentRequest` provision
//! operations.

use forge_domain::{AgentTransportKind, AuthenticatedPrincipal};
use heterarchy_orchestration_operations::{AgentProvisionState, OperationsError};
use heterarchy_orchestration_sessions::{
    AgentProfileError, AgentProfileLifecycleState, AgentProfileRuntimeSessionState,
};
use uuid::Uuid;

use crate::membership::{
    MemberActivation, MemberRegisterCommand, MembershipTarget, member_registration_request_id,
};
use crate::{OrchestrationRuntime, RuntimeError, profile_error_to_runtime};

const AGENT_REQUEST_SESSION_OPEN_DOMAIN: &[u8] =
    b"heterarchy.orchestration.agent-request.session-open.v1\0";

/// Validated Profile/runtime target for one confirmed provision operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentRequestParticipationTarget {
    pub operation_id: Uuid,
    pub request_id: Uuid,
    pub workspace_id: Uuid,
    pub activity_id: Uuid,
    pub profile_id: Uuid,
    pub runtime_session_id: Uuid,
    pub display_name: String,
}

/// Durable request and membership evidence after participation reconciliation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentRequestParticipationSummary {
    pub operation_id: Uuid,
    pub request_id: Uuid,
    pub state: &'static str,
    pub revision: u64,
    pub profile_id: Uuid,
    pub runtime_session_id: Uuid,
    pub session_id: Uuid,
    pub membership: MemberActivation,
}

/// Derive the byte-identical logical Session-open request identity used by MCP.
#[must_use]
pub fn agent_request_session_open_request_id(operation_id: Uuid) -> Uuid {
    let mut hasher = blake3::Hasher::new();
    hasher.update(AGENT_REQUEST_SESSION_OPEN_DOMAIN);
    hasher.update(operation_id.as_bytes());
    let digest = hasher.finalize();
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest.as_bytes()[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x80;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Uuid::from_bytes(bytes)
}

impl OrchestrationRuntime {
    /// Resolve the exact confirmed provision, linked request and ready provider
    /// runtime used by the existing `AgentRequest` participation flow.
    ///
    /// # Errors
    /// Returns invalid input, conflict, missing-resource or catalog errors when
    /// the operation is not a confirmed, fully linked participation target.
    pub fn agent_request_participation_target(
        &self,
        operation_id: Uuid,
    ) -> Result<AgentRequestParticipationTarget, RuntimeError> {
        if operation_id.is_nil() {
            return Err(RuntimeError::InvalidInput);
        }

        // Release the Operations guard before acquiring the Profile guard.
        let catalog = self.provisioning_catalog()?;
        let operation =
            catalog
                .agent_provision_operation(operation_id)?
                .ok_or(OperationsError::Conflict(
                    "Agent provision operation missing",
                ))?;
        let request =
            catalog
                .agent_request(operation.request_id())?
                .ok_or(OperationsError::Conflict(
                    "AgentRequest missing for provision operation",
                ))?;
        drop(catalog);

        if operation.state() != AgentProvisionState::Confirmed {
            return Err(RuntimeError::Conflict);
        }
        let profile_id = operation.profile_id().ok_or(RuntimeError::Conflict)?;
        let runtime_session_id = operation
            .runtime_session_id()
            .ok_or(RuntimeError::Conflict)?;

        let display_name = {
            let catalog = self.profile_catalog_guard()?;
            let profile = catalog
                .resolve(profile_id)
                .map_err(profile_error_to_runtime)?
                .ok_or_else(|| {
                    profile_error_to_runtime(AgentProfileError::ProfileMissing(profile_id))
                })?;
            let runtime = catalog
                .runtime_session(runtime_session_id)
                .map_err(profile_error_to_runtime)?
                .ok_or_else(|| {
                    profile_error_to_runtime(AgentProfileError::RuntimeSessionMissing(
                        runtime_session_id,
                    ))
                })?;

            if runtime.profile_id() != profile_id
                || runtime.state() != AgentProfileRuntimeSessionState::Ready
                || runtime.provider_conversation_id().is_none()
                || profile.lifecycle_state() != AgentProfileLifecycleState::Active
            {
                return Err(profile_error_to_runtime(
                    AgentProfileError::InvalidIdentity("AgentRequest join target"),
                ));
            }
            profile.display_name().to_owned()
        };

        Ok(AgentRequestParticipationTarget {
            operation_id,
            request_id: request.request_id(),
            workspace_id: request.workspace_id().into_uuid(),
            activity_id: request.activity_id().into_uuid(),
            profile_id: profile_id.into_uuid(),
            runtime_session_id: runtime_session_id.into_uuid(),
            display_name,
        })
    }

    /// Check whether the caller's open Session is linked to this operation.
    ///
    /// This deliberately reads only the caller-owned Session and its canonical
    /// Profile link; it does not inspect or disclose global operation state.
    ///
    /// # Errors
    /// Returns invalid input, forbidden for a missing/foreign/closed Session,
    /// or the Profile catalog's existing read errors.
    pub fn agent_request_participation_linked(
        &self,
        operation_id: Uuid,
        session_id: Uuid,
        principal: AuthenticatedPrincipal,
    ) -> Result<bool, RuntimeError> {
        if operation_id.is_nil() {
            return Err(RuntimeError::InvalidInput);
        }
        self.require_open_session(session_id, principal)?;
        let link = self
            .profile_catalog_guard()?
            .logical_session_link(forge_domain::AgentSessionId::from_uuid(session_id))
            .map_err(profile_error_to_runtime)?;
        Ok(link.is_some_and(|link| link.request_id() == operation_id))
    }

    /// Build the existing member-registration command for one request Session.
    ///
    /// A durable registration replay keeps its original metadata even if the
    /// current Profile display name or transport has changed.
    ///
    /// # Errors
    /// Returns invalid input, forbidden for a missing/foreign/closed Session,
    /// conflict for a missing or mismatched Profile link, or catalog errors.
    pub fn agent_request_member_register_command(
        &self,
        operation_id: Uuid,
        session_id: Uuid,
        principal: AuthenticatedPrincipal,
        migration_id: Uuid,
        transport: AgentTransportKind,
        now_ms: i64,
    ) -> Result<MemberRegisterCommand, RuntimeError> {
        if operation_id.is_nil() {
            return Err(RuntimeError::InvalidInput);
        }
        self.require_open_session(session_id, principal)?;
        let profile_link = {
            let catalog = self.profile_catalog_guard()?;
            catalog
                .logical_session_link(forge_domain::AgentSessionId::from_uuid(session_id))
                .map_err(profile_error_to_runtime)?
        }
        .ok_or(RuntimeError::Conflict)?;
        if profile_link.request_id() != operation_id {
            return Err(RuntimeError::Conflict);
        }

        let target = self.agent_request_participation_target(operation_id)?;
        if profile_link.profile_id().into_uuid() != target.profile_id {
            return Err(RuntimeError::Conflict);
        }
        let membership_target =
            MembershipTarget::new(target.workspace_id, target.activity_id, session_id)?;
        let request_id =
            member_registration_request_id(target.workspace_id, target.activity_id, session_id);
        let progress = self.member_registration(membership_target, principal, request_id)?;
        let (display_name, declared_role, registration_transport) = match progress {
            Some(progress) => {
                let request = progress.request();
                (
                    request.display_name().as_str().to_owned(),
                    request.declared_role().as_str().to_owned(),
                    request.transport(),
                )
            }
            None => (target.display_name, "participant".to_owned(), transport),
        };

        Ok(MemberRegisterCommand {
            target: membership_target,
            principal,
            display_name,
            declared_role,
            transport: registration_transport,
            migration_id,
            now_ms,
        })
    }

    /// Bind one caller-owned open Session to the confirmed request's Profile.
    ///
    /// # Errors
    /// Preserves target validation, Session ownership/lifecycle, Profile and
    /// durable link errors from the existing Runtime owners.
    pub fn bind_agent_request_participation(
        &self,
        operation_id: Uuid,
        session_id: Uuid,
        principal: AuthenticatedPrincipal,
        now_ms: i64,
    ) -> Result<AgentRequestParticipationTarget, RuntimeError> {
        let target = self.agent_request_participation_target(operation_id)?;
        self.bind_profile_session(
            operation_id,
            target.profile_id,
            session_id,
            principal,
            now_ms,
        )?;
        Ok(target)
    }

    /// Reconcile one already activated membership into its `AgentRequest`.
    ///
    /// This method only reads existing membership/Profile evidence and records
    /// the existing fulfillment witness; source activation remains Backend-owned.
    ///
    /// # Errors
    /// Returns target, Session, membership, Profile-link or Operations failures
    /// when the exact participation evidence is not already Ready.
    pub fn complete_agent_request_participation(
        &self,
        operation_id: Uuid,
        session_id: Uuid,
        principal: AuthenticatedPrincipal,
        now_ms: i64,
    ) -> Result<AgentRequestParticipationSummary, RuntimeError> {
        let target = self.agent_request_participation_target(operation_id)?;
        let membership_target =
            MembershipTarget::new(target.workspace_id, target.activity_id, session_id)?;
        let membership_request_id =
            member_registration_request_id(target.workspace_id, target.activity_id, session_id);
        let activation_plan =
            self.member_activation_plan(membership_target, principal, membership_request_id)?;
        let membership = activation_plan
            .activation()
            .cloned()
            .ok_or(RuntimeError::Conflict)?;

        let profile_binding = self
            .profile_session_binding(target.profile_id, session_id, principal)?
            .ok_or(RuntimeError::Conflict)?;
        if profile_binding.request_id != operation_id
            || profile_binding.profile_id != target.profile_id
            || profile_binding.session_id != session_id
        {
            return Err(RuntimeError::Conflict);
        }

        let reconciled = self.reconcile_agent_request_fulfillment(target.request_id, now_ms)?;
        Ok(AgentRequestParticipationSummary {
            operation_id: target.operation_id,
            request_id: target.request_id,
            state: reconciled.state,
            revision: reconciled.revision,
            profile_id: target.profile_id,
            runtime_session_id: target.runtime_session_id,
            session_id,
            membership,
        })
    }
}
