//! Canonical identities shared with the retired MCP membership adapter.

use forge_domain::{
    ActivityId, AgentDeclaredRole, AgentDisplayName, AgentSessionId, AgentTransportKind,
    AuthenticatedPrincipal, TimestampMillis,
};
use heterarchy_orchestration_operations::{AgentBindingReservationRequest, RegistrationRequest};
use uuid::Uuid;

use super::MembershipTarget;
use crate::RuntimeError;

const MEMBER_REQUEST_DOMAIN: &[u8] = b"heterarchy-orchestration/member-registration-request/v1\0";
const MEMBER_PROFILE_REBIND_DOMAIN: &[u8] = b"heterarchy.mcp.member.profile-rebind.v1\0";
const MEMBER_PROFILE_PARTICIPATION_DOMAIN: &[u8] = b"heterarchy-member-profile-participation-v1\0";
const MEMBER_PROFILE_MAPPING_DOMAIN: &[u8] = b"heterarchy-member-profile-mapping-v1\0";

pub(super) fn registration_request(
    target: MembershipTarget,
    participant_id: Uuid,
    principal: AuthenticatedPrincipal,
    display_name: AgentDisplayName,
    declared_role: AgentDeclaredRole,
    transport: AgentTransportKind,
    requested_at: TimestampMillis,
) -> Result<RegistrationRequest, RuntimeError> {
    let binding = AgentBindingReservationRequest::new(
        request_id(target.workspace_id, target.activity_id, target.session_id),
        ActivityId::from_uuid(target.activity_id),
        crate::leadership::member_agent_id(
            target.workspace_id,
            target.activity_id,
            target.session_id,
        ),
        AgentSessionId::from_uuid(target.session_id),
        principal,
        requested_at,
    )
    .map_err(|_| RuntimeError::InvalidInput)?;
    RegistrationRequest::new(
        participant_id,
        binding,
        display_name,
        declared_role,
        transport,
    )
    .map_err(Into::into)
}

pub(super) fn request_id(workspace: Uuid, activity: Uuid, session: Uuid) -> Uuid {
    derived_member_uuid(MEMBER_REQUEST_DOMAIN, workspace, Some(activity), session)
}

pub(super) fn profile_participation_id(workspace: Uuid, profile: Uuid) -> Uuid {
    derived_member_uuid(
        MEMBER_PROFILE_PARTICIPATION_DOMAIN,
        workspace,
        None,
        profile,
    )
}

pub(super) fn profile_mapping_request_id(workspace: Uuid, session: Uuid) -> Uuid {
    derived_member_uuid(MEMBER_PROFILE_MAPPING_DOMAIN, workspace, None, session)
}

pub(super) fn profile_rebind_request_id(workspace: Uuid, session: Uuid) -> Uuid {
    derived_member_uuid(MEMBER_PROFILE_REBIND_DOMAIN, workspace, None, session)
}

fn derived_member_uuid(
    domain: &[u8],
    workspace: Uuid,
    activity: Option<Uuid>,
    session: Uuid,
) -> Uuid {
    let mut hasher = blake3::Hasher::new();
    hasher.update(domain);
    hasher.update(workspace.as_bytes());
    if let Some(activity) = activity {
        hasher.update(activity.as_bytes());
    }
    hasher.update(session.as_bytes());
    let digest = hasher.finalize();
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest.as_bytes()[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x80;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Uuid::from_bytes(bytes)
}
