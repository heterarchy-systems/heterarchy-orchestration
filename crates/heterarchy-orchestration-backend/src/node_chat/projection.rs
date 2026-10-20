//! Native-compatible chat projections and checked destination conversion.

use crate::wire::workspace_project::ProviderProjectReferenceWire;
use crate::{
    RuntimeError,
    wire::node_chat::{
        ChatEntryWire, ChatMessageRequest, ChatProviderStateWire, ChatQueryWire, ChatReceiptWire,
        ChatSnapshotWire, ChatTargetFenceWire, NodeChatTargetWire,
    },
};
use heterarchy_orchestration_operations::{ProviderMessageState, WorkspaceProjectReference};
use heterarchy_orchestration_runtime::chat::{
    ChatEntry, ChatNodeTarget, ChatQuery, ChatReceipt, ChatSend, ChatSnapshot, ChatTargetFence,
};

/// Preserve the full native snapshot, including unavailable history and explicit nulls.
#[must_use]
pub fn project_snapshot(value: &ChatSnapshot) -> ChatSnapshotWire {
    ChatSnapshotWire {
        workspace_id: value.query.workspace_id.into_uuid(),
        agent_profile_id: value
            .query
            .agent_profile_id
            .map(heterarchy_orchestration_sessions::AgentProfileId::into_uuid),
        targets: value.targets.iter().map(project_fence).collect(),
        target: value.target.as_ref().map(project_fence),
        messages: value.entries.iter().map(project_entry).collect(),
        node_scope: value.query.node_scope.map(Into::into),
        before_sequence: value.query.before_sequence,
        next_before_sequence: value.next_before_sequence,
        unavailable_reason: value.unavailable_reason.map(str::to_owned),
    }
}

/// Preserve query selectors for native and HTTP consumers.
#[must_use]
pub fn project_query(value: &ChatQuery) -> ChatQueryWire {
    ChatQueryWire {
        workspace_id: value.workspace_id.into_uuid(),
        agent_profile_id: value
            .agent_profile_id
            .map(heterarchy_orchestration_sessions::AgentProfileId::into_uuid),
        runtime_session_id: value.runtime,
        node_scope: value.node_scope.map(Into::into),
        before_sequence: value.before_sequence,
    }
}

/// Preserve the frozen message intent at a transport boundary.
#[must_use]
pub fn project_send(value: &ChatSend) -> ChatMessageRequest {
    ChatMessageRequest {
        query: project_query(&value.query),
        message_id: value.message_id,
        text: value.text.clone(),
        target_fence: project_fence(&value.target_fence),
    }
}

/// Project durable admission or recovery without claiming Provider delivery.
#[must_use]
pub fn project_receipt(value: &ChatReceipt) -> ChatReceiptWire {
    ChatReceiptWire {
        message_id: value.message_id,
        provider_state: provider_state(value.provider_state),
    }
}

fn project_fence(value: &ChatTargetFence) -> ChatTargetFenceWire {
    ChatTargetFenceWire {
        operation_id: value.operation_id,
        profile_id: value.profile_id,
        profile_name: value.profile_name.clone(),
        participation_id: value.participation_id,
        participation_revision: value.participation_revision,
        runtime_session_id: value.runtime_session_id,
        runtime_revision: value.runtime_revision,
        provider_conversation_id: value.provider_conversation_id.clone(),
        project: value.project.map(ProviderProjectReferenceWire::from),
        leadership_revision: value.leadership_revision,
        node_target: value.node_target.as_ref().map(|target| match *target {
            ChatNodeTarget::Agent {
                assignment_id,
                assignment_revision,
                agent_id,
                session_id,
            } => NodeChatTargetWire::Agent {
                assignment_id,
                assignment_revision,
                agent_id,
                session_id,
            },
            ChatNodeTarget::Team {
                team_id,
                binding_revision,
            } => NodeChatTargetWire::Team {
                team_id,
                binding_revision,
            },
        }),
    }
}

pub(super) fn domain_fence(value: &ChatTargetFenceWire) -> Result<ChatTargetFence, RuntimeError> {
    let project = value
        .project
        .clone()
        .map(WorkspaceProjectReference::try_from)
        .transpose()
        .map_err(|_| RuntimeError::InvalidInput)?;
    if [
        value.operation_id,
        value.profile_id,
        value.participation_id,
        value.runtime_session_id,
    ]
    .iter()
    .any(uuid::Uuid::is_nil)
        || value.participation_revision == 0
        || value.runtime_revision <= 0
        || value
            .leadership_revision
            .is_some_and(|revision| revision <= 0)
    {
        return Err(RuntimeError::InvalidInput);
    }
    let node_target = match value
        .node_target
        .as_ref()
        .ok_or(RuntimeError::InvalidInput)?
    {
        NodeChatTargetWire::Agent {
            assignment_id,
            assignment_revision,
            agent_id,
            session_id,
        } => {
            if [assignment_id, agent_id, session_id]
                .iter()
                .any(|id| id.is_nil())
                || *assignment_revision <= 0
                || value.leadership_revision.is_some()
            {
                return Err(RuntimeError::InvalidInput);
            }
            ChatNodeTarget::Agent {
                assignment_id: *assignment_id,
                assignment_revision: *assignment_revision,
                agent_id: *agent_id,
                session_id: *session_id,
            }
        }
        NodeChatTargetWire::Team {
            team_id,
            binding_revision,
        } => {
            if team_id.is_nil() || *binding_revision <= 0 || value.leadership_revision.is_none() {
                return Err(RuntimeError::InvalidInput);
            }
            ChatNodeTarget::Team {
                team_id: *team_id,
                binding_revision: *binding_revision,
            }
        }
    };
    Ok(ChatTargetFence {
        operation_id: value.operation_id,
        profile_id: value.profile_id,
        profile_name: value.profile_name.clone(),
        participation_id: value.participation_id,
        participation_revision: value.participation_revision,
        runtime_session_id: value.runtime_session_id,
        runtime_revision: value.runtime_revision,
        provider_conversation_id: value.provider_conversation_id.clone(),
        project,
        leadership_revision: value.leadership_revision,
        node_target: Some(node_target),
    })
}

fn project_entry(value: &ChatEntry) -> ChatEntryWire {
    crate::workspace_chat::project_entry(value)
}

fn provider_state(value: ProviderMessageState) -> ChatProviderStateWire {
    crate::workspace_chat::provider_state(value)
}
