//! Historical exact Forge node-route evidence for chat recovery.

use forge_domain::{
    AgentAssignmentId, AgentAssignmentRevision, AgentAssignmentState, AgentInstanceId,
    AgentSessionId, DagRevisionNumber,
};
use heterarchy_orchestration_operations::{
    NodeMessageRouteTarget, NodeMessageRouteWitness, NodeTeamBindingScope, OperationalActor,
};
use uuid::Uuid;

use super::{ChatError, ChatQuery, ChatTargetFence, ResolvedChat};

/// Historical node recipient identity captured in a chat destination fence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChatNodeTarget {
    /// A current Forge Agent assignment for the node.
    Agent {
        /// Exact assignment identity returned by Forge.
        assignment_id: Uuid,
        /// Exact assignment revision returned by Forge.
        assignment_revision: i64,
        /// Assigned Forge Agent identity.
        agent_id: Uuid,
        /// Assigned logical Agent Session.
        session_id: Uuid,
    },
    /// The Team currently bound to the node.
    Team {
        /// Exact Team identity.
        team_id: Uuid,
        /// Node-Team binding revision captured with the destination.
        binding_revision: i64,
    },
}

/// Current assignment details returned by the trusted Forge source adapter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChatSourceAssignment {
    /// Exact Forge assignment identity.
    pub id: AgentAssignmentId,
    /// Exact Forge assignment revision.
    pub revision: AgentAssignmentRevision,
    /// Assigned Forge Agent identity.
    pub target_agent_id: AgentInstanceId,
    /// Assigned logical Agent Session.
    pub target_session_id: AgentSessionId,
    /// Assignment lifecycle state observed from Forge.
    pub state: AgentAssignmentState,
    /// Whether Forge has requested cancellation for this assignment.
    pub cancellation_requested: bool,
}

/// Typed in-process Forge evidence for one exact historical node scope.
///
/// This is an adapter-to-Runtime value, never a deserialized HTTP request field.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChatSourceObservation {
    /// Exact Activity/Goal/DAG/node identity requested from Forge.
    pub scope: NodeTeamBindingScope,
    /// Current DAG revision observed alongside the requested node.
    pub current_dag_revision: DagRevisionNumber,
    /// Assignment attached to the requested node, when one exists.
    pub assignment: Option<ChatSourceAssignment>,
}

impl ChatNodeTarget {
    pub(super) fn node_witness(
        &self,
        fence: &ChatTargetFence,
        actor: OperationalActor,
        sender_leadership_revision: Option<i64>,
    ) -> Result<Option<NodeMessageRouteWitness>, ChatError> {
        let target = match self {
            Self::Agent {
                assignment_id,
                assignment_revision,
                agent_id,
                session_id,
            } => {
                if fence.leadership_revision.is_some() {
                    return Err(ChatError::Identity);
                }
                NodeMessageRouteTarget::Agent {
                    assignment_id: *assignment_id,
                    assignment_revision: *assignment_revision,
                    agent_id: *agent_id,
                    session_id: *session_id,
                }
            }
            Self::Team {
                team_id,
                binding_revision,
            } => NodeMessageRouteTarget::Team {
                team_id: *team_id,
                binding_revision: *binding_revision,
                leadership_revision: fence.leadership_revision.ok_or(ChatError::Identity)?,
            },
        };
        let witness = NodeMessageRouteWitness::new(
            fence.operation_id,
            fence.participation_revision,
            fence.runtime_revision,
            actor,
            sender_leadership_revision,
            target,
        )?;
        Ok(Some(witness))
    }
}

fn current_node_observation_matches(
    scope: NodeTeamBindingScope,
    observed: &ChatSourceObservation,
) -> bool {
    observed.scope == scope && observed.current_dag_revision == scope.dag_revision()
}

pub(super) fn resolve_node_chat(
    query: &ChatQuery,
    observation: Option<&ChatSourceObservation>,
) -> Result<ResolvedChat, ChatError> {
    let scope = query.node_scope.ok_or(ChatError::Identity)?;
    if scope.workspace_id() != query.workspace_id || query.agent_profile_id.is_some() {
        return Err(ChatError::Identity);
    }
    let unavailable =
        if observation.is_some_and(|value| current_node_observation_matches(scope, value)) {
            "ChatGPT destinations are retired. Node history remains available."
        } else {
            "This node has no current Forge destination. History remains available."
        };
    Ok(unavailable_chat(unavailable))
}

pub(super) fn unavailable_chat(reason: &'static str) -> ResolvedChat {
    ResolvedChat {
        channel: None,
        unavailable: Some(reason),
    }
}
