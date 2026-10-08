//! Exact Forge node recipient routing for shared Runtime chat operations.

use forge_domain::{
    ActivityId, AgentAssignmentId, AgentAssignmentRevision, AgentAssignmentState, AgentInstanceId,
    AgentSessionId, DagRevisionNumber, TimestampMillis, WorkspaceId,
};
use heterarchy_orchestration_operations::{
    LeadershipScope, NodeMessageRouteTarget, NodeMessageRouteWitness, NodeTeamBinding,
    NodeTeamBindingScope, OperationalActor, OperationsCatalog,
};
use heterarchy_orchestration_sessions::{AgentProfileCatalog, AgentProfileId};
use uuid::Uuid;

use super::{ChatError, ChatQuery, ChatTargetFence, ResolvedChat, resolve_chat};

/// Exact node recipient identity captured in a chat destination fence.
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
    catalog: &OperationsCatalog,
    profiles: &AgentProfileCatalog,
    workspace: WorkspaceId,
    query: &ChatQuery,
    observation: Option<&ChatSourceObservation>,
    now: TimestampMillis,
) -> Result<ResolvedChat, ChatError> {
    let scope = query.node_scope.ok_or(ChatError::Identity)?;
    if scope.workspace_id() != workspace || query.agent_profile_id.is_some() {
        return Err(ChatError::Identity);
    }
    let Some(observed) =
        observation.filter(|observed| current_node_observation_matches(scope, observed))
    else {
        return Ok(unavailable(
            "This node has no current Forge destination. History remains available.",
        ));
    };
    let binding = catalog.node_team_binding(scope)?;
    if let Some(binding) = binding.filter(|value| value.team_id().is_some()) {
        resolve_team_chat(catalog, profiles, workspace, query, now, &binding)
    } else {
        resolve_agent_chat(catalog, profiles, workspace, query, now, scope, observed)
    }
}

fn resolve_team_chat(
    catalog: &OperationsCatalog,
    profiles: &AgentProfileCatalog,
    workspace: WorkspaceId,
    query: &ChatQuery,
    now: TimestampMillis,
    binding: &NodeTeamBinding,
) -> Result<ResolvedChat, ChatError> {
    let team = binding.team_id().ok_or(ChatError::TargetChanged)?;
    let scope = LeadershipScope::team(workspace, team);
    let Some(authority) = catalog.current_leadership_authority(scope)? else {
        return Ok(unavailable(
            "This Team has no confirmed leader available for messages.",
        ));
    };
    let Some(view) = catalog.leadership_scope_view(scope, now.get())? else {
        return Ok(unavailable(
            "This Team has no confirmed leader available for messages.",
        ));
    };
    if view.leader_participant_id() != Some(authority.participant_id())
        || view.leadership_revision() != authority.leadership_revision()
    {
        return Ok(unavailable("The Team leader changed. Refresh this node."));
    }
    let Some(profile) = view.leader_profile_id() else {
        return Ok(unavailable("The Team leader has no active conversation."));
    };
    let binding_revision = i64::try_from(binding.revision()).map_err(|_| ChatError::Identity)?;
    let node_target = ChatNodeTarget::Team {
        team_id: team,
        binding_revision,
    };
    resolve_node_profile(
        catalog,
        profiles,
        workspace,
        query,
        now,
        &NodeRecipient {
            profile: AgentProfileId::from_uuid(profile)?,
            participant: authority.participant_id(),
            session: None,
            activity: None,
            node_target,
            leadership_revision: Some(authority.leadership_revision()),
        },
    )
}

fn resolve_agent_chat(
    catalog: &OperationsCatalog,
    profiles: &AgentProfileCatalog,
    workspace: WorkspaceId,
    query: &ChatQuery,
    now: TimestampMillis,
    scope: NodeTeamBindingScope,
    observed: &ChatSourceObservation,
) -> Result<ResolvedChat, ChatError> {
    let Some(assignment) = observed
        .assignment
        .filter(|assignment| assignment.state.is_active() && !assignment.cancellation_requested)
    else {
        return Ok(unavailable(
            "This node has no active Agent assignment. History remains available.",
        ));
    };
    let Some(link) = profiles.logical_session_link(assignment.target_session_id)? else {
        return Ok(unavailable("The assigned Agent has no linked Profile."));
    };
    let Some(identity) =
        catalog.logical_identity_for_session(workspace, assignment.target_session_id)?
    else {
        return Ok(unavailable(
            "The assigned Agent is not active in this Workspace.",
        ));
    };
    if !identity.effect_bindings().iter().any(|binding| {
        binding.activity_id() == scope.activity_id()
            && binding.agent_id() == assignment.target_agent_id
    }) {
        return Ok(unavailable(
            "The assigned Agent binding changed. Refresh this node.",
        ));
    }
    let node_target = ChatNodeTarget::Agent {
        assignment_id: assignment.id.into_uuid(),
        assignment_revision: assignment.revision.get(),
        agent_id: assignment.target_agent_id.into_uuid(),
        session_id: assignment.target_session_id.into_uuid(),
    };
    resolve_node_profile(
        catalog,
        profiles,
        workspace,
        query,
        now,
        &NodeRecipient {
            profile: link.profile_id(),
            participant: identity.participant_id(),
            session: Some(assignment.target_session_id),
            activity: Some(scope.activity_id()),
            node_target,
            leadership_revision: None,
        },
    )
}

struct NodeRecipient {
    profile: AgentProfileId,
    participant: Uuid,
    session: Option<AgentSessionId>,
    activity: Option<ActivityId>,
    node_target: ChatNodeTarget,
    leadership_revision: Option<i64>,
}

fn resolve_node_profile(
    catalog: &OperationsCatalog,
    profiles: &AgentProfileCatalog,
    workspace: WorkspaceId,
    query: &ChatQuery,
    now: TimestampMillis,
    recipient: &NodeRecipient,
) -> Result<ResolvedChat, ChatError> {
    let direct = ChatQuery {
        workspace_id: workspace,
        agent_profile_id: Some(recipient.profile),
        runtime: query.runtime,
        node_scope: None,
        before_sequence: None,
    };
    let mut resolved = resolve_chat(catalog, profiles, workspace, &direct, now)?;
    let Some(participation) = catalog.workspace_participation(workspace, recipient.profile)? else {
        return Ok(unavailable(
            "The node recipient is no longer in this Workspace.",
        ));
    };
    let current_identity =
        catalog.logical_identity_for_participant(workspace, recipient.participant)?;
    if !current_identity.is_some_and(|identity| {
        recipient
            .session
            .is_none_or(|session| identity.session_id() == session)
    }) {
        return Ok(unavailable(
            "The node recipient Session changed. Refresh this node.",
        ));
    }
    let routes = catalog.workspace_chat_targets(workspace, recipient.profile, 8)?;
    if routes.truncated() {
        return Err(ChatError::TargetChanged);
    }
    resolved.targets.retain(|(fence, _)| {
        participation.session_mappings().iter().any(|mapping| {
            mapping.profile_mapping_request_id() == fence.operation_id
                && mapping.participant_id() == recipient.participant
        }) && recipient.activity.is_none_or(|activity| {
            routes.targets().iter().any(|route| {
                route.operation_id() == fence.operation_id && route.activity_id() == activity
            })
        })
    });
    for (fence, _) in &mut resolved.targets {
        fence.node_target = Some(recipient.node_target.clone());
        fence.leadership_revision = recipient.leadership_revision;
    }
    Ok(resolved)
}

pub(super) fn unavailable(reason: &'static str) -> ResolvedChat {
    ResolvedChat {
        channel: None,
        targets: Vec::new(),
        unavailable: Some(reason),
    }
}
