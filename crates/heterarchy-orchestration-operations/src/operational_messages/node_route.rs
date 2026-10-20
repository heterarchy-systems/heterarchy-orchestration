//! Immutable observed routing facts for exact node-message replay.
//! These facts never grant work or replace fresh admission checks.

use forge_domain::{ActivityId, AgentAssignmentId, AgentInstanceId, AgentSessionId};
use rusqlite::{OptionalExtension, Transaction, params};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{OperationalActor, OperationsError};

use super::{MessageAuthor, MessageChannel, ProviderMessageTarget, WorkspaceMessage};
use crate::{AssignmentAffinityDisposition, LeadershipScope, NodeTeamBindingScope, wire};

/// Revalidate Operations-owned routing facts inside the append/enqueue transaction.
/// Forge assignment state and Profile runtime state are ingress observations;
/// this transaction does not claim atomicity with either of those owners.
pub(super) fn validate_node_admission(
    transaction: &Transaction<'_>,
    message: &WorkspaceMessage,
    target: &ProviderMessageTarget,
    witness: &NodeMessageRouteWitness,
) -> Result<(), OperationsError> {
    let context = message.context();
    let missing = || OperationsError::Conflict("node message exact scope");
    let scope = NodeTeamBindingScope::new(
        message.workspace_id(),
        context.activity_id().ok_or_else(missing)?,
        context.goal_id().ok_or_else(missing)?,
        context.goal_created_at().ok_or_else(missing)?,
        context.dag_revision().ok_or_else(missing)?,
        context.dag_base_goal_revision().ok_or_else(missing)?,
        context.node_id().ok_or_else(missing)?,
    )?;
    if message.channel() != MessageChannel::Direct(target.participation_id()) {
        return Err(OperationsError::Conflict("node message recipient channel"));
    }
    validate_sender(transaction, message, witness)?;
    let recipient = recipient_binding(transaction, target, witness)?;
    let binding = crate::node_team::read_node_team_binding_in_transaction(transaction, scope)?;
    match witness.target() {
        NodeMessageRouteTarget::Agent {
            assignment_id,
            agent_id,
            session_id,
            ..
        } => {
            if binding
                .as_ref()
                .and_then(crate::NodeTeamBinding::team_id)
                .is_some()
                || recipient.activity != scope.activity_id()
                || recipient.agent.into_uuid() != agent_id
                || recipient.session.into_uuid() != session_id
            {
                return Err(OperationsError::Conflict(
                    "node message Agent responsibility",
                ));
            }
            let affinity = crate::team_assignment::read_assignment_affinity_in_transaction(
                transaction,
                scope.workspace_id(),
                AgentAssignmentId::from_uuid(assignment_id),
            )?
            .ok_or(OperationsError::Conflict(
                "node message assignment admission",
            ))?;
            if affinity.node_scope() != Some(scope)
                || affinity.team_id().is_some()
                || affinity.binding_revision()
                    != binding.as_ref().map(crate::NodeTeamBinding::revision)
                || affinity.target_agent_id() != recipient.agent
                || affinity.target_session_id() != recipient.session
                || affinity.target_participant_id() != recipient.participant
                || affinity.target_participation_id() != target.participation_id()
                || !matches!(
                    affinity.disposition(),
                    AssignmentAffinityDisposition::Pending | AssignmentAffinityDisposition::Active
                )
            {
                return Err(OperationsError::Conflict(
                    "node message assignment admission",
                ));
            }
        }
        NodeMessageRouteTarget::Team {
            team_id,
            binding_revision,
            leadership_revision,
        } => {
            let expected_revision = u64::try_from(binding_revision)
                .map_err(|_| OperationsError::Conflict("node message Team revision"))?;
            if !binding.as_ref().is_some_and(|binding| {
                binding.team_id() == Some(team_id) && binding.revision() == expected_revision
            }) {
                return Err(OperationsError::Conflict(
                    "node message Team responsibility",
                ));
            }
            let authority = crate::leadership::current_leadership_authority_in_transaction(
                transaction,
                LeadershipScope::team(scope.workspace_id(), team_id),
            )?
            .ok_or(OperationsError::Conflict("node message Team leader"))?;
            if authority.participant_id() != recipient.participant
                || authority.leadership_revision() != leadership_revision
            {
                return Err(OperationsError::Conflict("node message Team leader"));
            }
            let joined: bool = transaction.query_row(
                "SELECT EXISTS(SELECT 1 FROM ops_teams AS t JOIN ops_team_members AS m ON m.workspace_id=t.workspace_id AND m.team_id=t.team_id WHERE t.workspace_id=?1 AND t.team_id=?2 AND t.state='ACTIVE' AND m.participation_id=?3 AND m.state='JOINED')",
                params![scope.workspace_id().into_uuid().as_bytes(), team_id.as_bytes(), target.participation_id().into_uuid().as_bytes()],
                |row| row.get(0),
            )?;
            if !joined {
                return Err(OperationsError::Conflict("node message Team membership"));
            }
        }
    }
    Ok(())
}

fn validate_sender(
    transaction: &Transaction<'_>,
    message: &WorkspaceMessage,
    witness: &NodeMessageRouteWitness,
) -> Result<(), OperationsError> {
    let actor = crate::operational_actor::resolve_actor(
        transaction,
        message.workspace_id(),
        witness.sender_actor(),
    )?;
    if !super::author_matches_resolved(message.author(), &actor)
        || !witness.matches_author(message.author())
    {
        return Err(OperationsError::Conflict("node message sender identity"));
    }
    match (message.author(), witness.sender_leadership_revision()) {
        (MessageAuthor::Operator, None) => Ok(()),
        (
            MessageAuthor::Participant { participant_id, .. }
            | MessageAuthor::NativeParticipant { participant_id, .. },
            Some(revision),
        ) => {
            let authority = crate::leadership::current_leadership_authority_in_transaction(
                transaction,
                LeadershipScope::workspace(message.workspace_id()),
            )?;
            if authority.is_some_and(|authority| {
                authority.participant_id() == *participant_id
                    && authority.leadership_revision() == revision
            }) {
                Ok(())
            } else {
                Err(OperationsError::Conflict("node message sender leadership"))
            }
        }
        _ => Err(OperationsError::Conflict("node message sender witness")),
    }
}

struct RecipientBinding {
    activity: ActivityId,
    agent: AgentInstanceId,
    session: AgentSessionId,
    participant: Uuid,
}

fn recipient_binding(
    transaction: &Transaction<'_>,
    target: &ProviderMessageTarget,
    witness: &NodeMessageRouteWitness,
) -> Result<RecipientBinding, OperationsError> {
    let route = crate::workspace_chat_target::read_workspace_chat_target_in_transaction(
        transaction,
        target.workspace_id(),
        target.profile_id(),
        witness.operation_id(),
    )?
    .ok_or(OperationsError::Conflict("node message activated route"))?;
    if route.participation_id() != target.participation_id()
        || route.runtime_session_id() != target.runtime_session_id()
        || route.provider_conversation_id() != target.provider_conversation_id()
    {
        return Err(OperationsError::Conflict("node message activated route"));
    }
    let revision = i64::try_from(witness.participation_revision())
        .map_err(|_| OperationsError::Conflict("node message participation revision"))?;
    let raw = transaction.query_row(
        "SELECT substr(l.participant_id,1,17),substr(l.session_id,1,17),substr(b.agent_id,1,17) FROM ops_workspace_participations AS p JOIN ops_workspace_participation_mappings AS m ON m.workspace_id=p.workspace_id AND m.participation_id=p.participation_id AND m.agent_profile_id=p.agent_profile_id JOIN ops_logical_members AS l ON l.workspace_id=m.workspace_id AND l.participant_id=m.participant_id JOIN ops_current_effect_bindings AS b ON b.workspace_id=l.workspace_id AND b.participant_id=l.participant_id AND b.session_id=l.session_id AND b.activity_id=?6 WHERE p.workspace_id=?1 AND p.agent_profile_id=?2 AND p.participation_id=?3 AND p.revision=?4 AND m.profile_mapping_request_id=?5",
        params![target.workspace_id().into_uuid().as_bytes(),target.profile_id().into_uuid().as_bytes(),target.participation_id().into_uuid().as_bytes(),revision,witness.operation_id().as_bytes(),route.activity_id().into_uuid().as_bytes()],
        |row| Ok((row.get::<_, Vec<u8>>(0)?,row.get::<_, Vec<u8>>(1)?,row.get::<_, Vec<u8>>(2)?)),
    ).optional()?.ok_or(OperationsError::Conflict("node message current recipient"))?;
    Ok(RecipientBinding {
        activity: route.activity_id(),
        participant: Uuid::from_bytes(wire::id(raw.0)?),
        session: AgentSessionId::from_uuid(Uuid::from_bytes(wire::id(raw.1)?)),
        agent: AgentInstanceId::from_uuid(Uuid::from_bytes(wire::id(raw.2)?)),
    })
}

pub(super) const MAX_NODE_ROUTE_WITNESS_BYTES: usize = 2048;

/// Exactly one node responsibility destination. Team-owned nodes cannot use
/// the Agent variant to bypass the Team leader.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum NodeMessageRouteTarget {
    Agent {
        assignment_id: Uuid,
        assignment_revision: i64,
        agent_id: Uuid,
        session_id: Uuid,
    },
    Team {
        team_id: Uuid,
        binding_revision: i64,
        leadership_revision: i64,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum RouteSender {
    Operator,
    Participant {
        activity_id: Uuid,
        agent_id: Uuid,
        session_id: Uuid,
    },
}

impl From<OperationalActor> for RouteSender {
    fn from(actor: OperationalActor) -> Self {
        match actor {
            OperationalActor::LocalOperator => Self::Operator,
            OperationalActor::Participant {
                activity_id,
                agent_id,
                session_id,
            } => Self::Participant {
                activity_id: activity_id.into_uuid(),
                agent_id: agent_id.into_uuid(),
                session_id: session_id.into_uuid(),
            },
        }
    }
}

/// Versioned immutable supplement to the message context and outbox target.
/// Local operators have no sender leadership revision; participant sends must
/// retain the exact Workspace leadership revision observed before admission.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct NodeMessageRouteWitness {
    version: u8,
    operation_id: Uuid,
    participation_revision: u64,
    runtime_revision: i64,
    sender_leadership_revision: Option<i64>,
    sender: RouteSender,
    target: NodeMessageRouteTarget,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RouteWire {
    version: u8,
    operation_id: Uuid,
    participation_revision: u64,
    runtime_revision: i64,
    sender_leadership_revision: Option<i64>,
    sender: RouteSender,
    target: NodeMessageRouteTarget,
}

impl NodeMessageRouteWitness {
    /// Construct observed routing facts; this does not validate live authority.
    ///
    /// # Errors
    /// Rejects nil identities, nonpositive revisions and unsigned overflow.
    pub fn new(
        operation_id: Uuid,
        participation_revision: u64,
        runtime_revision: i64,
        sender: OperationalActor,
        sender_leadership_revision: Option<i64>,
        target: NodeMessageRouteTarget,
    ) -> Result<Self, OperationsError> {
        let value = Self {
            version: 1,
            operation_id,
            participation_revision,
            runtime_revision,
            sender_leadership_revision,
            sender: sender.into(),
            target,
        };
        value.validate()?;
        Ok(value)
    }

    #[must_use]
    pub const fn operation_id(&self) -> Uuid {
        self.operation_id
    }

    #[must_use]
    pub const fn participation_revision(&self) -> u64 {
        self.participation_revision
    }

    #[must_use]
    pub const fn runtime_revision(&self) -> i64 {
        self.runtime_revision
    }

    #[must_use]
    pub const fn sender_leadership_revision(&self) -> Option<i64> {
        self.sender_leadership_revision
    }

    #[must_use]
    pub const fn target(&self) -> NodeMessageRouteTarget {
        self.target
    }

    #[must_use]
    pub fn sender_actor(&self) -> OperationalActor {
        match self.sender {
            RouteSender::Operator => OperationalActor::LocalOperator,
            RouteSender::Participant {
                activity_id,
                agent_id,
                session_id,
            } => OperationalActor::Participant {
                activity_id: ActivityId::from_uuid(activity_id),
                agent_id: AgentInstanceId::from_uuid(agent_id),
                session_id: AgentSessionId::from_uuid(session_id),
            },
        }
    }

    #[must_use]
    pub fn matches_actor(&self, actor: OperationalActor) -> bool {
        self.sender == RouteSender::from(actor)
    }

    pub(super) fn matches_author(&self, author: &MessageAuthor) -> bool {
        match (self.sender, author) {
            (RouteSender::Operator, MessageAuthor::Operator) => true,
            (
                RouteSender::Participant { session_id, .. },
                MessageAuthor::Participant {
                    session_id: stored, ..
                },
            )
            | (
                RouteSender::Participant { session_id, .. },
                MessageAuthor::NativeParticipant {
                    session_id: stored, ..
                },
            ) => session_id == stored.into_uuid(),
            _ => false,
        }
    }

    fn validate(&self) -> Result<(), OperationsError> {
        let valid_sender = match (self.sender, self.sender_leadership_revision) {
            (RouteSender::Operator, None) => true,
            (
                RouteSender::Participant {
                    activity_id,
                    agent_id,
                    session_id,
                },
                Some(revision),
            ) => {
                !activity_id.is_nil() && !agent_id.is_nil() && !session_id.is_nil() && revision > 0
            }
            _ => false,
        };
        let valid_target = match self.target {
            NodeMessageRouteTarget::Agent {
                assignment_id,
                assignment_revision,
                agent_id,
                session_id,
            } => {
                !assignment_id.is_nil()
                    && assignment_revision > 0
                    && !agent_id.is_nil()
                    && !session_id.is_nil()
            }
            NodeMessageRouteTarget::Team {
                team_id,
                binding_revision,
                leadership_revision,
            } => !team_id.is_nil() && binding_revision > 0 && leadership_revision > 0,
        };
        if self.version != 1
            || self.operation_id.is_nil()
            || self.participation_revision == 0
            || i64::try_from(self.participation_revision).is_err()
            || self.runtime_revision <= 0
            || self
                .sender_leadership_revision
                .is_some_and(|revision| revision <= 0)
            || !valid_target
            || !valid_sender
        {
            return Err(OperationsError::Conflict("node message route witness"));
        }
        Ok(())
    }

    pub(super) fn encode(&self) -> Result<Vec<u8>, OperationsError> {
        self.validate()?;
        let bytes = serde_json::to_vec(self)
            .map_err(|_| OperationsError::Conflict("node message route encoding"))?;
        if bytes.len() > MAX_NODE_ROUTE_WITNESS_BYTES {
            return Err(OperationsError::Bounds("node message route witness"));
        }
        Ok(bytes)
    }

    pub(super) fn decode(bytes: &[u8]) -> Result<Self, OperationsError> {
        if bytes.is_empty() || bytes.len() > MAX_NODE_ROUTE_WITNESS_BYTES {
            return Err(OperationsError::Corrupt("node message route size"));
        }
        let value: RouteWire = serde_json::from_slice(bytes)
            .map_err(|_| OperationsError::Corrupt("node message route encoding"))?;
        if value.version != 1 {
            return Err(OperationsError::Corrupt("node message route version"));
        }
        Self::new(
            value.operation_id,
            value.participation_revision,
            value.runtime_revision,
            match value.sender {
                RouteSender::Operator => OperationalActor::LocalOperator,
                RouteSender::Participant {
                    activity_id,
                    agent_id,
                    session_id,
                } => OperationalActor::Participant {
                    activity_id: ActivityId::from_uuid(activity_id),
                    agent_id: AgentInstanceId::from_uuid(agent_id),
                    session_id: AgentSessionId::from_uuid(session_id),
                },
            },
            value.sender_leadership_revision,
            value.target,
        )
        .map_err(|_| OperationsError::Corrupt("node message route witness"))
    }
}
