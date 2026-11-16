//! Immutable observed routing facts for exact node-message replay.
//! These facts never grant work or replace fresh admission checks.

use forge_domain::{ActivityId, AgentInstanceId, AgentSessionId};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{OperationalActor, OperationsError};

/// Revalidate Operations-owned routing facts inside the append/enqueue transaction.
/// Forge assignment state and Profile runtime state are ingress observations;
/// this transaction does not claim atomicity with either of those owners.
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
