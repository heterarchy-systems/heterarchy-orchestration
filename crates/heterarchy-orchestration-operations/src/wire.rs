//! Versioned scalar encoding used for immutable import parity and digests.
use crate::OperationsError;
use forge_domain::{
    AgentAttachmentState, AgentFocus, AgentFocusNode, AgentRecordParts, AgentTransportKind,
    AuthenticatedPrincipal, RegisteredAgentInstance,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct FocusWire {
    pub(crate) goal: Option<([u8; 16], i64)>,
    pub(crate) dag: Option<i64>,
    pub(crate) node: Option<([u8; 16], i64)>,
}
impl FocusWire {
    pub(crate) fn from_focus(focus: AgentFocus) -> Self {
        Self {
            goal: focus.goal().map(|v| {
                (
                    v.goal_id().into_uuid().into_bytes(),
                    v.expected_revision().get(),
                )
            }),
            dag: focus
                .dag_revision()
                .map(forge_domain::DagRevisionNumber::get),
            node: focus.node().map(|v| {
                (
                    v.node_id().into_uuid().into_bytes(),
                    v.expected_revision().get(),
                )
            }),
        }
    }
    pub(crate) fn focus(&self) -> Result<AgentFocus, OperationsError> {
        let goal = self
            .goal
            .map(|(id, rev)| {
                Ok::<_, OperationsError>(forge_domain::GoalAttribution::new(
                    forge_domain::GoalId::from_uuid(Uuid::from_bytes(id)),
                    forge_domain::GoalRevision::new(rev)
                        .map_err(|_| OperationsError::Corrupt("focus goal revision"))?,
                ))
            })
            .transpose()?;
        let dag = self
            .dag
            .map(forge_domain::DagRevisionNumber::new)
            .transpose()
            .map_err(|_| OperationsError::Corrupt("focus DAG revision"))?;
        let node = self
            .node
            .map(|(id, rev)| {
                Ok::<_, OperationsError>(AgentFocusNode::new(
                    forge_domain::DagNodeId::from_uuid(Uuid::from_bytes(id)),
                    forge_domain::DagNodeRevision::new(rev)
                        .map_err(|_| OperationsError::Corrupt("focus node revision"))?,
                ))
            })
            .transpose()?;
        AgentFocus::new(goal, dag, node).map_err(|_| OperationsError::Corrupt("focus shape"))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct MemberWire {
    pub(crate) agent: [u8; 16],
    pub(crate) workspace: [u8; 16],
    pub(crate) activity: [u8; 16],
    pub(crate) session: Option<[u8; 16]>,
    pub(crate) display_name: String,
    pub(crate) declared_role: String,
    pub(crate) transport: i64,
    pub(crate) principal_kind: i64,
    pub(crate) principal_id: Option<[u8; 16]>,
    pub(crate) attachment: i64,
    pub(crate) binding_revision: i64,
    pub(crate) created_at: i64,
    pub(crate) updated_at: i64,
    pub(crate) source_focus: FocusWire,
}

impl MemberWire {
    pub(crate) fn from_agent(agent: &RegisteredAgentInstance) -> Self {
        let (principal_kind, principal_id) = match agent.principal() {
            AuthenticatedPrincipal::TrustedStdio => (0, None),
            AuthenticatedPrincipal::DirectToken(id) => (1, Some(id.into_uuid().into_bytes())),
            AuthenticatedPrincipal::OAuthGrant(id) => (2, Some(id.into_uuid().into_bytes())),
        };
        Self {
            agent: agent.id().into_uuid().into_bytes(),
            workspace: agent.workspace_id().into_uuid().into_bytes(),
            activity: agent.activity_id().into_uuid().into_bytes(),
            session: agent.session_id().map(|v| v.into_uuid().into_bytes()),
            display_name: agent.display_name().as_str().to_owned(),
            declared_role: agent.declared_role().as_str().to_owned(),
            transport: match agent.transport_kind() {
                AgentTransportKind::Retired => 0,
                AgentTransportKind::Stdio => 1,
                AgentTransportKind::StreamableHttp => 2,
                AgentTransportKind::Other => 3,
            },
            principal_kind,
            principal_id,
            attachment: match agent.attachment_state() {
                AgentAttachmentState::Attached => 0,
                AgentAttachmentState::Detached => 1,
            },
            binding_revision: agent.revision().get(),
            created_at: agent.created_at().get(),
            updated_at: agent.updated_at().get(),
            source_focus: FocusWire::from_focus(agent.focus()),
        }
    }
    pub(crate) fn agent(&self) -> Result<RegisteredAgentInstance, OperationsError> {
        let principal = match (self.principal_kind, self.principal_id) {
            (0, None) => AuthenticatedPrincipal::TrustedStdio,
            (1, Some(v)) => AuthenticatedPrincipal::DirectToken(
                forge_domain::DirectTokenId::from_uuid(Uuid::from_bytes(v)),
            ),
            (2, Some(v)) => AuthenticatedPrincipal::OAuthGrant(
                forge_domain::OAuthGrantId::from_uuid(Uuid::from_bytes(v)),
            ),
            _ => return Err(OperationsError::Corrupt("principal")),
        };
        let transport_kind = match self.transport {
            0 => AgentTransportKind::Retired,
            1 => AgentTransportKind::Stdio,
            2 => AgentTransportKind::StreamableHttp,
            3 => AgentTransportKind::Other,
            _ => return Err(OperationsError::Corrupt("transport")),
        };
        let attachment_state = match self.attachment {
            0 => AgentAttachmentState::Attached,
            1 => AgentAttachmentState::Detached,
            _ => return Err(OperationsError::Corrupt("attachment")),
        };
        RegisteredAgentInstance::from_parts(AgentRecordParts {
            id: forge_domain::AgentInstanceId::from_uuid(Uuid::from_bytes(self.agent)),
            workspace_id: forge_domain::WorkspaceId::from_uuid(Uuid::from_bytes(self.workspace)),
            activity_id: forge_domain::ActivityId::from_uuid(Uuid::from_bytes(self.activity)),
            session_id: self
                .session
                .map(|v| forge_domain::AgentSessionId::from_uuid(Uuid::from_bytes(v))),
            display_name: forge_domain::AgentDisplayName::new(self.display_name.clone())
                .map_err(|_| OperationsError::Corrupt("display name"))?,
            declared_role: forge_domain::AgentDeclaredRole::new(self.declared_role.clone())
                .map_err(|_| OperationsError::Corrupt("declared role"))?,
            transport_kind,
            principal,
            attachment_state,
            focus: self.source_focus.focus()?,
            created_at: forge_domain::TimestampMillis::new(self.created_at)
                .map_err(|_| OperationsError::Corrupt("created timestamp"))?,
            updated_at: forge_domain::TimestampMillis::new(self.updated_at)
                .map_err(|_| OperationsError::Corrupt("updated timestamp"))?,
            revision: forge_domain::AgentInstanceRevision::new(self.binding_revision)
                .map_err(|_| OperationsError::Corrupt("binding revision"))?,
        })
        .map_err(|_| OperationsError::Corrupt("member record"))
    }
}

pub(crate) fn id(bytes: Vec<u8>) -> Result<[u8; 16], OperationsError> {
    bytes
        .try_into()
        .map_err(|_| OperationsError::Corrupt("identity length"))
}

#[cfg(test)]
mod retired_transport_tests {
    use super::{FocusWire, MemberWire};
    use forge_domain::AgentTransportKind;
    use uuid::Uuid;

    #[test]
    fn persisted_transport_zero_remains_retired_on_read_and_reencode()
    -> Result<(), Box<dyn std::error::Error>> {
        let wire = MemberWire {
            agent: Uuid::from_u128(1).into_bytes(),
            workspace: Uuid::from_u128(2).into_bytes(),
            activity: Uuid::from_u128(3).into_bytes(),
            session: Some(Uuid::from_u128(4).into_bytes()),
            display_name: "historical participant".to_owned(),
            declared_role: "worker".to_owned(),
            transport: 0,
            principal_kind: 0,
            principal_id: None,
            attachment: 1,
            binding_revision: 2,
            created_at: 1,
            updated_at: 2,
            source_focus: FocusWire {
                goal: None,
                dag: None,
                node: None,
            },
        };
        let historical_bytes = serde_json::to_vec(&wire)?;
        let restored: MemberWire = serde_json::from_slice(&historical_bytes)?;
        let agent = restored.agent()?;
        assert_eq!(agent.transport_kind(), AgentTransportKind::Retired);
        assert_eq!(
            serde_json::to_vec(&MemberWire::from_agent(&agent))?,
            historical_bytes
        );
        let mut invalid = restored;
        invalid.transport = 4;
        assert!(invalid.agent().is_err());
        Ok(())
    }
}
