//! Exact generation Node-Team reads through the existing Operations authority.

use crate::{OrchestrationRuntime, RuntimeError, participation::ParticipationTarget};
use forge_domain::{
    ActivityId, AgentAssignmentId, AgentInstanceId, AgentSessionId, AuthenticatedPrincipal,
    DagRevisionNumber, WorkspaceId,
};
use heterarchy_orchestration_operations::{
    AssignmentAffinityReservationPage, NodeTeamBinding, NodeTeamBindingScope, OperationalActor,
    SetNodeTeamBindingCommand,
};
use uuid::Uuid;

/// Trusted host observation of the exact Forge-owned node generation before a
/// Node-Team mutation. This is an in-process witness, never request-body data.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NodeTeamSourceObservation {
    pub scope: NodeTeamBindingScope,
    pub current_dag_revision: DagRevisionNumber,
    pub assignment_present: bool,
}

/// Request to compare-and-swap one exact Node-Team binding.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NodeTeamSet {
    pub scope: NodeTeamBindingScope,
    pub expected_revision: Option<u64>,
    pub team_id: Option<Uuid>,
    pub expected_sender_leadership_revision: Option<i64>,
    pub observed_at: forge_domain::TimestampMillis,
}

/// Complete owner read: exact binding, retained aggregate legacy row and bounded reservations.
#[derive(Debug)]
pub struct NodeTeamRead {
    pub binding: Option<NodeTeamBinding>,
    pub legacy_binding: Option<NodeTeamBinding>,
    pub reservations: AssignmentAffinityReservationPage,
}

impl OrchestrationRuntime {
    /// Read exact generation state for a host-authorized operator bridge.
    ///
    /// # Errors
    /// Rejects unauthorized operators and owner storage failures.
    pub fn read_node_team_operator(
        &self,
        principal: AuthenticatedPrincipal,
        scope: NodeTeamBindingScope,
        after_assignment: Option<AgentAssignmentId>,
    ) -> Result<NodeTeamRead, RuntimeError> {
        self.require_operator(principal)?;
        let catalog = self.catalog()?;
        Self::node_team_snapshot(&catalog, scope, after_assignment)
    }

    /// Set an exact Node-Team binding for an authenticated Workspace participant.
    /// The sender Activity is validated independently of the historical node
    /// Activity recorded by `command.scope`.
    ///
    /// # Errors
    /// Rejects stale or assigned Source observations, foreign or non-Ready
    /// participation, missing leadership fences, stale CAS revisions and
    /// Operations owner failures.
    pub fn set_node_team(
        &self,
        target: ParticipationTarget,
        principal: AuthenticatedPrincipal,
        command: NodeTeamSet,
        source: NodeTeamSourceObservation,
    ) -> Result<NodeTeamBinding, RuntimeError> {
        if target.workspace_id != command.scope.workspace_id().into_uuid() {
            return Err(RuntimeError::InvalidInput);
        }
        if command
            .expected_sender_leadership_revision
            .is_none_or(|revision| revision <= 0)
        {
            return Err(RuntimeError::InvalidInput);
        }
        validate_source_observation(command, source)?;
        self.require_participation(target, principal)?;
        let mut catalog = self.catalog()?;
        Self::require_ready_sender_binding(&catalog, target)?;
        let derived_agent = crate::leadership::member_agent_id(
            target.workspace_id,
            target.activity_id,
            target.session_id,
        );
        if derived_agent.into_uuid() != target.agent_id {
            return Err(RuntimeError::Forbidden);
        }

        let actor = OperationalActor::Participant {
            activity_id: ActivityId::from_uuid(target.activity_id),
            agent_id: AgentInstanceId::from_uuid(target.agent_id),
            session_id: AgentSessionId::from_uuid(target.session_id),
        };
        Self::commit_node_team_binding(&mut catalog, command, actor)
    }

    /// Set an exact Node-Team binding from the explicit host operator surface.
    ///
    /// # Errors
    /// Rejects non-operators, stale or assigned Source observations, participant
    /// leadership fences, stale CAS revisions and Operations owner failures.
    pub fn set_node_team_operator(
        &self,
        principal: AuthenticatedPrincipal,
        command: NodeTeamSet,
        source: NodeTeamSourceObservation,
    ) -> Result<NodeTeamBinding, RuntimeError> {
        self.require_operator(principal)?;
        if command.expected_sender_leadership_revision.is_some() {
            return Err(RuntimeError::InvalidInput);
        }
        validate_source_observation(command, source)?;
        let mut catalog = self.catalog()?;
        Self::commit_node_team_binding(&mut catalog, command, OperationalActor::LocalOperator)
    }
    /// Read an exact node responsibility after owned Ready Activity binding authorization.
    ///
    /// # Errors
    /// Rejects foreign/closed Session, mismatched Activity/Workspace or corrupt owner state.
    pub fn read_node_team(
        &self,
        target: ParticipationTarget,
        principal: AuthenticatedPrincipal,
        scope: NodeTeamBindingScope,
        after_assignment: Option<AgentAssignmentId>,
    ) -> Result<NodeTeamRead, RuntimeError> {
        if scope.workspace_id().into_uuid() != target.workspace_id
            || after_assignment.is_some_and(|id| id.into_uuid().is_nil())
        {
            return Err(RuntimeError::InvalidInput);
        }
        self.require_participation(target, principal)?;
        let catalog = self.catalog()?;
        Self::require_ready_sender_binding(&catalog, target)?;
        Self::node_team_snapshot(&catalog, scope, after_assignment)
    }

    fn require_ready_sender_binding(
        catalog: &heterarchy_orchestration_operations::OperationsCatalog,
        target: ParticipationTarget,
    ) -> Result<(), RuntimeError> {
        let workspace_id = WorkspaceId::from_uuid(target.workspace_id);
        let identity = catalog
            .logical_identity_for_session(
                workspace_id,
                AgentSessionId::from_uuid(target.session_id),
            )?
            .ok_or(RuntimeError::Forbidden)?;
        if !identity.effect_bindings().iter().any(|binding| {
            binding.activity_id().into_uuid() == target.activity_id
                && binding.agent_id().into_uuid() == target.agent_id
        }) {
            return Err(RuntimeError::Forbidden);
        }
        Ok(())
    }

    fn commit_node_team_binding(
        catalog: &mut heterarchy_orchestration_operations::OperationsCatalog,
        command: NodeTeamSet,
        actor: OperationalActor,
    ) -> Result<NodeTeamBinding, RuntimeError> {
        catalog
            .set_node_team_binding(SetNodeTeamBindingCommand {
                scope: command.scope,
                expected_revision: command.expected_revision,
                team_id: command.team_id,
                actor,
                expected_sender_leadership_revision: command.expected_sender_leadership_revision,
                observed_at: command.observed_at,
            })
            .map_err(RuntimeError::Persistence)
    }

    fn node_team_snapshot(
        catalog: &heterarchy_orchestration_operations::OperationsCatalog,
        scope: NodeTeamBindingScope,
        after_assignment: Option<AgentAssignmentId>,
    ) -> Result<NodeTeamRead, RuntimeError> {
        if after_assignment.is_some_and(|id| id.into_uuid().is_nil()) {
            return Err(RuntimeError::InvalidInput);
        }
        let binding = catalog.node_team_binding(scope)?;
        let legacy_binding = catalog.legacy_node_team_binding(
            scope.workspace_id(),
            scope.goal_id(),
            scope.dag_revision(),
            scope.node_id(),
        )?;
        let reservations = catalog.assignment_affinity_reservations(scope, after_assignment, 32)?;
        Ok(NodeTeamRead {
            binding,
            legacy_binding,
            reservations,
        })
    }
}

fn validate_source_observation(
    command: NodeTeamSet,
    source: NodeTeamSourceObservation,
) -> Result<(), RuntimeError> {
    if source.scope != command.scope {
        return Err(RuntimeError::Conflict);
    }
    if source.current_dag_revision != command.scope.dag_revision() || source.assignment_present {
        return Err(RuntimeError::Conflict);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use forge_domain::{
        ActivityId, AgentInstanceId, DagNodeId, DagRevisionNumber, DirectTokenId, GoalId,
        GoalRevision, TimestampMillis, WorkspaceId,
    };
    use uuid::Uuid;

    #[test]
    fn node_team_read_cannot_promote_participant_to_operator_or_guess_scope()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = std::env::temp_dir().join(format!("node-team-owner-{}", Uuid::now_v7()));
        std::fs::create_dir_all(&root)?;
        let runtime = OrchestrationRuntime::open(&root)?;
        let scope = NodeTeamBindingScope::new(
            WorkspaceId::from_uuid(Uuid::from_u128(1)),
            ActivityId::from_uuid(Uuid::from_u128(2)),
            GoalId::from_uuid(Uuid::from_u128(3)),
            TimestampMillis::new(1)?,
            DagRevisionNumber::new(1)?,
            GoalRevision::new(1)?,
            DagNodeId::from_uuid(Uuid::from_u128(4)),
        )?;
        let principal =
            AuthenticatedPrincipal::DirectToken(DirectTokenId::from_uuid(Uuid::from_u128(5)));
        assert!(matches!(
            runtime.read_node_team_operator(principal, scope, None),
            Err(RuntimeError::Forbidden)
        ));
        let target = ParticipationTarget {
            workspace_id: Uuid::from_u128(1),
            activity_id: Uuid::from_u128(2),
            agent_id: AgentInstanceId::from_uuid(Uuid::from_u128(6)).into_uuid(),
            session_id: Uuid::from_u128(7),
        };
        assert!(matches!(
            runtime.read_node_team(target, principal, scope, None),
            Err(RuntimeError::Forbidden)
        ));
        let changed = ParticipationTarget {
            workspace_id: Uuid::from_u128(8),
            ..target
        };
        assert!(matches!(
            runtime.read_node_team(changed, principal, scope, None),
            Err(RuntimeError::InvalidInput)
        ));
        let read =
            runtime.read_node_team_operator(AuthenticatedPrincipal::TrustedStdio, scope, None)?;
        assert!(
            read.binding.is_none()
                && read.legacy_binding.is_none()
                && read.reservations.receipts().is_empty()
        );
        drop(runtime);
        std::fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn node_team_set_keeps_operator_and_participant_authority_separate()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = std::env::temp_dir().join(format!("node-team-set-auth-{}", Uuid::now_v7()));
        std::fs::create_dir_all(&root)?;
        let runtime = OrchestrationRuntime::open(&root)?;
        let scope = node_team_scope()?;
        let source = current_node_team_source(scope);
        let token =
            AuthenticatedPrincipal::DirectToken(DirectTokenId::from_uuid(Uuid::from_u128(5)));

        assert!(matches!(
            runtime.set_node_team_operator(token, node_team_set(scope, None, None)?, source),
            Err(RuntimeError::Forbidden)
        ));
        assert!(matches!(
            runtime.set_node_team_operator(
                AuthenticatedPrincipal::TrustedStdio,
                node_team_set(scope, None, Some(1))?,
                source,
            ),
            Err(RuntimeError::InvalidInput)
        ));

        let sender_activity = Uuid::from_u128(9);
        let sender_session = Uuid::from_u128(10);
        let target = ParticipationTarget {
            workspace_id: scope.workspace_id().into_uuid(),
            activity_id: sender_activity,
            agent_id: crate::leadership::member_agent_id(
                scope.workspace_id().into_uuid(),
                sender_activity,
                sender_session,
            )
            .into_uuid(),
            session_id: sender_session,
        };
        let participant_set = NodeTeamSet {
            expected_sender_leadership_revision: Some(1),
            ..node_team_set(scope, None, None)?
        };
        assert!(matches!(
            runtime.set_node_team(
                target,
                AuthenticatedPrincipal::TrustedStdio,
                participant_set,
                source,
            ),
            Err(RuntimeError::Forbidden)
        ));
        assert!(matches!(
            runtime.set_node_team(
                target,
                token,
                NodeTeamSet {
                    expected_sender_leadership_revision: None,
                    ..participant_set
                },
                source,
            ),
            Err(RuntimeError::InvalidInput)
        ));
        assert!(
            runtime
                .read_node_team_operator(AuthenticatedPrincipal::TrustedStdio, scope, None)?
                .binding
                .is_none()
        );

        drop(runtime);
        std::fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn node_team_set_rejects_stale_assigned_and_mismatched_source_observations()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = std::env::temp_dir().join(format!("node-team-set-source-{}", Uuid::now_v7()));
        std::fs::create_dir_all(&root)?;
        let runtime = OrchestrationRuntime::open(&root)?;
        let scope = node_team_scope()?;
        let command = node_team_set(scope, None, None)?;

        assert!(matches!(
            runtime.set_node_team_operator(
                AuthenticatedPrincipal::TrustedStdio,
                command,
                NodeTeamSourceObservation {
                    scope,
                    current_dag_revision: DagRevisionNumber::new(2)?,
                    assignment_present: false,
                },
            ),
            Err(RuntimeError::Conflict)
        ));
        assert!(matches!(
            runtime.set_node_team_operator(
                AuthenticatedPrincipal::TrustedStdio,
                command,
                NodeTeamSourceObservation {
                    assignment_present: true,
                    ..current_node_team_source(scope)
                },
            ),
            Err(RuntimeError::Conflict)
        ));
        let other_scope = NodeTeamBindingScope::new(
            scope.workspace_id(),
            scope.activity_id(),
            scope.goal_id(),
            scope.goal_created_at(),
            scope.dag_revision(),
            scope.dag_base_goal_revision(),
            DagNodeId::from_uuid(Uuid::from_u128(11)),
        )?;
        assert!(matches!(
            runtime.set_node_team_operator(
                AuthenticatedPrincipal::TrustedStdio,
                command,
                current_node_team_source(other_scope),
            ),
            Err(RuntimeError::Conflict)
        ));
        assert!(
            runtime
                .read_node_team_operator(AuthenticatedPrincipal::TrustedStdio, scope, None)?
                .binding
                .is_none()
        );

        drop(runtime);
        std::fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn operator_can_clear_exact_node_team_with_cas_and_retains_tombstone()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = std::env::temp_dir().join(format!("node-team-set-cas-{}", Uuid::now_v7()));
        std::fs::create_dir_all(&root)?;
        let runtime = OrchestrationRuntime::open(&root)?;
        let scope = node_team_scope()?;
        let source = current_node_team_source(scope);

        let created = runtime.set_node_team_operator(
            AuthenticatedPrincipal::TrustedStdio,
            node_team_set(scope, None, None)?,
            source,
        )?;
        assert_eq!(created.scope(), Some(scope));
        assert_eq!(created.team_id(), None);
        assert_eq!(created.revision(), 1);
        assert_eq!(created.bound_by(), "operator");

        let cleared = runtime.set_node_team_operator(
            AuthenticatedPrincipal::TrustedStdio,
            node_team_set(scope, Some(created.revision()), None)?,
            source,
        )?;
        assert_eq!(cleared.team_id(), None);
        assert_eq!(cleared.revision(), 2);

        assert!(matches!(
            runtime.set_node_team_operator(
                AuthenticatedPrincipal::TrustedStdio,
                node_team_set(scope, Some(created.revision()), None)?,
                source,
            ),
            Err(RuntimeError::Persistence(
                heterarchy_orchestration_operations::OperationsError::Conflict(
                    "node binding revision",
                )
            ))
        ));
        let retained = runtime
            .read_node_team_operator(AuthenticatedPrincipal::TrustedStdio, scope, None)?
            .binding
            .ok_or_else(|| std::io::Error::other("unbound tombstone was not retained"))?;
        assert_eq!(retained.scope(), Some(scope));
        assert_eq!(retained.team_id(), None);
        assert_eq!(retained.revision(), cleared.revision());

        drop(runtime);
        std::fs::remove_dir_all(root)?;
        Ok(())
    }

    fn node_team_scope() -> Result<NodeTeamBindingScope, Box<dyn std::error::Error>> {
        Ok(NodeTeamBindingScope::new(
            WorkspaceId::from_uuid(Uuid::from_u128(1)),
            ActivityId::from_uuid(Uuid::from_u128(2)),
            GoalId::from_uuid(Uuid::from_u128(3)),
            TimestampMillis::new(1)?,
            DagRevisionNumber::new(1)?,
            GoalRevision::new(1)?,
            DagNodeId::from_uuid(Uuid::from_u128(4)),
        )?)
    }

    fn current_node_team_source(scope: NodeTeamBindingScope) -> NodeTeamSourceObservation {
        NodeTeamSourceObservation {
            scope,
            current_dag_revision: scope.dag_revision(),
            assignment_present: false,
        }
    }

    fn node_team_set(
        scope: NodeTeamBindingScope,
        expected_revision: Option<u64>,
        expected_sender_leadership_revision: Option<i64>,
    ) -> Result<NodeTeamSet, Box<dyn std::error::Error>> {
        Ok(NodeTeamSet {
            scope,
            expected_revision,
            team_id: None,
            expected_sender_leadership_revision,
            observed_at: TimestampMillis::new(10)?,
        })
    }
}
