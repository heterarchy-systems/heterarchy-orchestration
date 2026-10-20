//! Participant-authorized admission of exact pre-offer Assignment affinities.

use forge_domain::{AuthenticatedPrincipal, WorkspaceId};
use heterarchy_orchestration_operations::{
    AssignmentAffinityCommand, AssignmentAffinityReceipt,
    MAX_ASSIGNMENT_AFFINITY_RESERVATION_PAGE_SIZE, OperationalActor, OperationsCatalog,
};
use heterarchy_orchestration_sessions::AgentProfileId;

use crate::{OrchestrationRuntime, RuntimeError, participation::ParticipationTarget};

/// Result of recording or exactly replaying one immutable admission.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeAssignmentAdmission {
    pub receipt: AssignmentAffinityReceipt,
    pub created: bool,
}

impl OrchestrationRuntime {
    /// Record or exactly replay a Source-validated pre-offer affinity.
    ///
    /// # Errors
    /// Rejects a foreign caller, changed immutable intent, non-Ready target,
    /// stale Node-Team binding or catalog failure. A native Coordinator is
    /// admitted only with an exact current Ready binding and no Team.
    pub fn admit_node_assignment_offer(
        &self,
        requester: ParticipationTarget,
        principal: AuthenticatedPrincipal,
        mut command: AssignmentAffinityCommand,
    ) -> Result<NodeAssignmentAdmission, RuntimeError> {
        self.require_participation(requester, principal)?;
        command.requester = requester_actor(requester);
        let existing = self
            .catalog()?
            .read_assignment_affinity(command.workspace_id, command.assignment_id)?;
        if existing.is_none() {
            self.require_participation(
                ParticipationTarget {
                    workspace_id: command.workspace_id.into_uuid(),
                    activity_id: command.activity_id.into_uuid(),
                    agent_id: command.target_agent_id.into_uuid(),
                    session_id: command.target_session_id.into_uuid(),
                },
                command.expected_target_principal,
            )?;
        }
        let receipt = self
            .catalog()?
            .record_assignment_affinity(&command)
            .map_err(RuntimeError::from)?;
        let created = !receipt.replayed();
        Ok(NodeAssignmentAdmission { receipt, created })
    }

    /// Read one immutable affinity and require it to match the complete intent.
    ///
    /// This operation is a pure catalog read. An absent row returns `None` and
    /// never authorizes a caller to retry the original mutation.
    ///
    /// # Errors
    /// Rejects a foreign caller, cross-Workspace alias, changed immutable intent
    /// or catalog failure. Native requesters are re-resolved against their current
    /// Ready identity, effect binding and exact non-Team Node binding.
    pub fn recover_node_assignment_offer(
        &self,
        requester: ParticipationTarget,
        principal: AuthenticatedPrincipal,
        mut command: AssignmentAffinityCommand,
    ) -> Result<Option<AssignmentAffinityReceipt>, RuntimeError> {
        self.require_participation(requester, principal)?;
        command.requester = requester_actor(requester);
        let catalog = self.catalog()?;
        let receipt = catalog.read_assignment_affinity(
            WorkspaceId::from_uuid(requester.workspace_id),
            command.assignment_id,
        )?;
        let Some(receipt) = receipt else {
            return Ok(None);
        };
        if !receipt_matches(&receipt, &command, requester)
            || !current_requester_matches(&catalog, &receipt, requester)?
        {
            return Err(RuntimeError::Conflict);
        }
        Ok(Some(receipt))
    }
}

fn requester_actor(requester: ParticipationTarget) -> OperationalActor {
    OperationalActor::Participant {
        activity_id: forge_domain::ActivityId::from_uuid(requester.activity_id),
        agent_id: forge_domain::AgentInstanceId::from_uuid(requester.agent_id),
        session_id: forge_domain::AgentSessionId::from_uuid(requester.session_id),
    }
}

fn receipt_matches(
    receipt: &AssignmentAffinityReceipt,
    command: &AssignmentAffinityCommand,
    requester: ParticipationTarget,
) -> bool {
    let actor = receipt.requester();
    receipt.workspace_id() == command.workspace_id
        && receipt.assignment_id() == command.assignment_id
        && receipt.activity_id() == command.activity_id
        && receipt.target_agent_id() == command.target_agent_id
        && receipt.target_session_id() == command.target_session_id
        && receipt.team_id() == command.team_id
        && receipt.node_scope() == Some(command.node_scope)
        && receipt.binding_revision() == command.expected_binding_revision
        && receipt.assignment_payload_fingerprint() == command.assignment_payload_fingerprint
        && receipt.original_offer_payload() == Some(command.original_offer_payload.as_slice())
        && receipt.observed_at() == command.observed_at
        && actor.activity_id().map(forge_domain::ActivityId::into_uuid)
            == Some(requester.activity_id)
        && actor
            .agent_id()
            .map(forge_domain::AgentInstanceId::into_uuid)
            == Some(requester.agent_id)
        && actor
            .session_id()
            .map(forge_domain::AgentSessionId::into_uuid)
            == Some(requester.session_id)
}

fn current_requester_matches(
    catalog: &OperationsCatalog,
    receipt: &AssignmentAffinityReceipt,
    requester: ParticipationTarget,
) -> Result<bool, RuntimeError> {
    let stored = receipt.requester();
    let workspace = WorkspaceId::from_uuid(requester.workspace_id);
    if let (Some(profile_id), Some(participant_id), Some(participation_id)) = (
        stored.profile_id(),
        stored.participant_id(),
        stored.participation_id(),
    ) {
        let profile = AgentProfileId::from_uuid(profile_id).map_err(|_| RuntimeError::Conflict)?;
        let Some(participation) = catalog.workspace_participation(workspace, profile)? else {
            return Ok(false);
        };
        let session = forge_domain::AgentSessionId::from_uuid(requester.session_id);
        let Some(identity) = catalog.logical_identity_for_session(workspace, session)? else {
            return Ok(false);
        };
        return Ok(identity.participant_id() == participant_id
            && participation.participation_id() == participation_id
            && participation.profile_id() == profile
            && identity.effect_bindings().iter().any(|binding| {
                binding.activity_id().into_uuid() == requester.activity_id
                    && binding.agent_id().into_uuid() == requester.agent_id
            }));
    }

    let (Some(activity_id), Some(agent_id), Some(session_id), Some(participant_id)) = (
        stored.activity_id(),
        stored.agent_id(),
        stored.session_id(),
        stored.participant_id(),
    ) else {
        return Ok(false);
    };
    let session = forge_domain::AgentSessionId::from_uuid(requester.session_id);
    let Some(identity) = catalog.logical_identity_for_session(workspace, session)? else {
        return Ok(false);
    };
    if stored.profile_id().is_some()
        || stored.participation_id().is_some()
        || activity_id.into_uuid() != requester.activity_id
        || agent_id.into_uuid() != requester.agent_id
        || session_id.into_uuid() != requester.session_id
        || identity.participant_id() != participant_id
        || !identity.effect_bindings().iter().any(|binding| {
            binding.activity_id().into_uuid() == requester.activity_id
                && binding.agent_id().into_uuid() == requester.agent_id
        })
    {
        return Ok(false);
    }
    let actor = OperationalActor::Participant {
        activity_id,
        agent_id,
        session_id,
    };
    if !catalog.current_native_assignment_affinity_requester_matches(workspace, actor, stored)? {
        return Ok(false);
    }
    current_native_node_binding_matches(catalog, receipt)
}

fn current_native_node_binding_matches(
    catalog: &OperationsCatalog,
    receipt: &AssignmentAffinityReceipt,
) -> Result<bool, RuntimeError> {
    if receipt.team_id().is_some() {
        return Ok(false);
    }
    let Some(scope) = receipt.node_scope() else {
        return Ok(false);
    };
    let binding = catalog.node_team_binding(scope)?;
    match (receipt.binding_revision(), binding) {
        (Some(expected_revision), Some(binding))
            if binding.revision() == expected_revision && binding.team_id().is_none() => {}
        (None, None) => {}
        _ => return Ok(false),
    }
    if catalog
        .legacy_node_team_binding(
            scope.workspace_id(),
            scope.goal_id(),
            scope.dag_revision(),
            scope.node_id(),
        )?
        .is_some_and(|legacy| legacy.legacy_retirement().is_none())
    {
        return Ok(false);
    }
    let reservations = catalog.assignment_affinity_reservations(
        scope,
        None,
        MAX_ASSIGNMENT_AFFINITY_RESERVATION_PAGE_SIZE,
    )?;
    if reservations.next_assignment().is_some()
        || reservations
            .receipts()
            .iter()
            .any(|reservation| reservation.assignment_id() != receipt.assignment_id())
    {
        return Ok(false);
    }
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use forge_domain::{
        AgentAssignmentId, AgentInstanceId, AgentSessionId, DagNodeId, DagRevisionNumber,
        DirectTokenId, GoalId, GoalRevision, TimestampMillis,
    };
    use heterarchy_orchestration_operations::NodeTeamBindingScope;
    use rusqlite::{Connection, params};
    use uuid::Uuid;

    #[test]
    #[allow(
        clippy::too_many_lines,
        reason = "The fixture keeps both Ready participants and immutable admission recovery in one exact catalog lifecycle."
    )]
    fn exact_replay_and_recovery_bind_observed_time_and_current_requester()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = std::env::temp_dir().join(format!("assignment-admit-{}", Uuid::now_v7()));
        std::fs::create_dir_all(&root)?;
        let runtime = OrchestrationRuntime::open(&root)?;
        let principal =
            AuthenticatedPrincipal::DirectToken(DirectTokenId::from_uuid(Uuid::from_u128(1)));
        let requester_session = runtime
            .open_session(Uuid::from_u128(2), principal, 1)?
            .session;
        let workspace = WorkspaceId::from_uuid(Uuid::from_u128(3));
        let activity = Uuid::from_u128(4);
        let requester_agent = Uuid::from_u128(5);
        let target_agent = Uuid::from_u128(6);
        let target_session = runtime
            .open_session(Uuid::from_u128(7), principal, 1)?
            .session
            .session_id;
        let requester_participant = Uuid::from_u128(8);
        let target_participant = Uuid::from_u128(9);
        let requester_profile = Uuid::from_u128(10);
        let target_profile = Uuid::from_u128(11);
        let requester_participation = Uuid::from_u128(12);
        let target_participation = Uuid::from_u128(13);
        let connection = Connection::open(root.join("agent-operations.sqlite3"))?;
        connection.execute(
            "INSERT INTO ops_imports(migration_id,workspace_id,destination_id,source_revision,phase,next_cursor,page_count,record_count,import_digest,verified_pages,verify_digest) VALUES(?1,?2,?3,1,2,NULL,5,0,?4,5,?4)",
            params![
                Uuid::from_u128(14).as_bytes(),
                workspace.into_uuid().as_bytes(),
                runtime.destination_id().as_bytes(),
                [0_u8; 32].as_slice()
            ],
        )?;
        for (profile, participation) in [
            (requester_profile, requester_participation),
            (target_profile, target_participation),
        ] {
            connection.execute(
                "INSERT INTO ops_workspace_participations(workspace_id,agent_profile_id,participation_id,created_at_ms,revision) VALUES(?1,?2,?3,1,1)",
                params![workspace.into_uuid().as_bytes(),profile.as_bytes(),participation.as_bytes()],
            )?;
        }
        for (participant, session, profile, participation, agent, request) in [
            (
                requester_participant,
                requester_session.session_id,
                requester_profile,
                requester_participation,
                requester_agent,
                Uuid::from_u128(15),
            ),
            (
                target_participant,
                target_session,
                target_profile,
                target_participation,
                target_agent,
                Uuid::from_u128(16),
            ),
        ] {
            connection.execute(
                "INSERT INTO ops_logical_members(workspace_id,participant_id,session_id,principal_kind,principal_id,display_name,declared_role,transport_kind,membership_revision,created_at_ms) VALUES(?1,?2,?3,1,?4,'member','researcher',2,1,1)",
                params![workspace.into_uuid().as_bytes(),participant.as_bytes(),session.as_bytes(),Uuid::from_u128(1).as_bytes()],
            )?;
            connection.execute(
                "INSERT INTO ops_workspace_participation_mappings(request_id,workspace_id,participation_id,agent_profile_id,profile_revision,profile_mapping_request_id,session_id,session_revision,participant_id,participant_membership_revision,mapped_at_ms) VALUES(?1,?2,?3,?4,1,?1,?5,1,?6,1,1)",
                params![request.as_bytes(),workspace.into_uuid().as_bytes(),participation.as_bytes(),profile.as_bytes(),session.as_bytes(),participant.as_bytes()],
            )?;
            connection.execute(
                "INSERT INTO ops_current_effect_bindings(workspace_id,participant_id,activity_id,session_id,agent_id,source_kind,source_request_id) VALUES(?1,?2,?3,?4,?5,0,?6)",
                params![workspace.into_uuid().as_bytes(),participant.as_bytes(),activity.as_bytes(),session.as_bytes(),agent.as_bytes(),request.as_bytes()],
            )?;
        }
        let requester = ParticipationTarget {
            workspace_id: workspace.into_uuid(),
            activity_id: activity,
            agent_id: requester_agent,
            session_id: requester_session.session_id,
        };
        let scope = NodeTeamBindingScope::new(
            workspace,
            forge_domain::ActivityId::from_uuid(activity),
            GoalId::from_uuid(Uuid::from_u128(17)),
            TimestampMillis::new(18)?,
            DagRevisionNumber::new(1)?,
            GoalRevision::new(1)?,
            DagNodeId::from_uuid(Uuid::from_u128(19)),
        )?;
        let command = AssignmentAffinityCommand {
            workspace_id: workspace,
            assignment_id: AgentAssignmentId::from_uuid(Uuid::from_u128(20)),
            activity_id: forge_domain::ActivityId::from_uuid(activity),
            target_agent_id: AgentInstanceId::from_uuid(target_agent),
            target_session_id: AgentSessionId::from_uuid(target_session),
            expected_target_principal: principal,
            requester: OperationalActor::LocalOperator,
            team_id: None,
            node_scope: scope,
            expected_binding_revision: None,
            assignment_payload_fingerprint: [0xA5; 32],
            original_offer_payload: br#"{"action":{"kind":"offer"}}"#.to_vec(),
            observed_at: TimestampMillis::new(21)?,
        };
        let first = runtime.admit_node_assignment_offer(requester, principal, command.clone())?;
        assert!(first.created);
        let replay = runtime.admit_node_assignment_offer(requester, principal, command.clone())?;
        assert!(!replay.created);
        assert!(replay.receipt.replayed());
        assert_eq!(
            replay.receipt.assignment_id(),
            first.receipt.assignment_id()
        );
        assert_eq!(
            replay.receipt.assignment_payload_fingerprint(),
            first.receipt.assignment_payload_fingerprint()
        );
        assert_eq!(
            runtime.recover_node_assignment_offer(requester, principal, command.clone(),)?,
            Some(first.receipt.clone())
        );
        let mut changed_time = command.clone();
        changed_time.observed_at = TimestampMillis::new(22)?;
        assert!(matches!(
            runtime.recover_node_assignment_offer(requester, principal, changed_time),
            Err(RuntimeError::Conflict)
        ));
        connection.execute(
            "DELETE FROM ops_current_effect_bindings WHERE workspace_id=?1 AND activity_id=?2 AND agent_id=?3",
            params![workspace.into_uuid().as_bytes(),activity.as_bytes(),requester_agent.as_bytes()],
        )?;
        assert!(matches!(
            runtime.recover_node_assignment_offer(requester, principal, command),
            Err(RuntimeError::Conflict)
        ));
        drop(connection);
        drop(runtime);
        std::fs::remove_dir_all(root)?;
        Ok(())
    }
}
