//! Preserve native exact/legacy responsibility and reservation projections.
use super::{
    AssignmentAffinityDisposition, AssignmentAffinityReceipt, NodeTeamBinding,
    NodeTeamBindingScope, NodeTeamScopeWire, RuntimeError,
};
use crate::wire::node_team::{
    NodeTeamBindingWire, NodeTeamLegacyRetirementWire, NodeTeamReservationDispositionWire,
    NodeTeamReservationScopeRelationWire, NodeTeamReservationWire, NodeTeamRetirementAuthorityWire,
    NodeTeamSnapshotWire,
};

pub(super) fn snapshot(
    read: &heterarchy_orchestration_runtime::node_team::NodeTeamRead,
    scope: NodeTeamBindingScope,
    node_scope: NodeTeamScopeWire,
    after: Option<uuid::Uuid>,
) -> Result<NodeTeamSnapshotWire, RuntimeError> {
    let reservations = read
        .reservations
        .receipts()
        .iter()
        .map(|receipt| reservation(receipt, scope))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(NodeTeamSnapshotWire {
        workspace_id: scope.workspace_id().into_uuid(),
        node_scope,
        after_assignment_id: after,
        binding: read.binding.as_ref().map(binding),
        legacy_binding: read.legacy_binding.as_ref().map(binding),
        reservations,
        next_assignment_id: read
            .reservations
            .next_assignment()
            .map(forge_domain::AgentAssignmentId::into_uuid),
    })
}

/// Project a canonical binding without changing its exact or legacy generation.
#[must_use]
pub fn binding(value: &NodeTeamBinding) -> NodeTeamBindingWire {
    NodeTeamBindingWire {
        activity_id: value.scope().map(|scope| scope.activity_id().into_uuid()),
        goal_created_at_ms: value.scope().map(|scope| scope.goal_created_at().get()),
        dag_base_goal_revision: value
            .scope()
            .map(|scope| scope.dag_base_goal_revision().get()),
        goal_id: value.goal_id().into_uuid(),
        dag_revision: value.dag_revision().get(),
        node_id: value.node_id().into_uuid(),
        team_id: value.team_id(),
        bound_by: value.bound_by().to_owned(),
        bound_at_ms: value.bound_at().get(),
        revision: value.revision(),
        legacy_retirement: value.legacy_retirement().map(retirement),
    }
}

/// Project retained retirement evidence without inventing a generation or grant.
#[must_use]
pub fn retirement(
    receipt: &heterarchy_orchestration_operations::LegacyNodeTeamBindingRetirementReceipt,
) -> NodeTeamLegacyRetirementWire {
    NodeTeamLegacyRetirementWire {
        request_id: receipt.request_id(),
        retired_at_ms: receipt.retired_at().get(),
        binding_revision: receipt.binding_revision(),
        authority: match receipt.authority() {
            heterarchy_orchestration_operations::LegacyNodeTeamBindingRetirementAuthority::LocalOperator => NodeTeamRetirementAuthorityWire::LocalOperator,
            heterarchy_orchestration_operations::LegacyNodeTeamBindingRetirementAuthority::WorkspaceLeader { .. } => NodeTeamRetirementAuthorityWire::WorkspaceLeader,
        },
    }
}

fn reservation(
    value: &AssignmentAffinityReceipt,
    scope: NodeTeamBindingScope,
) -> Result<NodeTeamReservationWire, RuntimeError> {
    let relation = match value.node_scope() {
        None if value.disposition() == AssignmentAffinityDisposition::LegacyUnknown => {
            NodeTeamReservationScopeRelationWire::LegacyUnknown
        }
        Some(existing) if existing == scope => NodeTeamReservationScopeRelationWire::Exact,
        Some(existing)
            if existing.goal_id() == scope.goal_id()
                && existing.dag_revision() == scope.dag_revision()
                && existing.node_id() == scope.node_id() =>
        {
            NodeTeamReservationScopeRelationWire::HistoricalCoordinates
        }
        _ => return Err(RuntimeError::Conflict),
    };
    let disposition = match value.disposition() {
        AssignmentAffinityDisposition::Pending => NodeTeamReservationDispositionWire::Pending,
        AssignmentAffinityDisposition::Active => NodeTeamReservationDispositionWire::Active,
        AssignmentAffinityDisposition::LegacyUnknown => {
            NodeTeamReservationDispositionWire::LegacyUnknown
        }
        _ => return Err(RuntimeError::Conflict),
    };
    Ok(NodeTeamReservationWire {
        activity_id: value.activity_id().into_uuid(),
        assignment_id: value.assignment_id().into_uuid(),
        disposition,
        scope_relation: relation,
    })
}
