//! Shared full native Activity coordination projection.

use forge_domain::{
    ActivityCoordinatorBinding, ActivityCoordinatorRevision, AgentEvent, AgentEventKind,
    AgentParticipantRef, CoordinationSnapshot, CoordinatorSnapshot, DagHeadRevision,
    DagRevisionNumber,
};
use heterarchy_orchestration_operations::{
    LogicalActivityJoinedEvent, LogicalActivityParticipant, LogicalActivitySnapshot,
};
use serde_json::{Value, json};

pub(super) fn snapshot_json(
    logical: &LogicalActivitySnapshot,
    planning_coordinator: Option<&ActivityCoordinatorBinding>,
) -> Result<Value, crate::RuntimeError> {
    let logical_participants = u32::try_from(logical.participants().len())
        .map_err(|_| crate::RuntimeError::InvalidInput)?;
    let planning_coordinator_count = i64::from(planning_coordinator.is_some());
    Ok(json!({
        "workspace_id": logical.workspace_id().to_string(),
        "activity_id": logical.activity_id().to_string(),
        "membership_revision": Value::Null,
        "membership_revision_scope": "per_participant",
        "coordinator": planning_coordinator_output(
            planning_coordinator,
            logical.participants(),
        ),
        "counts": {
            "current_coordinator": planning_coordinator_count,
            "attached_participants": logical_participants,
            "ready_logical_participants": logical_participants,
            "unresolved_legacy": Value::Null,
            "closed_activity": Value::Null,
            "handed_off": Value::Null,
            "detached": Value::Null,
        },
        "unread_count": logical.unread_count(),
        "unread_join_count": logical.unread_join_count(),
        "participants": logical
            .participants()
            .iter()
            .map(logical_activity_participant_output)
            .collect::<Vec<_>>(),
        "events": logical
            .events()
            .iter()
            .map(logical_joined_event_output)
            .collect::<Vec<_>>(),
        "legacy_history": {
            "source": "orchestration_imported_cutover_snapshot",
            "freshness": "cutover_snapshot_not_live",
            "live_after_cutover_writes_included": false,
            "truncated": logical.imported_history_truncated(),
            "events": logical
                .imported_history_events()
                .iter()
                .map(event_output)
                .collect::<Vec<_>>(),
        },
        "authority": {
            "participants": "orchestration_logical_membership",
            "presence": "orchestration_logical_presence",
            "inbox": "orchestration_logical_inbox",
            "planning_coordinator": "forge_activity_planning",
            "legacy_history": "orchestration_imported_cutover_snapshot",
        },
        "snapshot_consistency": "cross_authority_read",
    }))
}

fn participant_output(value: &AgentParticipantRef) -> Value {
    json!({"agent_id": value.agent_id().to_string(), "session_id": value.session_id().map(|id| id.to_string()), "display_name": value.display_name().as_str()})
}

fn coordinator_output(value: CoordinatorSnapshot) -> Value {
    json!({"agent_id": value.agent_id().map(|id| id.to_string()), "session_id": value.session_id().map(|id| id.to_string()), "revision": value.revision().map(ActivityCoordinatorRevision::get)})
}

fn plan_output(value: CoordinationSnapshot) -> Value {
    json!({"goal": value.goal().map(|goal| json!({"goal_id": goal.goal_id().to_string(), "revision": goal.expected_revision().get()})),
        "dag_revision": value.dag_revision().map(DagRevisionNumber::get), "dag_head_revision": value.dag_head_revision().map(DagHeadRevision::get),
        "node": value.node().map(|node| json!({"node_id": node.node_id().to_string(), "revision": node.revision().get()}))})
}

fn planning_coordinator_output(
    binding: Option<&ActivityCoordinatorBinding>,
    participants: &[LogicalActivityParticipant],
) -> Value {
    let Some(binding) = binding else {
        return json!({
            "agent_id": Value::Null,
            "session_id": Value::Null,
            "participant_id": Value::Null,
            "revision": Value::Null,
            "authority": "forge_activity_planning",
        });
    };
    let participant = participants
        .iter()
        .find(|participant| participant.agent_id() == binding.agent_id());
    json!({
        "agent_id": binding.agent_id().to_string(),
        "session_id": participant.map(|value| value.session_id().to_string()),
        "participant_id": participant.map(|value| value.participant_id().to_string()),
        "revision": binding.revision().get(),
        "authority": "forge_activity_planning",
    })
}

fn logical_activity_participant_output(value: &LogicalActivityParticipant) -> Value {
    let transport = match value.transport() {
        forge_domain::AgentTransportKind::Retired => "Mcp",
        forge_domain::AgentTransportKind::Stdio => "Stdio",
        forge_domain::AgentTransportKind::StreamableHttp => "StreamableHttp",
        forge_domain::AgentTransportKind::Other => "Other",
    };
    json!({
        "participant_id": value.participant_id().to_string(),
        "agent_id": value.agent_id().to_string(),
        "session_id": value.session_id().to_string(),
        "display_name": value.display_name().as_str(),
        "declared_role": value.declared_role().as_str(),
        "transport": transport,
        "partition": "ready_logical_participant",
        "membership_revision": value.membership_revision(),
        "presence_revision": value.presence_revision().get(),
        "last_authenticated_at_ms": value
            .last_authenticated_at()
            .map(forge_domain::TimestampMillis::get),
    })
}

fn logical_joined_event_output(value: &LogicalActivityJoinedEvent) -> Value {
    let participant = json!({
        "participant_id": value.participant_id().to_string(),
        "agent_id": value.agent_id().to_string(),
        "session_id": value.session_id().to_string(),
        "display_name": value.display_name().as_str(),
    });
    json!({
        "event_id": value.event_id().to_string(),
        "workspace_id": value.workspace_id().to_string(),
        "activity_id": value.activity_id().to_string(),
        "membership_revision": value.membership_revision(),
        "kind": "joined",
        "from": Value::Null,
        "to": participant,
        "plan_before": Value::Null,
        "plan_after": Value::Null,
        "coordinator_before": Value::Null,
        "coordinator_after": Value::Null,
        "observed_at_ms": value.observed_at().get(),
    })
}

fn event_output(value: &AgentEvent) -> Value {
    let kind = match value.kind() {
        AgentEventKind::Joined => "joined",
        AgentEventKind::Left => "left",
        AgentEventKind::HandoffStarted => "handoff_started",
        AgentEventKind::HandoffCompleted => "handoff_completed",
        AgentEventKind::FocusChanged => "focus_changed",
        AgentEventKind::RoleChanged => "role_changed",
    };
    json!({"event_id": value.id().to_string(), "workspace_id": value.workspace_id().to_string(), "activity_id": value.activity_id().to_string(),
        "membership_revision": value.membership_revision().get(), "kind": kind,
        "from": value.from().map(participant_output), "to": value.to().map(participant_output),
        "plan_before": plan_output(value.coordination_before()), "plan_after": plan_output(value.coordination_after()),
        "coordinator_before": coordinator_output(value.coordinator_before()), "coordinator_after": coordinator_output(value.coordinator_after()),
        "observed_at_ms": value.observed_at().get()})
}
