//! Bounded HTTP client for complete Activity coordination queries.

use std::collections::HashSet;

use hyper::Method;
use hyper::header::HeaderValue;
use uuid::Uuid;

use crate::wire::coordination::{
    CoordinationCoordinatorWire, CoordinationEventKind, CoordinationEventWire,
    CoordinationParticipantWire, CoordinationQueryRequest, CoordinationQueryResponse,
    CoordinationQuerySnapshot,
};

use super::{ClientError, OrchestrationHttpClient};

const MAX_COORDINATION_ROWS: usize = 64;
const MAX_DISPLAY_NAME_BYTES: usize = 128;
const MAX_ROLE_BYTES: usize = 128;

impl OrchestrationHttpClient {
    /// Read the complete native coordination projection for one Activity.
    ///
    /// The caller's original credential remains the only authority. This is a
    /// read-only query, so an uncertain exchange is never retried or converted
    /// into a mutation readback.
    ///
    /// # Errors
    /// Rejects invalid scope or identity, malformed or mismatched projections,
    /// typed service rejections, and bounded transport failures.
    pub async fn agent_coordination_get(
        &self,
        authorization: &HeaderValue,
        request: &CoordinationQueryRequest,
    ) -> Result<CoordinationQueryResponse, ClientError> {
        if !super::teams::valid_scope(&request.scope)
            || request.activity_id.is_nil()
            || request.session_id.is_some_and(|id| id.is_nil())
        {
            return Err(ClientError::InvalidRequest);
        }

        let response: CoordinationQueryResponse = self
            .call(
                authorization,
                Method::POST,
                format!(
                    "/internal/v1/workspaces/{}/activities/{}/coordination/query",
                    request.scope.workspace_id, request.activity_id
                ),
                Some(request),
                None,
            )
            .await?;
        if response.request != *request
            || !valid_snapshot(&response.snapshot, request)
            || response
                .catch_up
                .as_ref()
                .is_some_and(|progress| !valid_catch_up(progress))
        {
            return Err(ClientError::Protocol);
        }
        Ok(response)
    }
}

fn valid_catch_up(value: &crate::wire::coordination::CoordinationCatchUpWire) -> bool {
    !value.migration_id.is_nil()
        && !value.destination_id.is_nil()
        && value.page_limit == 64
        && value.page_size <= value.page_limit
        && value.applied_through >= value.requested_after
        && value.more_may_remain == (value.page_size == value.page_limit)
}

fn valid_snapshot(
    snapshot: &CoordinationQuerySnapshot,
    request: &CoordinationQueryRequest,
) -> bool {
    if snapshot.workspace_id != request.scope.workspace_id
        || snapshot.activity_id != request.activity_id
        || snapshot.membership_revision.is_some()
        || snapshot.counts.unresolved_legacy.is_some()
        || snapshot.counts.closed_activity.is_some()
        || snapshot.counts.handed_off.is_some()
        || snapshot.counts.detached.is_some()
        || snapshot.counts.current_coordinator != i64::from(snapshot.coordinator.agent_id.is_some())
        || snapshot.counts.attached_participants as usize != snapshot.participants.len()
        || snapshot.counts.ready_logical_participants as usize != snapshot.participants.len()
        || snapshot.unread_count != snapshot.unread_join_count
        || snapshot.participants.len() > MAX_COORDINATION_ROWS
        || snapshot.events.len() > MAX_COORDINATION_ROWS
        || snapshot.legacy_history.events.len() > MAX_COORDINATION_ROWS
        || (snapshot.legacy_history.truncated
            && snapshot.legacy_history.events.len() != MAX_COORDINATION_ROWS)
        || snapshot.legacy_history.live_after_cutover_writes_included
        || snapshot
            .participants
            .iter()
            .any(|participant| !valid_participant(participant, request.scope.workspace_id))
        || !unique_participants(&snapshot.participants)
        || !snapshot
            .participants
            .windows(2)
            .all(|pair| pair[0].participant_id.as_bytes() < pair[1].participant_id.as_bytes())
        || snapshot
            .events
            .iter()
            .any(|event| !valid_event(event, request.scope.workspace_id, request.activity_id, true))
        || !current_events_match_participants(&snapshot.events, &snapshot.participants)
        || snapshot.legacy_history.events.iter().any(|event| {
            !valid_event(
                event,
                request.scope.workspace_id,
                request.activity_id,
                false,
            )
        })
        || !unique_events(&snapshot.events)
        || !unique_events(&snapshot.legacy_history.events)
        || !snapshot.events.windows(2).all(|pair| {
            pair[0].observed_at_ms > pair[1].observed_at_ms
                || (pair[0].observed_at_ms == pair[1].observed_at_ms
                    && pair[0].event_id.as_bytes() > pair[1].event_id.as_bytes())
        })
        || !snapshot.legacy_history.events.windows(2).all(|pair| {
            pair[0].membership_revision > pair[1].membership_revision
                || (pair[0].membership_revision == pair[1].membership_revision
                    && pair[0].event_id.as_bytes() > pair[1].event_id.as_bytes())
        })
    {
        return false;
    }

    valid_coordinator(&snapshot.coordinator, &snapshot.participants)
}

fn current_events_match_participants(
    events: &[CoordinationEventWire],
    participants: &[CoordinationParticipantWire],
) -> bool {
    events.iter().all(|event| {
        event.to.as_ref().is_some_and(|endpoint| {
            participants.iter().any(|participant| {
                endpoint.participant_id == Some(participant.participant_id)
                    && endpoint.agent_id == participant.agent_id
                    && endpoint.session_id == Some(participant.session_id)
                    && endpoint.display_name == participant.display_name
            })
        })
    })
}

fn valid_participant(value: &CoordinationParticipantWire, workspace_id: Uuid) -> bool {
    !value.participant_id.is_nil()
        && !value.agent_id.is_nil()
        && !value.session_id.is_nil()
        && !value.display_name.trim().is_empty()
        && value.display_name.len() <= MAX_DISPLAY_NAME_BYTES
        && !value.declared_role.trim().is_empty()
        && value.declared_role.len() <= MAX_ROLE_BYTES
        && value.membership_revision > 0
        && value.presence_revision > 0
        && value.last_authenticated_at_ms.is_none_or(|time| time >= 0)
        && workspace_id != Uuid::nil()
}

fn valid_coordinator(
    value: &CoordinationCoordinatorWire,
    participants: &[CoordinationParticipantWire],
) -> bool {
    if value.revision.is_some_and(|revision| revision <= 0)
        || value.agent_id.is_some_and(|id| id.is_nil())
        || value.session_id.is_some_and(|id| id.is_nil())
        || value.participant_id.is_some_and(|id| id.is_nil())
    {
        return false;
    }
    match value.agent_id {
        None => {
            value.session_id.is_none() && value.participant_id.is_none() && value.revision.is_none()
        }
        Some(agent_id) => {
            value.revision.is_some()
                && match (value.session_id, value.participant_id) {
                    (Some(session_id), Some(participant_id)) => {
                        participants.iter().any(|participant| {
                            participant.agent_id == agent_id
                                && participant.session_id == session_id
                                && participant.participant_id == participant_id
                        })
                    }
                    (None, None) => true,
                    _ => false,
                }
        }
    }
}

fn valid_event(
    value: &CoordinationEventWire,
    workspace_id: Uuid,
    activity_id: Uuid,
    current_joined: bool,
) -> bool {
    if value.event_id.is_nil()
        || value.workspace_id != workspace_id
        || value.activity_id != activity_id
        || value.membership_revision == 0
        || value.observed_at_ms < 0
        || value
            .from
            .as_ref()
            .is_some_and(|participant| !valid_event_participant(participant))
        || value
            .to
            .as_ref()
            .is_some_and(|participant| !valid_event_participant(participant))
        || value
            .plan_before
            .as_ref()
            .is_some_and(|plan| !valid_plan(plan))
        || value
            .plan_after
            .as_ref()
            .is_some_and(|plan| !valid_plan(plan))
        || value
            .coordinator_before
            .as_ref()
            .is_some_and(|coordinator| {
                coordinator.agent_id.is_some_and(|id| id.is_nil())
                    || coordinator.session_id.is_some_and(|id| id.is_nil())
                    || coordinator.revision.is_some_and(|revision| revision <= 0)
            })
        || value.coordinator_after.as_ref().is_some_and(|coordinator| {
            coordinator.agent_id.is_some_and(|id| id.is_nil())
                || coordinator.session_id.is_some_and(|id| id.is_nil())
                || coordinator.revision.is_some_and(|revision| revision <= 0)
        })
    {
        return false;
    }

    if current_joined {
        value.kind == CoordinationEventKind::Joined
            && value.from.is_none()
            && value.to.as_ref().is_some_and(|participant| {
                participant.participant_id.is_some()
                    && participant.session_id.is_some()
                    && value.plan_before.is_none()
                    && value.plan_after.is_none()
                    && value.coordinator_before.is_none()
                    && value.coordinator_after.is_none()
            })
    } else {
        value
            .from
            .as_ref()
            .into_iter()
            .chain(value.to.as_ref())
            .all(|participant| participant.participant_id.is_none())
    }
}

fn valid_event_participant(
    value: &crate::wire::coordination::CoordinationEventParticipantWire,
) -> bool {
    value.agent_id != Uuid::nil()
        && value.session_id.is_none_or(|id| !id.is_nil())
        && value.participant_id.is_none_or(|id| !id.is_nil())
        && !value.display_name.trim().is_empty()
        && value.display_name.len() <= MAX_DISPLAY_NAME_BYTES
}

fn valid_plan(value: &crate::wire::coordination::CoordinationPlanWire) -> bool {
    let shape = !(value.goal.is_none()
        && (value.dag_revision.is_some()
            || value.dag_head_revision.is_some()
            || value.node.is_some()))
        && (value.dag_revision.is_some()
            || (value.dag_head_revision.is_none() && value.node.is_none()))
        && (value.dag_revision.is_none() || value.dag_head_revision.is_some());
    shape
        && value
            .goal
            .as_ref()
            .is_none_or(|goal| !goal.goal_id.is_nil() && goal.revision > 0)
        && value.dag_revision.is_none_or(|revision| revision > 0)
        && value.dag_head_revision.is_none_or(|revision| revision > 0)
        && value
            .node
            .as_ref()
            .is_none_or(|node| !node.node_id.is_nil() && node.revision > 0)
}

fn unique_participants(values: &[CoordinationParticipantWire]) -> bool {
    let mut ids = HashSet::with_capacity(values.len());
    values
        .iter()
        .all(|participant| ids.insert(participant.participant_id))
}

fn unique_events(values: &[CoordinationEventWire]) -> bool {
    let mut ids = HashSet::with_capacity(values.len());
    values.iter().all(|event| ids.insert(event.event_id))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounded_catch_up_never_claims_full_source_exhaustion() {
        let mut progress = crate::wire::coordination::CoordinationCatchUpWire {
            migration_id: Uuid::from_u128(1),
            destination_id: Uuid::from_u128(2),
            requested_after: 3,
            applied_through: 67,
            page_size: 64,
            page_limit: 64,
            more_may_remain: true,
        };
        assert!(valid_catch_up(&progress));
        progress.more_may_remain = false;
        assert!(!valid_catch_up(&progress));
        progress.page_size = 0;
        progress.applied_through = 3;
        assert!(valid_catch_up(&progress));
        progress.applied_through = 2;
        assert!(!valid_catch_up(&progress));
    }
    use crate::wire::WorkspaceQueryScope;
    use crate::wire::coordination::CoordinationQueryRequest;

    fn request() -> CoordinationQueryRequest {
        CoordinationQueryRequest {
            scope: WorkspaceQueryScope {
                workspace_id: Uuid::from_u128(1),
                location_id: Uuid::from_u128(2),
                location_revision: 3,
                workspace_revision: 4,
                locator: "workspace".to_owned(),
            },
            activity_id: Uuid::from_u128(5),
            session_id: Some(Uuid::from_u128(6)),
        }
    }

    #[test]
    fn query_contract_requires_nullable_fields_and_rejects_unknown_native_fields()
    -> Result<(), Box<dyn std::error::Error>> {
        let missing_session = serde_json::json!({
            "scope": {
                "workspaceId": Uuid::from_u128(1),
                "locationId": Uuid::from_u128(2),
                "locationRevision": 3,
                "workspaceRevision": 4,
                "locator": "workspace"
            },
            "activityId": Uuid::from_u128(5)
        });
        assert!(serde_json::from_value::<CoordinationQueryRequest>(missing_session).is_err());

        let mut snapshot = serde_json::json!({
            "workspace_id": Uuid::from_u128(1),
            "activity_id": Uuid::from_u128(5),
            "membership_revision": null,
            "membership_revision_scope": "per_participant",
            "coordinator": {
                "agent_id": null,
                "session_id": null,
                "participant_id": null,
                "revision": null,
                "authority": "forge_activity_planning"
            },
            "counts": {
                "current_coordinator": 0,
                "attached_participants": 0,
                "ready_logical_participants": 0,
                "unresolved_legacy": null,
                "closed_activity": null,
                "handed_off": null,
                "detached": null
            },
            "unread_count": 0,
            "unread_join_count": 0,
            "participants": [],
            "events": [],
            "legacy_history": {
                "source": "orchestration_imported_cutover_snapshot",
                "freshness": "cutover_snapshot_not_live",
                "live_after_cutover_writes_included": false,
                "truncated": false,
                "events": []
            },
            "authority": {
                "participants": "orchestration_logical_membership",
                "presence": "orchestration_logical_presence",
                "inbox": "orchestration_logical_inbox",
                "planning_coordinator": "forge_activity_planning",
                "legacy_history": "orchestration_imported_cutover_snapshot"
            },
            "snapshot_consistency": "cross_authority_read"
        });
        let valid: CoordinationQuerySnapshot = serde_json::from_value(snapshot.clone())?;
        assert!(valid_snapshot(&valid, &request()));

        snapshot["unexpected"] = serde_json::json!(true);
        assert!(serde_json::from_value::<CoordinationQuerySnapshot>(snapshot).is_err());
        Ok(())
    }

    #[test]
    fn query_validation_binds_the_complete_scope_and_native_count_invariants()
    -> Result<(), Box<dyn std::error::Error>> {
        let query = request();
        let snapshot: CoordinationQuerySnapshot = serde_json::from_value(serde_json::json!({
            "workspace_id": query.scope.workspace_id,
            "activity_id": query.activity_id,
            "membership_revision": null,
            "membership_revision_scope": "per_participant",
            "coordinator": {"agent_id": null, "session_id": null, "participant_id": null, "revision": null, "authority": "forge_activity_planning"},
            "counts": {"current_coordinator": 0, "attached_participants": 0, "ready_logical_participants": 0, "unresolved_legacy": null, "closed_activity": null, "handed_off": null, "detached": null},
            "unread_count": 0,
            "unread_join_count": 0,
            "participants": [],
            "events": [],
            "legacy_history": {"source": "orchestration_imported_cutover_snapshot", "freshness": "cutover_snapshot_not_live", "live_after_cutover_writes_included": false, "truncated": false, "events": []},
            "authority": {"participants": "orchestration_logical_membership", "presence": "orchestration_logical_presence", "inbox": "orchestration_logical_inbox", "planning_coordinator": "forge_activity_planning", "legacy_history": "orchestration_imported_cutover_snapshot"},
            "snapshot_consistency": "cross_authority_read"
        }))?;
        assert!(valid_snapshot(&snapshot, &query));

        let mut foreign = snapshot.clone();
        foreign.workspace_id = Uuid::from_u128(99);
        assert!(!valid_snapshot(&foreign, &query));

        let mut inconsistent = snapshot;
        inconsistent.counts.ready_logical_participants = 1;
        assert!(!valid_snapshot(&inconsistent, &query));
        Ok(())
    }
}
