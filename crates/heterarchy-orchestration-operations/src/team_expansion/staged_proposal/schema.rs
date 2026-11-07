//! Additive persistence for effect-free Coordinator proposals.

use rusqlite::Transaction;

use crate::OperationsError;

const PROPOSALS: &str = "CREATE TABLE ops_team_expansion_staged_agent_proposals(
    proposal_id BLOB PRIMARY KEY NOT NULL CHECK(length(proposal_id)=16 AND proposal_id!=zeroblob(16)),
    request_id BLOB NOT NULL UNIQUE CHECK(length(request_id)=16 AND request_id!=zeroblob(16)),
    workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16),
    team_id BLOB NOT NULL CHECK(length(team_id)=16 AND team_id!=zeroblob(16)),
    decision_id BLOB NOT NULL CHECK(length(decision_id)=16 AND decision_id!=zeroblob(16)),
    approval_request_id BLOB NOT NULL CHECK(length(approval_request_id)=16 AND approval_request_id!=zeroblob(16)),
    approval_expected_team_revision INTEGER NOT NULL CHECK(approval_expected_team_revision>=1),
    approval_capacity_revision INTEGER NOT NULL CHECK(approval_capacity_revision>=1),
    ordinal INTEGER NOT NULL CHECK(ordinal BETWEEN 0 AND 7),
    status TEXT NOT NULL CHECK(status='COORDINATOR_PROPOSED_UNVERIFIED'),
    expected_team_revision INTEGER NOT NULL CHECK(expected_team_revision>=1),
    capacity_revision INTEGER NOT NULL CHECK(capacity_revision>=1),
    orchestrator_revision INTEGER NOT NULL CHECK(orchestrator_revision>=1),
    workspace_participation_revision INTEGER NOT NULL CHECK(workspace_participation_revision>=1),
    activity_id BLOB NOT NULL CHECK(length(activity_id)=16 AND activity_id!=zeroblob(16)),
    agent_id BLOB NOT NULL CHECK(length(agent_id)=16 AND agent_id!=zeroblob(16)),
    session_id BLOB NOT NULL CHECK(length(session_id)=16 AND session_id!=zeroblob(16)),
    coordinator_revision INTEGER NOT NULL CHECK(coordinator_revision>0),
    requested_count INTEGER NOT NULL CHECK(requested_count=1),
    objective TEXT NOT NULL CHECK(length(CAST(objective AS BLOB)) BETWEEN 1 AND 4096),
    capability TEXT NOT NULL CHECK(length(CAST(capability AS BLOB)) BETWEEN 1 AND 128),
    effect_scope INTEGER NOT NULL CHECK(effect_scope IN(0,1)),
    independence_requirement INTEGER NOT NULL CHECK(independence_requirement IN(0,1)),
    reason TEXT NOT NULL CHECK(length(CAST(reason AS BLOB)) BETWEEN 1 AND 2048),
    actor_profile_id BLOB NOT NULL CHECK(length(actor_profile_id)=16 AND actor_profile_id!=zeroblob(16)),
    actor_participation_id BLOB NOT NULL CHECK(length(actor_participation_id)=16 AND actor_participation_id!=zeroblob(16)),
    actor_participant_id BLOB NOT NULL CHECK(length(actor_participant_id)=16 AND actor_participant_id!=zeroblob(16)),
    proposal_digest BLOB NOT NULL CHECK(length(proposal_digest)=32),
    recorded_at_ms INTEGER NOT NULL CHECK(recorded_at_ms>=0),
    UNIQUE(workspace_id,team_id,decision_id,ordinal),
    FOREIGN KEY(workspace_id,team_id,decision_id,ordinal)
        REFERENCES ops_team_expansion_slots(workspace_id,team_id,decision_id,ordinal),
    FOREIGN KEY(workspace_id,team_id,decision_id,ordinal)
        REFERENCES ops_team_expansion_workspace_hold_plans(workspace_id,team_id,decision_id,ordinal)
)";

const PROPOSALS_NEW_PROFILE_SOURCE: &str = "CREATE TRIGGER ops_team_expansion_staged_agent_proposals_new_profile_source
BEFORE INSERT ON ops_team_expansion_staged_agent_proposals
WHEN NOT EXISTS(
    SELECT 1
      FROM ops_team_expansion_decisions AS decision
      JOIN ops_team_expansion_workspace_hold_manifests AS manifest
        ON manifest.workspace_id=decision.workspace_id
       AND manifest.team_id=decision.team_id
       AND manifest.decision_id=decision.decision_id
       AND manifest.request_id=decision.request_id
      JOIN ops_team_expansion_slots AS slot
        ON slot.workspace_id=decision.workspace_id
       AND slot.team_id=decision.team_id
       AND slot.decision_id=decision.decision_id
       AND slot.request_id=decision.request_id
       AND slot.ordinal=NEW.ordinal
      JOIN ops_team_expansion_workspace_hold_plans AS plan
        ON plan.workspace_id=manifest.workspace_id
       AND plan.team_id=manifest.team_id
       AND plan.decision_id=manifest.decision_id
       AND plan.ordinal=NEW.ordinal
      JOIN ops_team_expansion_cooldown_anchors AS anchor
        ON anchor.workspace_id=decision.workspace_id
       AND anchor.team_id=decision.team_id
       AND anchor.decision_id=decision.decision_id
     WHERE decision.workspace_id=NEW.workspace_id
       AND decision.team_id=NEW.team_id
       AND decision.decision_id=NEW.decision_id
       AND decision.request_id=NEW.approval_request_id
       AND decision.state='APPROVED'
       AND decision.approved_count=manifest.slot_count
       AND NEW.ordinal<decision.approved_count
       AND decision.expected_team_revision=NEW.approval_expected_team_revision
       AND decision.capacity_revision=NEW.approval_capacity_revision
       AND plan.target_kind=2
       AND plan.target_profile_id IS NULL
       AND plan.target_participation_id IS NULL
       AND NEW.requested_count=1
       AND NOT EXISTS(
           SELECT 1 FROM ops_team_expansion_slot_assignments AS assignment
            WHERE assignment.workspace_id=NEW.workspace_id AND assignment.team_id=NEW.team_id
              AND assignment.decision_id=NEW.decision_id AND assignment.ordinal=NEW.ordinal
       )
       AND NOT EXISTS(
           SELECT 1 FROM ops_team_expansion_slot_uses AS slot_use
            WHERE slot_use.workspace_id=NEW.workspace_id AND slot_use.team_id=NEW.team_id
              AND slot_use.decision_id=NEW.decision_id AND slot_use.ordinal=NEW.ordinal
       )
       AND NOT EXISTS(
           SELECT 1 FROM ops_team_expansion_workspace_hold_bindings AS binding
            WHERE binding.workspace_id=NEW.workspace_id AND binding.decision_id=NEW.decision_id
              AND binding.ordinal=NEW.ordinal
       )
       AND NOT EXISTS(
           SELECT 1 FROM ops_agent_requests AS request WHERE request.request_id=NEW.request_id
       )
)
BEGIN SELECT RAISE(ABORT,'staged proposal requires the exact approved unassigned NewProfile hold'); END";

const PROPOSALS_NO_UPDATE: &str =
    "CREATE TRIGGER ops_team_expansion_staged_agent_proposals_no_update
BEFORE UPDATE ON ops_team_expansion_staged_agent_proposals
BEGIN SELECT RAISE(ABORT,'staged Coordinator proposals are immutable'); END";

const PROPOSALS_NO_DELETE: &str =
    "CREATE TRIGGER ops_team_expansion_staged_agent_proposals_no_delete
BEFORE DELETE ON ops_team_expansion_staged_agent_proposals
BEGIN SELECT RAISE(ABORT,'staged Coordinator proposals are retained'); END";

pub(super) const fn schema_objects() -> &'static [(&'static str, &'static str, &'static str)] {
    &[
        (
            "table",
            "ops_team_expansion_staged_agent_proposals",
            PROPOSALS,
        ),
        (
            "trigger",
            "ops_team_expansion_staged_agent_proposals_new_profile_source",
            PROPOSALS_NEW_PROFILE_SOURCE,
        ),
        (
            "trigger",
            "ops_team_expansion_staged_agent_proposals_no_update",
            PROPOSALS_NO_UPDATE,
        ),
        (
            "trigger",
            "ops_team_expansion_staged_agent_proposals_no_delete",
            PROPOSALS_NO_DELETE,
        ),
    ]
}

pub(super) fn create_schema(transaction: &Transaction<'_>) -> Result<(), OperationsError> {
    for &(_, _, statement) in schema_objects() {
        transaction.execute_batch(statement)?;
    }
    Ok(())
}
