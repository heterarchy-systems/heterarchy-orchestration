//! Real Source preparation/proposal/abort over explicitly synthetic bootstrap actors.

use super::*;
use forge_domain::{
    ActivityCoordinatorActor, AgentAssignmentId, AgentAssignmentTarget, AgentDagDraft,
    AgentDagPlanRequest, AgentDagPlanRequestId, AgentWorkUpdateId, AgentWorkUpdateSequence,
    BootstrapActivity, ClaimActivityCoordinator, DagAcceptanceCriterion, DagHeadRevision,
    DagNodeDefinition, DagNodeId, DagNodeObjective, DagNodeOrdinal, DagNodeTitle, DagRevision,
    DagRevisionId, DagRevisionNumber, EstablishWorkspace, GoalAttribution, GoalContract, GoalId,
    GoalObjective, GoalStatement, LocationId, OfferAgentAssignment, RegisteredGoal,
    RuntimeInstanceId, WorkspaceRoot,
};
use forge_persistence::WorkspaceStore;
use forge_workspace::WorkspaceStateLayout;

pub(super) struct Source {
    pub store: WorkspaceStore,
    pub actor: ActivityCoordinatorActor,
    pub assignment: forge_domain::RegisteredAgentAssignment,
    pub recipient: AgentAssignmentTarget,
    pub scope: WorkspaceProjectScope,
}

impl Source {
    pub(super) fn new(
        root: &Path,
        preparation: &crate::TeamPermanentTransferPreparationReceipt,
    ) -> SourceDispatchResult<Self> {
        let source_root = root.join("source");
        fs::create_dir(&source_root)?;
        let layout = WorkspaceStateLayout::initialize(&source_root)?;
        let mut store = WorkspaceStore::open(
            &layout,
            EstablishWorkspace::new(
                WORKSPACE,
                LocationId::from_uuid(SOURCE_LOCATION),
                WorkspaceRoot::from_canonical(layout.workspace_root().to_path_buf())?,
                layout.root_fingerprint()?,
                ts(1)?,
            ),
        )?;
        let (activity, coordinator, coordinator_session) =
            seed_source_actors(&mut store, &layout, preparation)?;
        let binding = store.claim_activity_coordinator(
            ClaimActivityCoordinator::new(
                WORKSPACE,
                activity,
                coordinator,
                AuthenticatedPrincipal::TrustedStdio,
                ts(5)?,
                coordinator_session,
            ),
            ts(5)?,
        )?;
        let actor = ActivityCoordinatorActor::new(
            WORKSPACE,
            activity,
            coordinator,
            binding.revision(),
            AuthenticatedPrincipal::TrustedStdio,
            coordinator_session,
        );
        let assignment = create_work(&mut store, actor)?;
        let recipient_agent = store
            .get_agent(AgentInstanceId::from_uuid(RECIPIENT_AGENT))?
            .ok_or("Source recipient missing")?;
        let recipient = AgentAssignmentTarget::new(
            recipient_agent.id(),
            recipient_agent
                .session_id()
                .ok_or("Source recipient Session missing")?,
            recipient_agent.principal(),
            recipient_agent.revision(),
        );
        let scope = WorkspaceProjectScope::new(
            WORKSPACE.into_uuid(),
            SOURCE_LOCATION,
            1,
            store.workspace()?.revision().get(),
            format!("file://{}", source_root.display()),
        )?;
        let freeze = store.freeze_agent_operations(
            &forge_persistence::AgentOperationsFreezeRequest::new(
                Uuid::from_u128(0xAC20),
                DESTINATION,
                ts(100)?,
            )?,
        )?;
        let _delegation = store.delegate_agent_operations(&freeze, [0x5C; 32], ts(101)?)?;
        Ok(Self {
            store,
            actor,
            assignment,
            recipient,
            scope,
        })
    }
}

fn seed_source_actors(
    store: &mut WorkspaceStore,
    layout: &WorkspaceStateLayout,
    preparation: &crate::TeamPermanentTransferPreparationReceipt,
) -> SourceDispatchResult<(ActivityId, AgentInstanceId, AgentSessionId)> {
    let mut activities = std::collections::BTreeSet::new();
    for binding in preparation.bindings() {
        if activities.insert(binding.activity_id().into_uuid()) {
            let revision = store.workspace()?.revision();
            store.bootstrap_activity(&BootstrapActivity::new(
                binding.activity_id(),
                WORKSPACE,
                revision,
                RuntimeInstanceId::from_uuid(Uuid::now_v7()),
                ts(100_000)?,
                ts(2)?,
            ))?;
        }
        crate::tests::insert_legacy_attached_agent(
            layout,
            WORKSPACE,
            binding.activity_id(),
            binding.agent_id(),
            binding.session_id(),
            "synthetic moving bootstrap",
            ts(3)?,
        )?;
    }
    let activity = ActivityId::from_uuid(MEMBER_SIX_ACTIVITY);
    let coordinator = AgentInstanceId::from_uuid(Uuid::from_u128(0xAC00));
    let coordinator_session = AgentSessionId::from_uuid(Uuid::from_u128(0xAC01));
    crate::tests::insert_legacy_attached_agent(
        layout,
        WORKSPACE,
        activity,
        coordinator,
        coordinator_session,
        "synthetic Source coordinator",
        ts(3)?,
    )?;
    crate::tests::insert_legacy_attached_agent(
        layout,
        WORKSPACE,
        activity,
        AgentInstanceId::from_uuid(RECIPIENT_AGENT),
        AgentSessionId::from_uuid(MEMBER_FOUR_SESSION),
        "synthetic recipient binding",
        ts(3)?,
    )?;
    Ok((activity, coordinator, coordinator_session))
}

fn create_work(
    store: &mut WorkspaceStore,
    actor: ActivityCoordinatorActor,
) -> SourceDispatchResult<forge_domain::RegisteredAgentAssignment> {
    let goal = RegisteredGoal::create(
        GoalId::from_uuid(Uuid::from_u128(0xAC03)),
        WORKSPACE,
        actor.activity_id(),
        GoalContract::new(
            GoalObjective::new("Native Home admission regression".to_owned())?,
            vec![GoalStatement::new(
                "Preserve the existing work scope".to_owned(),
            )?],
            vec![],
            vec![],
            vec![],
            vec![],
        )?,
        ts(10)?,
    );
    let goal = store.insert_goal(actor, &goal, ts(10)?)?;
    let definition = DagNodeDefinition::new(
        DagNodeId::from_uuid(Uuid::from_u128(0xAC04)),
        DagNodeOrdinal::new(0)?,
        DagNodeTitle::new("Existing work".to_owned())?,
        DagNodeObjective::new("Keep original lineage".to_owned())?,
        vec![DagAcceptanceCriterion::new(
            "Exact Source receipt".to_owned(),
        )?],
    )?;
    let revision = DagRevision::new(
        DagRevisionId::from_uuid(Uuid::from_u128(0xAC05)),
        goal.id(),
        goal.revision(),
        DagRevisionNumber::INITIAL,
        None,
        vec![definition.clone()],
        vec![],
        ts(15)?,
    )?;
    let node =
        forge_domain::RegisteredDagNode::create(goal.id(), revision.number(), definition, ts(15)?);
    let _plan = store.commit_coordinator_dag_plan(
        &AgentDagPlanRequest::new(
            AgentDagPlanRequestId::from_uuid(Uuid::from_u128(0xAC06)),
            actor,
            GoalAttribution::new(goal.id(), goal.revision()),
            None,
            None,
            AgentDagDraft::new(revision, vec![node.clone()], vec![])?,
            AgentWorkUpdateId::from_uuid(Uuid::from_u128(0xAC07)),
            AgentWorkUpdateSequence::INITIAL,
            ts(16)?,
        ),
        ts(16)?,
    )?;
    let moving = store
        .get_agent(AgentInstanceId::from_uuid(MEMBER_SIX_AGENT))?
        .ok_or("Source moving Agent missing")?;
    Ok(store.offer_agent_assignment(
        &OfferAgentAssignment::new(
            AgentAssignmentId::from_uuid(Uuid::from_u128(0xAC08)),
            actor,
            AgentAssignmentTarget::new(
                moving.id(),
                moving.session_id().ok_or("moving Session missing")?,
                moving.principal(),
                moving.revision(),
            ),
            goal.id(),
            goal.revision(),
            goal.timestamps().created_at(),
            node.dag_revision(),
            DagHeadRevision::INITIAL,
            node.id(),
            node.revision(),
            None,
            ts(20)?,
        ),
        ts(20)?,
    )?)
}
