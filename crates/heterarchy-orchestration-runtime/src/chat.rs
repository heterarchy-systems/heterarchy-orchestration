//! Protocol-neutral Workspace and node conversation operations.
//!
//! This module owns authenticated Workspace and node chat history plus exact
//! historical recovery. Provider destinations and new message delivery are
//! retired.

mod node;

pub use node::{ChatNodeTarget, ChatSourceAssignment, ChatSourceObservation};

use forge_domain::{
    ActivityId, AgentInstanceId, AgentSessionId, AuthenticatedPrincipal, TimestampMillis,
    WorkspaceId,
};
use heterarchy_orchestration_operations::{
    MessageAuthor, MessageChannel, MessageContext, MessageId, MessageReadScope,
    NodeMessageRouteWitness, OperationalActor, OperationsCatalog, OperationsError,
    ProviderMessageState, ProviderMessageTarget, WorkspaceMessage, validate_chat_text,
};
use heterarchy_orchestration_sessions::AgentProfileId;
use thiserror::Error;
use uuid::Uuid;

use crate::{OrchestrationRuntime, RuntimeError, participation::ParticipationTarget};

const CHAT_HISTORY_PAGE_SIZE: usize = 64;
const PROVIDER_CHAT_RETIRED: &str =
    "ChatGPT destinations are retired. Existing history remains available.";

#[cfg(test)]
mod auth_tests {
    use super::*;
    use crate::access::OperatorPolicy;
    use heterarchy_orchestration_operations::AppendWorkspaceMessageCommand;
    use rusqlite::{Connection, params};
    #[test]
    fn participant_cannot_use_operator_or_foreign_or_closed_session()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = std::env::temp_dir().join(format!("chat-authority-{}", Uuid::now_v7()));
        std::fs::create_dir_all(&root)?;
        let runtime = OrchestrationRuntime::open(&root)?;
        let principal = AuthenticatedPrincipal::DirectToken(
            forge_domain::DirectTokenId::from_uuid(Uuid::from_u128(10)),
        );
        let foreign = AuthenticatedPrincipal::DirectToken(forge_domain::DirectTokenId::from_uuid(
            Uuid::from_u128(11),
        ));
        let session = runtime.open_session(Uuid::now_v7(), principal, 1)?.session;
        let workspace = WorkspaceId::from_uuid(Uuid::from_u128(1));
        let activity = Uuid::from_u128(2);
        let target = ParticipationTarget {
            workspace_id: workspace.into_uuid(),
            activity_id: activity,
            session_id: session.session_id,
            agent_id: crate::leadership::member_agent_id(
                workspace.into_uuid(),
                activity,
                session.session_id,
            )
            .into_uuid(),
        };
        let query = ChatQuery {
            workspace_id: workspace,
            agent_profile_id: None,
            runtime: None,
            node_scope: None,
            before_sequence: None,
        };
        assert!(matches!(
            runtime.read_chat_operator(principal, &query, None, TimestampMillis::new(1)?),
            Err(ChatError::Runtime(RuntimeError::Forbidden))
        ));
        assert!(matches!(
            runtime.authorize_chat_sender(target, foreign, workspace),
            Err(ChatError::Runtime(RuntimeError::Forbidden))
        ));
        runtime.close_session(session.session_id, principal, session.revision, 2)?;
        assert!(matches!(
            runtime.read_chat(target, principal, &query, None, TimestampMillis::new(2)?),
            Err(ChatError::Runtime(RuntimeError::Forbidden))
        ));
        drop(runtime);
        std::fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn direct_participant_intent_uses_no_node_or_leadership_witness()
    -> Result<(), Box<dyn std::error::Error>> {
        let workspace = WorkspaceId::from_uuid(Uuid::from_u128(1));
        let activity_id = Uuid::from_u128(2);
        let agent_id = Uuid::from_u128(3);
        let session_id = Uuid::from_u128(4);
        let profile_id = AgentProfileId::from_uuid(Uuid::from_u128(5))?;
        let actor = OperationalActor::Participant {
            activity_id: ActivityId::from_uuid(activity_id),
            agent_id: AgentInstanceId::from_uuid(agent_id),
            session_id: AgentSessionId::from_uuid(session_id),
        };
        let request = ChatSend {
            query: ChatQuery {
                workspace_id: workspace,
                agent_profile_id: Some(profile_id),
                runtime: None,
                node_scope: None,
                before_sequence: None,
            },
            message_id: Uuid::from_u128(6),
            text: "direct research request".to_owned(),
            target_fence: ChatTargetFence {
                operation_id: Uuid::from_u128(7),
                profile_id: profile_id.into_uuid(),
                profile_name: "researcher".to_owned(),
                participation_id: Uuid::from_u128(8),
                participation_revision: 1,
                runtime_session_id: Uuid::from_u128(9),
                runtime_revision: 1,
                provider_conversation_id: "conversation".to_owned(),
                leadership_revision: None,
                node_target: None,
            },
        };
        let prepared = prepare_send(&request, actor, None)?;
        assert_eq!(prepared.context, MessageContext::default());
        assert!(prepared.witness.is_none());
        assert!(matches!(
            prepare_send(&request, actor, Some(1)),
            Err(ChatError::Identity)
        ));
        Ok(())
    }

    #[test]
    fn provider_targets_are_empty_while_generic_workspace_history_remains_readable()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = std::env::temp_dir().join(format!("chat-history-{}", Uuid::now_v7()));
        std::fs::create_dir_all(&root)?;
        let principal = AuthenticatedPrincipal::DirectToken(
            forge_domain::DirectTokenId::from_uuid(Uuid::from_u128(10)),
        );
        let runtime = OrchestrationRuntime::open(&root)?
            .with_operator_policy(OperatorPolicy::new(vec![principal])?);
        let workspace = WorkspaceId::from_uuid(Uuid::from_u128(12));
        let seed = Connection::open(root.join("agent-operations.sqlite3"))?;
        seed.execute(
            "INSERT INTO ops_imports(migration_id,workspace_id,destination_id,source_revision,phase,next_cursor,page_count,record_count,import_digest,verified_pages,verify_digest) VALUES(?1,?2,?3,1,2,NULL,5,0,?4,5,?4)",
            params![
                Uuid::from_u128(18).as_bytes(),
                workspace.into_uuid().as_bytes(),
                runtime.destination_id().as_bytes(),
                [0_u8; 32].as_slice()
            ],
        )?;
        drop(seed);

        let message_id = Uuid::from_u128(19);
        let mut catalog = runtime.catalog()?;
        catalog.append_workspace_message(AppendWorkspaceMessageCommand {
            message_id,
            workspace_id: workspace,
            channel: MessageChannel::Workspace,
            actor: OperationalActor::LocalOperator,
            body: "historical Workspace message".to_owned(),
            submitted_at: TimestampMillis::new(10)?,
            context: MessageContext::default(),
        })?;
        assert!(catalog.provider_message_outbox(message_id)?.is_none());
        drop(catalog);

        let query = ChatQuery {
            workspace_id: workspace,
            agent_profile_id: None,
            runtime: None,
            node_scope: None,
            before_sequence: None,
        };
        let snapshot =
            runtime.read_chat_operator(principal, &query, None, TimestampMillis::new(11)?)?;
        assert!(snapshot.targets.is_empty());
        assert!(snapshot.target.is_none());
        assert_eq!(snapshot.entries.len(), 1);
        assert_eq!(snapshot.entries[0].message.message_id(), message_id);
        assert_eq!(snapshot.entries[0].provider_state, None);
        assert!(
            snapshot
                .unavailable_reason
                .is_some_and(|reason| reason.contains("retired"))
        );

        drop(runtime);
        std::fs::remove_dir_all(root)?;
        Ok(())
    }
}

/// Validated selectors for Workspace history or exact historical recovery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChatQuery {
    /// Exact Workspace whose history or historical destination is requested.
    pub workspace_id: WorkspaceId,
    /// Optional Profile selector for a direct history conversation.
    pub agent_profile_id: Option<AgentProfileId>,
    /// Optional historical Runtime Session identity for exact recovery.
    pub runtime: Option<Uuid>,
    /// Optional exact Forge node generation and identity.
    pub node_scope: Option<heterarchy_orchestration_operations::NodeTeamBindingScope>,
    /// Exclusive history cursor; `None` starts at the newest message.
    pub before_sequence: Option<u64>,
}

/// Exact historical provider destination captured before retirement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChatTargetFence {
    /// Original provision operation identity.
    pub operation_id: Uuid,
    /// Original Profile identity.
    pub profile_id: Uuid,
    /// Display name captured with the historical route.
    pub profile_name: String,
    /// Workspace participation identity.
    pub participation_id: Uuid,
    /// Participation revision captured with the route.
    pub participation_revision: u64,
    /// Original provider Runtime Session identity.
    pub runtime_session_id: Uuid,
    /// Runtime Session revision captured with the route.
    pub runtime_revision: i64,
    /// Original provider conversation identity.
    pub provider_conversation_id: String,
    /// Leadership revision captured with the historical route.
    pub leadership_revision: Option<i64>,
    /// Exact node recipient identity when this is a node conversation.
    pub node_target: Option<ChatNodeTarget>,
}

/// One exact historical message intent retained for recovery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChatSend {
    /// Original query whose destination must match the durable receipt.
    pub query: ChatQuery,
    /// Idempotency identity for this message.
    pub message_id: Uuid,
    /// Original operator or participant text.
    pub text: String,
    /// Destination identity observed before the original submission.
    pub target_fence: ChatTargetFence,
}

/// A stored Workspace message and its original provider-delivery state, if any.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChatEntry {
    /// Durable Operations message.
    pub message: WorkspaceMessage,
    /// Provider delivery state when this message has an outbox row.
    pub provider_state: Option<ProviderMessageState>,
}

/// Bounded history and the retired provider-route projection for one query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChatSnapshot {
    /// Exact query that produced this snapshot.
    pub query: ChatQuery,
    /// Always empty after provider retirement.
    pub targets: Vec<ChatTargetFence>,
    /// Always `None` after provider retirement.
    pub target: Option<ChatTargetFence>,
    /// Bounded matching history in the canonical Operations page order.
    pub entries: Vec<ChatEntry>,
    /// Exclusive cursor for the next older history page.
    pub next_before_sequence: Option<u64>,
    /// Explanation that provider targets are unavailable while history remains readable.
    pub unavailable_reason: Option<&'static str>,
}

/// Durable receipt for an exact historical message recovery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChatReceipt {
    /// Exact message id submitted by the caller.
    pub message_id: Uuid,
    /// Historical durable provider outbox state.
    pub provider_state: ProviderMessageState,
}

/// Chat-specific failures, preserving the owner that produced each error.
#[derive(Debug, Error)]
pub enum ChatError {
    /// A typed identity or query shape is invalid.
    #[error("Chat identity is invalid")]
    Identity,
    /// The canonical catalog mutex is unavailable.
    #[error("Chat catalog is unavailable")]
    CatalogLock,
    /// The message id is already committed for a different request.
    #[error("The original Chat request does not match this retry")]
    ReplayConflict,
    /// The raw message exceeds an established text bound.
    #[error("Chat message exceeds the native text limit")]
    BodyLimit,
    /// An Operations catalog operation failed.
    #[error(transparent)]
    Operations(#[from] OperationsError),
    /// Runtime ownership or authenticated-principal checks failed.
    #[error(transparent)]
    Runtime(#[from] RuntimeError),
}

struct ResolvedChat {
    channel: Option<MessageChannel>,
    unavailable: Option<&'static str>,
}

struct PreparedSend {
    workspace_id: WorkspaceId,
    message_id: MessageId,
    context: MessageContext,
    witness: Option<NodeMessageRouteWitness>,
}

impl OrchestrationRuntime {
    /// Read Workspace or node history through the operator surface.
    ///
    /// A missing Forge observation is permitted for reads; node history remains
    /// available while the retired provider destination is reported unavailable.
    ///
    /// # Errors
    /// Returns operator, Operations, or poisoned-owner failures.
    pub fn read_chat_operator(
        &self,
        principal: AuthenticatedPrincipal,
        query: &ChatQuery,
        observation: Option<&ChatSourceObservation>,
        _now: TimestampMillis,
    ) -> Result<ChatSnapshot, ChatError> {
        self.require_operator(principal)?;
        let workspace_id = validate_query(query)?;
        let catalog = catalog_guard(self.catalog())?;
        let resolved = resolve_for_read(&catalog, workspace_id, query, observation)?;
        snapshot_from_resolved(&catalog, workspace_id, query, &resolved)
    }

    /// Read Workspace or node history as an authenticated participant.
    ///
    /// The sender must own the open Session and Workspace and retain its exact
    /// Ready Activity/Agent binding. Its Activity may differ from a node's.
    ///
    /// # Errors
    /// Returns participation, readiness, Operations, or owner failures.
    pub fn read_chat(
        &self,
        target: ParticipationTarget,
        principal: AuthenticatedPrincipal,
        query: &ChatQuery,
        observation: Option<&ChatSourceObservation>,
        _now: TimestampMillis,
    ) -> Result<ChatSnapshot, ChatError> {
        let workspace_id = validate_query(query)?;
        validate_participation_target(target)?;
        if workspace_id.into_uuid() != target.workspace_id {
            return Err(ChatError::Identity);
        }
        self.require_participation(target, principal)?;
        let catalog = catalog_guard(self.catalog())?;
        require_ready_sender_binding(&catalog, target)?;
        let resolved = resolve_for_read(&catalog, workspace_id, query, observation)?;
        snapshot_from_resolved(&catalog, workspace_id, query, &resolved)
    }

    /// Authorize a participant before a caller performs an external Forge read.
    ///
    /// Read operations recheck this authority under the same catalog guard used
    /// for their projection, so this early check only protects source-call order.
    ///
    /// # Errors
    /// Rejects a foreign/closed Session, inaccessible Workspace, non-Ready
    /// Activity binding, or canonical catalog failure.
    pub fn authorize_chat_sender(
        &self,
        target: ParticipationTarget,
        principal: AuthenticatedPrincipal,
        workspace: WorkspaceId,
    ) -> Result<(), ChatError> {
        validate_participation_target(target)?;
        if workspace.into_uuid().is_nil() || target.workspace_id != workspace.into_uuid() {
            return Err(ChatError::Identity);
        }
        self.require_participation(target, principal)?;
        let catalog = catalog_guard(self.catalog())?;
        require_ready_sender_binding(&catalog, target)
    }

    /// Recover one exact operator message receipt without resolving current routes.
    ///
    /// # Errors
    /// Returns invalid replay identities, Operations failures, or owner failures.
    pub fn recover_chat_operator(
        &self,
        principal: AuthenticatedPrincipal,
        request: &ChatSend,
    ) -> Result<Option<ChatReceipt>, ChatError> {
        self.require_operator(principal)?;
        let actor = OperationalActor::LocalOperator;
        let prepared = prepare_send(request, actor, None)?;
        let catalog = catalog_guard(self.catalog())?;
        recover_in_catalog(&catalog, request, actor, &prepared)
    }

    /// Recover one exact participant message receipt without requiring a current
    /// Ready binding, leadership revision, or Forge node observation.
    ///
    /// # Errors
    /// Returns Session/Workspace authorization, invalid replay, Operations, or
    /// owner failures.
    pub fn recover_chat(
        &self,
        target: ParticipationTarget,
        principal: AuthenticatedPrincipal,
        request: &ChatSend,
        sender_leadership_revision: i64,
    ) -> Result<Option<ChatReceipt>, ChatError> {
        let workspace_id = validate_query(&request.query)?;
        validate_participation_target(target)?;
        if workspace_id.into_uuid() != target.workspace_id || sender_leadership_revision <= 0 {
            return Err(ChatError::Identity);
        }
        self.require_participation(target, principal)?;
        let actor = participant_actor(target);
        let prepared = prepare_send(request, actor, Some(sender_leadership_revision))?;
        let catalog = catalog_guard(self.catalog())?;
        recover_in_catalog(&catalog, request, actor, &prepared)
    }

    /// Recover one exact Profile-direct participant message without resolving
    /// the current provider route or requiring a leadership fence.
    ///
    /// # Errors
    /// Returns Session/Workspace authorization, immutable-intent conflict,
    /// Operations failure or owner failure.
    pub fn recover_workspace_chat(
        &self,
        target: ParticipationTarget,
        principal: AuthenticatedPrincipal,
        request: &ChatSend,
    ) -> Result<Option<ChatReceipt>, ChatError> {
        require_direct_query(&request.query)?;
        validate_participation_target(target)?;
        if request.query.workspace_id.into_uuid() != target.workspace_id {
            return Err(ChatError::Identity);
        }
        self.require_participation(target, principal)?;
        let actor = participant_actor(target);
        let prepared = prepare_send(request, actor, None)?;
        let catalog = catalog_guard(self.catalog())?;
        recover_in_catalog(&catalog, request, actor, &prepared)
    }
}

fn validate_query(query: &ChatQuery) -> Result<WorkspaceId, ChatError> {
    let workspace_id = query.workspace_id;
    if workspace_id.into_uuid().is_nil()
        || query.runtime.is_some_and(|runtime| runtime.is_nil())
        || query
            .node_scope
            .is_some_and(|scope| scope.workspace_id() != workspace_id)
        || (query.node_scope.is_some() && query.agent_profile_id.is_some())
    {
        return Err(ChatError::Identity);
    }
    Ok(workspace_id)
}

fn require_direct_query(query: &ChatQuery) -> Result<(), ChatError> {
    let _workspace = validate_query(query)?;
    if query.agent_profile_id.is_none() || query.node_scope.is_some() {
        return Err(ChatError::Identity);
    }
    Ok(())
}

fn validate_participation_target(target: ParticipationTarget) -> Result<(), ChatError> {
    if [
        target.workspace_id,
        target.activity_id,
        target.agent_id,
        target.session_id,
    ]
    .iter()
    .any(Uuid::is_nil)
    {
        return Err(ChatError::Identity);
    }
    Ok(())
}

fn participant_actor(target: ParticipationTarget) -> OperationalActor {
    OperationalActor::Participant {
        activity_id: ActivityId::from_uuid(target.activity_id),
        agent_id: AgentInstanceId::from_uuid(target.agent_id),
        session_id: AgentSessionId::from_uuid(target.session_id),
    }
}

fn require_ready_sender_binding(
    catalog: &OperationsCatalog,
    target: ParticipationTarget,
) -> Result<(), ChatError> {
    let workspace_id = WorkspaceId::from_uuid(target.workspace_id);
    let session_id = AgentSessionId::from_uuid(target.session_id);
    let identity = catalog
        .logical_identity_for_session(workspace_id, session_id)?
        .ok_or(RuntimeError::Forbidden)?;
    let derived_agent = crate::leadership::member_agent_id(
        target.workspace_id,
        target.activity_id,
        target.session_id,
    );
    if derived_agent.into_uuid() != target.agent_id
        || !identity.effect_bindings().iter().any(|binding| {
            binding.activity_id().into_uuid() == target.activity_id
                && binding.agent_id().into_uuid() == target.agent_id
        })
    {
        return Err(RuntimeError::Forbidden.into());
    }
    Ok(())
}

fn prepare_send(
    request: &ChatSend,
    actor: OperationalActor,
    sender_leadership_revision: Option<i64>,
) -> Result<PreparedSend, ChatError> {
    let workspace_id = validate_query(&request.query)?;
    if request.message_id.is_nil()
        || request.query.before_sequence.is_some()
        || request.query.node_scope.is_some() != request.target_fence.node_target.is_some()
        || (request.query.node_scope.is_none()
            && request.target_fence.leadership_revision.is_some())
    {
        return Err(ChatError::Identity);
    }
    validate_fence(&request.target_fence)?;
    if let OperationalActor::Participant { .. } = actor {
        match (
            request.query.node_scope.is_some(),
            sender_leadership_revision,
        ) {
            (true, Some(revision)) if revision > 0 => {}
            (false, None) => {}
            _ => return Err(ChatError::Identity),
        }
    }
    let context = request
        .query
        .node_scope
        .map(|scope| {
            MessageContext::new(
                Some(scope.activity_id()),
                Some(scope.goal_id()),
                Some(scope.goal_created_at()),
                Some(scope.dag_revision()),
                Some(scope.dag_base_goal_revision()),
                Some(scope.node_id()),
            )
        })
        .transpose()?
        .unwrap_or_default();
    let witness = request
        .target_fence
        .node_target
        .as_ref()
        .map(|node_target| {
            node_target.node_witness(&request.target_fence, actor, sender_leadership_revision)
        })
        .transpose()?
        .flatten();
    match validate_chat_text(&request.text) {
        Ok(()) => {}
        Err(OperationsError::Bounds(_)) => return Err(ChatError::BodyLimit),
        Err(error) => return Err(ChatError::Operations(error)),
    }
    Ok(PreparedSend {
        workspace_id,
        message_id: request.message_id,
        context,
        witness,
    })
}

fn validate_fence(fence: &ChatTargetFence) -> Result<(), ChatError> {
    if [
        fence.operation_id,
        fence.profile_id,
        fence.participation_id,
        fence.runtime_session_id,
    ]
    .iter()
    .any(Uuid::is_nil)
        || fence.participation_revision == 0
        || fence.runtime_revision <= 0
        || fence.profile_name.trim().is_empty()
        || fence.provider_conversation_id.trim().is_empty()
        || fence
            .leadership_revision
            .is_some_and(|revision| revision <= 0)
    {
        return Err(ChatError::Identity);
    }
    if let Some(node) = &fence.node_target {
        match node {
            ChatNodeTarget::Agent {
                assignment_id,
                assignment_revision,
                agent_id,
                session_id,
            } if assignment_id.is_nil()
                || *assignment_revision <= 0
                || agent_id.is_nil()
                || session_id.is_nil()
                || fence.leadership_revision.is_some() =>
            {
                return Err(ChatError::Identity);
            }
            ChatNodeTarget::Team { team_id, .. }
                if team_id.is_nil() || fence.leadership_revision.is_none() =>
            {
                return Err(ChatError::Identity);
            }
            _ => {}
        }
    }
    Ok(())
}

fn recover_in_catalog(
    catalog: &OperationsCatalog,
    request: &ChatSend,
    actor: OperationalActor,
    prepared: &PreparedSend,
) -> Result<Option<ChatReceipt>, ChatError> {
    let Some(message) = catalog.workspace_message(prepared.message_id)? else {
        return Ok(None);
    };
    let outbox = catalog
        .provider_message_outbox(prepared.message_id)?
        .ok_or(ChatError::ReplayConflict)?;
    let same_actor = match (actor, message.author()) {
        (OperationalActor::LocalOperator, MessageAuthor::Operator) => true,
        (
            OperationalActor::Participant { session_id, .. },
            MessageAuthor::Participant {
                session_id: original,
                ..
            },
        )
        | (
            OperationalActor::Participant { session_id, .. },
            MessageAuthor::NativeParticipant {
                session_id: original,
                ..
            },
        ) => session_id == *original,
        _ => false,
    };
    if message.workspace_id() != prepared.workspace_id
        || outbox.target().workspace_id() != prepared.workspace_id
        || message.body() != request.text
        || !same_actor
        || message.context() != prepared.context
        || outbox.node_route_witness() != prepared.witness.as_ref()
        || request.query.node_scope.is_some() != request.target_fence.node_target.is_some()
        || !target_matches_fence(outbox.target(), &request.target_fence)
        || !channel_matches_query(message.channel(), &request.query, &request.target_fence)
        || request
            .query
            .runtime
            .is_some_and(|runtime| runtime != request.target_fence.runtime_session_id)
    {
        return Err(ChatError::ReplayConflict);
    }
    Ok(Some(ChatReceipt {
        message_id: prepared.message_id,
        provider_state: outbox.state(),
    }))
}

fn resolve_for_read(
    catalog: &OperationsCatalog,
    workspace_id: WorkspaceId,
    query: &ChatQuery,
    observation: Option<&ChatSourceObservation>,
) -> Result<ResolvedChat, ChatError> {
    if query.node_scope.is_some() {
        return node::resolve_node_chat(query, observation);
    }
    if let Some(profile_id) = query.agent_profile_id {
        let Some(participation) = catalog.workspace_participation(workspace_id, profile_id)? else {
            return Ok(ResolvedChat {
                channel: None,
                unavailable: Some("This agent is not a participant in this Workspace."),
            });
        };
        return Ok(ResolvedChat {
            channel: Some(MessageChannel::Direct(participation.participation_id())),
            unavailable: Some(PROVIDER_CHAT_RETIRED),
        });
    }
    Ok(ResolvedChat {
        channel: Some(MessageChannel::Workspace),
        unavailable: Some(PROVIDER_CHAT_RETIRED),
    })
}

fn snapshot_from_resolved(
    catalog: &OperationsCatalog,
    workspace_id: WorkspaceId,
    query: &ChatQuery,
    resolved: &ResolvedChat,
) -> Result<ChatSnapshot, ChatError> {
    let scope = match query.node_scope {
        Some(scope) => Some(MessageReadScope::Node(MessageContext::new(
            Some(scope.activity_id()),
            Some(scope.goal_id()),
            Some(scope.goal_created_at()),
            Some(scope.dag_revision()),
            Some(scope.dag_base_goal_revision()),
            Some(scope.node_id()),
        )?)),
        None => resolved.channel.map(MessageReadScope::Channel),
    };
    let page = scope
        .map(|scope| {
            catalog.workspace_messages_latest(
                workspace_id,
                scope,
                query.before_sequence,
                CHAT_HISTORY_PAGE_SIZE,
            )
        })
        .transpose()?;
    let entries = page
        .as_ref()
        .map(|page| {
            page.messages()
                .iter()
                .map(|message| message_entry(catalog, message))
                .collect()
        })
        .transpose()?
        .unwrap_or_default();
    Ok(ChatSnapshot {
        query: query.clone(),
        targets: Vec::new(),
        target: None,
        entries,
        next_before_sequence: page
            .as_ref()
            .and_then(heterarchy_orchestration_operations::MessagePage::next_cursor),
        unavailable_reason: resolved.unavailable,
    })
}

fn message_entry(
    catalog: &OperationsCatalog,
    message: &WorkspaceMessage,
) -> Result<ChatEntry, ChatError> {
    let outbox = catalog.provider_message_outbox(message.message_id())?;
    Ok(ChatEntry {
        message: message.clone(),
        provider_state: outbox
            .as_ref()
            .map(heterarchy_orchestration_operations::ProviderMessageOutbox::state),
    })
}

fn target_matches_fence(target: &ProviderMessageTarget, fence: &ChatTargetFence) -> bool {
    target.profile_id().into_uuid() == fence.profile_id
        && target.participation_id().into_uuid() == fence.participation_id
        && target.runtime_session_id().into_uuid() == fence.runtime_session_id
        && target.provider_conversation_id() == fence.provider_conversation_id
}

fn channel_matches_query(
    channel: MessageChannel,
    query: &ChatQuery,
    fence: &ChatTargetFence,
) -> bool {
    if query.node_scope.is_some() {
        return query.agent_profile_id.is_none()
            && matches!(channel, MessageChannel::Direct(participation) if participation.into_uuid() == fence.participation_id);
    }
    match (channel, query.agent_profile_id) {
        (MessageChannel::Workspace, None) => true,
        (MessageChannel::Direct(participation), Some(profile)) => {
            participation.into_uuid() == fence.participation_id
                && profile.into_uuid() == fence.profile_id
        }
        _ => false,
    }
}

fn catalog_guard<T>(result: Result<T, RuntimeError>) -> Result<T, ChatError> {
    result.map_err(map_catalog_guard_error)
}

fn map_catalog_guard_error(error: RuntimeError) -> ChatError {
    match error {
        RuntimeError::CatalogOwnerPoisoned => ChatError::CatalogLock,
        other => ChatError::Runtime(other),
    }
}
