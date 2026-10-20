//! Protocol-neutral Workspace and node conversation operations.
//!
//! This module owns chat route resolution and durable message admission over
//! Orchestration's canonical catalogs. It never dispatches to a provider.

mod node;

pub use node::{ChatNodeTarget, ChatSourceAssignment, ChatSourceObservation};

use forge_domain::{
    ActivityId, AgentInstanceId, AgentSessionId, AuthenticatedPrincipal, TimestampMillis,
    WorkspaceId,
};
use heterarchy_orchestration_operations::{
    AppendWorkspaceMessageCommand, LeadershipScope, MessageAuthor, MessageChannel, MessageContext,
    MessageId, MessageReadScope, NodeMessageRouteWitness, OperationalActor, OperationsCatalog,
    OperationsError, ProviderMessageState, ProviderMessageTarget, WorkspaceMessage,
    WorkspaceProjectReference, validate_chat_text,
};
use heterarchy_orchestration_sessions::{
    AgentProfileCatalog, AgentProfileError, AgentProfileId, AgentProfileLifecycleState,
    AgentProfileProviderFamily, AgentProfileRuntimeSessionState,
};
use thiserror::Error;
use uuid::Uuid;

use crate::{OrchestrationRuntime, RuntimeError, participation::ParticipationTarget};

const CHAT_HISTORY_PAGE_SIZE: usize = 64;
const CHAT_TARGET_LIMIT: usize = 8;

#[cfg(test)]
mod auth_tests {
    use super::*;
    use crate::{
        access::OperatorPolicy,
        provider_chat::{ProviderIncomingCommand, ProviderIncomingRole},
        provisioning::AgentRequestSubmit,
    };
    use heterarchy_orchestration_operations::{AgentRequestEffectScope, AgentRequestIndependence};
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
                project: None,
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
    #[allow(
        clippy::too_many_lines,
        reason = "The composed fixture keeps provisioning, direct delivery, incoming turn and restart proof in one lifecycle."
    )]
    fn direct_participant_send_and_recovery_preserve_empty_node_context()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = std::env::temp_dir().join(format!("direct-chat-{}", Uuid::now_v7()));
        std::fs::create_dir_all(&root)?;
        let principal = AuthenticatedPrincipal::DirectToken(
            forge_domain::DirectTokenId::from_uuid(Uuid::from_u128(10)),
        );
        let runtime = OrchestrationRuntime::open(&root)?
            .with_operator_policy(OperatorPolicy::new(vec![principal])?);
        let session = runtime
            .open_session(Uuid::from_u128(11), principal, 1)?
            .session;
        let workspace = WorkspaceId::from_uuid(Uuid::from_u128(12));
        crate::test_support::ensure_workspace_project_binding(&runtime, workspace.into_uuid(), 1)?;
        let activity_id = Uuid::from_u128(13);
        let request_id = Uuid::from_u128(14);
        runtime.submit_agent_request(AgentRequestSubmit {
            workspace_id: workspace.into_uuid(),
            activity_id,
            requester_agent_id: Uuid::from_u128(15),
            requester_session_id: session.session_id,
            coordinator_revision: 1,
            request_id,
            objective: "direct chat fixture".to_owned(),
            capability: "chat".to_owned(),
            count: 1,
            effect_scope: AgentRequestEffectScope::ReadOnly,
            independence: AgentRequestIndependence::None,
            reason: "verify direct route".to_owned(),
            created_at_ms: 2,
        })?;
        runtime.reserve_agent_request_resolution(request_id, 3, 4)?;
        runtime.prepare_agent_request_effect_targets(request_id, 5, 6)?;
        let action = runtime
            .provider_worker_next_provision(principal, 7)?
            .ok_or("provider fixture dispatch")?;
        let operation = runtime.provider_worker_confirm_provision_with_project(
            principal,
            action.operation().operation_id(),
            action.operation().revision(),
            "conversation-direct",
            Some("g-p-6ad231f7ad7c8191bfc1d5ff782c171f"),
            8,
        )?;
        let operation_id = operation.operation_id();
        let profile_id = operation.profile_id().ok_or("provider Profile")?;
        let runtime_id = operation.runtime_session_id().ok_or("provider Runtime")?;
        let _link = runtime.bind_profile_session(
            operation_id,
            profile_id.into_uuid(),
            session.session_id,
            principal,
            9,
        )?;
        let participant_id = Uuid::from_u128(16);
        let participation_id = Uuid::from_u128(17);
        let sender_agent = crate::leadership::member_agent_id(
            workspace.into_uuid(),
            activity_id,
            session.session_id,
        );
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
        seed.execute(
            "INSERT INTO ops_workspace_participations(workspace_id,agent_profile_id,participation_id,created_at_ms,revision) VALUES(?1,?2,?3,10,1)",
            params![
                workspace.into_uuid().as_bytes(),
                profile_id.into_uuid().as_bytes(),
                participation_id.as_bytes()
            ],
        )?;
        seed.execute(
            "INSERT INTO ops_logical_members(workspace_id,participant_id,session_id,principal_kind,principal_id,display_name,declared_role,transport_kind,membership_revision,created_at_ms) VALUES(?1,?2,?3,1,?4,'sender','researcher',2,1,10)",
            params![
                workspace.into_uuid().as_bytes(),
                participant_id.as_bytes(),
                session.session_id.as_bytes(),
                Uuid::from_u128(10).as_bytes()
            ],
        )?;
        seed.execute(
            "INSERT INTO ops_workspace_participation_mappings(request_id,workspace_id,participation_id,agent_profile_id,profile_revision,profile_mapping_request_id,session_id,session_revision,participant_id,participant_membership_revision,mapped_at_ms) VALUES(?1,?2,?3,?4,1,?1,?5,1,?6,1,10)",
            params![
                operation_id.as_bytes(),
                workspace.into_uuid().as_bytes(),
                participation_id.as_bytes(),
                profile_id.into_uuid().as_bytes(),
                session.session_id.as_bytes(),
                participant_id.as_bytes()
            ],
        )?;
        seed.execute(
            "INSERT INTO ops_current_effect_bindings(workspace_id,participant_id,activity_id,session_id,agent_id,source_kind,source_request_id) VALUES(?1,?2,?3,?4,?5,0,?6)",
            params![
                workspace.into_uuid().as_bytes(),
                participant_id.as_bytes(),
                activity_id.as_bytes(),
                session.session_id.as_bytes(),
                sender_agent.into_uuid().as_bytes(),
                operation_id.as_bytes()
            ],
        )?;
        drop(seed);

        let caller = ParticipationTarget {
            workspace_id: workspace.into_uuid(),
            activity_id,
            agent_id: sender_agent.into_uuid(),
            session_id: session.session_id,
        };
        let query = ChatQuery {
            workspace_id: workspace,
            agent_profile_id: Some(profile_id),
            runtime: Some(runtime_id.into_uuid()),
            node_scope: None,
            before_sequence: None,
        };
        let snapshot =
            runtime.read_chat(caller, principal, &query, None, TimestampMillis::new(11)?)?;
        let target_fence = snapshot.target.ok_or("direct target")?;
        assert_eq!(
            target_fence
                .project
                .map(|project| project.project_id().to_string())
                .as_deref(),
            Some("g-p-6ad231f7ad7c8191bfc1d5ff782c171f")
        );
        let request = ChatSend {
            query,
            message_id: Uuid::from_u128(19),
            text: "collect official evidence".to_owned(),
            target_fence,
        };
        let sent =
            runtime.send_workspace_chat(caller, principal, &request, TimestampMillis::new(12)?)?;
        assert_eq!(sent.message_id, request.message_id);
        assert_eq!(
            runtime.recover_workspace_chat(caller, principal, &request)?,
            Some(sent.clone())
        );
        let dispatch = runtime
            .provider_worker_next_message(principal, 13)?
            .ok_or("direct provider dispatch")?;
        let frozen_body = dispatch.body.ok_or("direct provider body")?;
        let delivered = runtime.provider_worker_confirm_message_delivery_with_project(
            principal,
            request.message_id,
            dispatch.outbox.revision(),
            "conversation-direct",
            Some("g-p-6ad231f7ad7c8191bfc1d5ff782c171f"),
            "user-turn".to_owned(),
            frozen_body,
            14,
        )?;
        assert_eq!(delivered.state(), ProviderMessageState::Delivered);
        let incoming = runtime.provider_worker_record_incoming(
            principal,
            ProviderIncomingCommand {
                reply_to_message_id: request.message_id,
                provider_conversation_id: "conversation-direct".to_owned(),
                provider_turn_id: "assistant-turn".to_owned(),
                role: ProviderIncomingRole::Assistant,
                body: "official evidence".to_owned(),
            },
            15,
        )?;
        assert_eq!(incoming.message().context(), MessageContext::default());
        assert!(matches!(
            incoming.message().channel(),
            MessageChannel::Direct(id) if id.into_uuid() == participation_id
        ));
        let catalog = runtime.catalog()?;
        let message = catalog
            .workspace_message(request.message_id)?
            .ok_or("direct message")?;
        let outbox = catalog
            .provider_message_outbox(request.message_id)?
            .ok_or("direct outbox")?;
        assert_eq!(message.context(), MessageContext::default());
        assert!(outbox.node_route_witness().is_none());
        drop(catalog);
        drop(runtime);
        let reopened = OrchestrationRuntime::open(&root)?
            .with_operator_policy(OperatorPolicy::new(vec![principal])?);
        let recovered = reopened
            .recover_workspace_chat(caller, principal, &request)?
            .ok_or("restart direct recovery")?;
        assert_eq!(recovered.message_id, sent.message_id);
        assert_eq!(recovered.provider_state, ProviderMessageState::Delivered);
        drop(reopened);
        std::fs::remove_dir_all(root)?;
        Ok(())
    }
}

/// Validated selectors for one Workspace chat read or send.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChatQuery {
    /// Exact Workspace whose history or destination is requested.
    pub workspace_id: WorkspaceId,
    /// Optional Profile selector for a direct conversation.
    pub agent_profile_id: Option<AgentProfileId>,
    /// Optional exact provider Runtime Session selector.
    pub runtime: Option<Uuid>,
    /// Optional exact Forge node generation and identity.
    pub node_scope: Option<heterarchy_orchestration_operations::NodeTeamBindingScope>,
    /// Exclusive history cursor; `None` starts at the newest message.
    pub before_sequence: Option<u64>,
}

/// Exact current provider destination captured for the caller.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChatTargetFence {
    /// Provisioning operation identity for this provider route.
    pub operation_id: Uuid,
    /// Selected Profile identity.
    pub profile_id: Uuid,
    /// Display name captured with the route.
    pub profile_name: String,
    /// Workspace participation identity.
    pub participation_id: Uuid,
    /// Participation revision captured with the route.
    pub participation_revision: u64,
    /// Provider Runtime Session identity.
    pub runtime_session_id: Uuid,
    /// Runtime Session revision captured with the route.
    pub runtime_revision: i64,
    /// Provider conversation identity.
    pub provider_conversation_id: String,
    /// Frozen Workspace Project identity and binding revision, when present.
    pub project: Option<WorkspaceProjectReference>,
    /// Current target leadership revision, when routing through a leader.
    pub leadership_revision: Option<i64>,
    /// Exact node recipient identity when this is a node conversation.
    pub node_target: Option<ChatNodeTarget>,
}

/// One exact durable message admission request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChatSend {
    /// Query whose selected destination must still match the fence.
    pub query: ChatQuery,
    /// Idempotency identity for this message.
    pub message_id: Uuid,
    /// Raw operator or participant text.
    pub text: String,
    /// Destination identity observed before submission.
    pub target_fence: ChatTargetFence,
}

/// A stored Workspace message and any matching provider outbox state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChatEntry {
    /// Durable Operations message.
    pub message: WorkspaceMessage,
    /// Provider delivery state when this message has an outbox row.
    pub provider_state: Option<ProviderMessageState>,
}

/// Bounded history and current route projection for one query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChatSnapshot {
    /// Exact query that produced this snapshot.
    pub query: ChatQuery,
    /// All currently selectable provider destinations.
    pub targets: Vec<ChatTargetFence>,
    /// Selected destination when selection is unambiguous.
    pub target: Option<ChatTargetFence>,
    /// Bounded matching history in the canonical Operations page order.
    pub entries: Vec<ChatEntry>,
    /// Exclusive cursor for the next older history page.
    pub next_before_sequence: Option<u64>,
    /// Existing user-facing explanation when no route is available.
    pub unavailable_reason: Option<&'static str>,
}

/// Durable receipt for an admitted or exactly recovered message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChatReceipt {
    /// Exact message id submitted by the caller.
    pub message_id: Uuid,
    /// Current durable provider outbox state.
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
    /// The previously observed destination is no longer current.
    #[error("Chat target changed; refresh the destination before sending")]
    TargetChanged,
    /// The message id is already committed for a different request.
    #[error("The original Chat request does not match this retry")]
    ReplayConflict,
    /// The raw message exceeds an established text bound.
    #[error("Chat message exceeds the native text limit")]
    BodyLimit,
    /// The atomic append-and-queue operation definitely rejected admission.
    #[error("Chat admission was rejected: {0}")]
    AdmissionRejected(#[source] OperationsError),
    /// An Operations catalog operation failed outside definite admission.
    #[error(transparent)]
    Operations(#[from] OperationsError),
    /// An Agent Profile catalog operation failed.
    #[error(transparent)]
    Profile(#[from] AgentProfileError),
    /// Runtime ownership or authenticated-principal checks failed.
    #[error(transparent)]
    Runtime(#[from] RuntimeError),
}

struct ResolvedChat {
    channel: Option<MessageChannel>,
    targets: Vec<(ChatTargetFence, ProviderMessageTarget)>,
    unavailable: Option<&'static str>,
}

struct PreparedSend {
    workspace_id: WorkspaceId,
    message_id: MessageId,
    context: MessageContext,
    witness: Option<NodeMessageRouteWitness>,
}

impl OrchestrationRuntime {
    /// Read history and provider destinations through the operator surface.
    ///
    /// A missing Forge observation is permitted for reads; node history remains
    /// available while its current destination is reported as unavailable.
    ///
    /// # Errors
    /// Returns operator, Operations, Profile, or poisoned-owner failures.
    pub fn read_chat_operator(
        &self,
        principal: AuthenticatedPrincipal,
        query: &ChatQuery,
        observation: Option<&ChatSourceObservation>,
        now: TimestampMillis,
    ) -> Result<ChatSnapshot, ChatError> {
        self.require_operator(principal)?;
        let workspace_id = validate_query(query)?;
        let catalog = catalog_guard(self.catalog())?;
        let resolved = resolve_for_read(
            &catalog,
            self.profile_catalog_guard(),
            workspace_id,
            query,
            observation,
            now,
        )?;
        snapshot_from_resolved(&catalog, workspace_id, query, resolved)
    }

    /// Read history and provider destinations as an authenticated participant.
    ///
    /// The sender must own the open Session and Workspace and retain its exact
    /// Ready Activity/Agent binding. Its Activity may differ from a node's.
    ///
    /// # Errors
    /// Returns participation, readiness, Operations, Profile, or owner failures.
    pub fn read_chat(
        &self,
        target: ParticipationTarget,
        principal: AuthenticatedPrincipal,
        query: &ChatQuery,
        observation: Option<&ChatSourceObservation>,
        now: TimestampMillis,
    ) -> Result<ChatSnapshot, ChatError> {
        let workspace_id = validate_query(query)?;
        validate_participation_target(target)?;
        if workspace_id.into_uuid() != target.workspace_id {
            return Err(ChatError::Identity);
        }
        self.require_participation(target, principal)?;
        let catalog = catalog_guard(self.catalog())?;
        require_ready_sender_binding(&catalog, target)?;
        let resolved = resolve_for_read(
            &catalog,
            self.profile_catalog_guard(),
            workspace_id,
            query,
            observation,
            now,
        )?;
        snapshot_from_resolved(&catalog, workspace_id, query, resolved)
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

    /// Persist and queue one operator message against its frozen target fence.
    ///
    /// Exact receipts are recovered before mutable Profile, route, or Source
    /// observations are consulted. Provider execution remains a separate owner.
    ///
    /// # Errors
    /// Distinguishes definite admission rejection from uncertain catalog outcomes.
    pub fn send_chat_operator(
        &self,
        principal: AuthenticatedPrincipal,
        request: &ChatSend,
        observation: Option<&ChatSourceObservation>,
        now: TimestampMillis,
    ) -> Result<ChatReceipt, ChatError> {
        self.require_operator(principal)?;
        let actor = OperationalActor::LocalOperator;
        let prepared = prepare_send(request, actor, None)?;
        let mut catalog = catalog_guard(self.catalog())?;
        if let Some(receipt) = recover_in_catalog(&catalog, request, actor, &prepared)? {
            return Ok(receipt);
        }
        self.send_prepared(&mut catalog, request, observation, actor, prepared, now)
    }

    /// Persist and queue one participant message against its frozen target fence.
    ///
    /// The participant must own the open Session and Workspace. A new message
    /// also requires the exact current Ready sender binding. Receipt recovery
    /// retains the original witness even if current leadership or Source changed.
    ///
    /// # Errors
    /// Distinguishes definite admission rejection from uncertain catalog outcomes.
    pub fn send_chat(
        &self,
        target: ParticipationTarget,
        principal: AuthenticatedPrincipal,
        request: &ChatSend,
        observation: Option<&ChatSourceObservation>,
        sender_leadership_revision: i64,
        now: TimestampMillis,
    ) -> Result<ChatReceipt, ChatError> {
        let workspace_id = validate_query(&request.query)?;
        validate_participation_target(target)?;
        if workspace_id.into_uuid() != target.workspace_id || sender_leadership_revision <= 0 {
            return Err(ChatError::Identity);
        }
        self.require_participation(target, principal)?;
        let actor = participant_actor(target);
        let prepared = prepare_send(request, actor, Some(sender_leadership_revision))?;
        let mut catalog = catalog_guard(self.catalog())?;
        if let Some(receipt) = recover_in_catalog(&catalog, request, actor, &prepared)? {
            return Ok(receipt);
        }
        require_ready_sender_binding(&catalog, target)?;
        self.send_prepared(&mut catalog, request, observation, actor, prepared, now)
    }

    /// Persist and queue one Profile-direct participant message.
    ///
    /// Direct Workspace chat has no Node or leadership fence. The participant
    /// still owns an open Session and exact current Ready sender binding.
    ///
    /// # Errors
    /// Distinguishes invalid direct intent, authorization failure, route change,
    /// definite admission rejection and uncertain catalog failure.
    pub fn send_workspace_chat(
        &self,
        target: ParticipationTarget,
        principal: AuthenticatedPrincipal,
        request: &ChatSend,
        now: TimestampMillis,
    ) -> Result<ChatReceipt, ChatError> {
        require_direct_query(&request.query)?;
        validate_participation_target(target)?;
        if request.query.workspace_id.into_uuid() != target.workspace_id {
            return Err(ChatError::Identity);
        }
        self.require_participation(target, principal)?;
        let actor = participant_actor(target);
        let prepared = prepare_send(request, actor, None)?;
        let mut catalog = catalog_guard(self.catalog())?;
        if let Some(receipt) = recover_in_catalog(&catalog, request, actor, &prepared)? {
            return Ok(receipt);
        }
        require_ready_sender_binding(&catalog, target)?;
        self.send_prepared(&mut catalog, request, None, actor, prepared, now)
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

    /// Revalidate a frozen provider target against current Workspace/Profile routes.
    ///
    /// # Errors
    /// Returns catalog failures without turning uncertainty into a stale `false`.
    pub fn validate_provider_target(
        &self,
        target: &ProviderMessageTarget,
        now: TimestampMillis,
    ) -> Result<bool, ChatError> {
        let query = ChatQuery {
            workspace_id: target.workspace_id(),
            agent_profile_id: Some(target.profile_id()),
            runtime: Some(target.runtime_session_id().into_uuid()),
            node_scope: None,
            before_sequence: None,
        };
        let catalog = catalog_guard(self.catalog())?;
        let profiles = profile_catalog_guard(self.profile_catalog_guard())?;
        let resolved = resolve_chat(&catalog, &profiles, target.workspace_id(), &query, now)?;
        Ok(resolved
            .selected(&query)
            .is_some_and(|(_, current)| current == target))
    }

    /// Resolve one unambiguous live provider destination for a governance recipient.
    ///
    /// # Errors
    /// Returns invalid identities and Operations, Profile, or owner failures.
    pub fn governance_chat_target(
        &self,
        workspace: WorkspaceId,
        profile: Uuid,
        now: TimestampMillis,
    ) -> Result<Option<ProviderMessageTarget>, ChatError> {
        if workspace.into_uuid().is_nil() || profile.is_nil() {
            return Err(ChatError::Identity);
        }
        let profile_id = AgentProfileId::from_uuid(profile)?;
        let query = ChatQuery {
            workspace_id: workspace,
            agent_profile_id: Some(profile_id),
            runtime: None,
            node_scope: None,
            before_sequence: None,
        };
        let catalog = catalog_guard(self.catalog())?;
        let profiles = profile_catalog_guard(self.profile_catalog_guard())?;
        let resolved = resolve_chat(&catalog, &profiles, workspace, &query, now)?;
        Ok(resolved.selected(&query).map(|(_, target)| target.clone()))
    }

    fn send_prepared(
        &self,
        catalog: &mut OperationsCatalog,
        request: &ChatSend,
        observation: Option<&ChatSourceObservation>,
        actor: OperationalActor,
        prepared: PreparedSend,
        now: TimestampMillis,
    ) -> Result<ChatReceipt, ChatError> {
        let profiles = profile_catalog_guard(self.profile_catalog_guard())?;
        let resolved = match request.query.node_scope {
            Some(_) => node::resolve_node_chat(
                catalog,
                &profiles,
                prepared.workspace_id,
                &request.query,
                observation,
                now,
            )?,
            None => resolve_chat(
                catalog,
                &profiles,
                prepared.workspace_id,
                &request.query,
                now,
            )?,
        };
        let (fence, target) = resolved
            .selected(&request.query)
            .ok_or(ChatError::TargetChanged)?;
        if fence != &request.target_fence {
            return Err(ChatError::TargetChanged);
        }
        let channel = resolved.channel.ok_or(ChatError::TargetChanged)?;
        let outcome = catalog
            .append_and_enqueue_provider_message(
                AppendWorkspaceMessageCommand {
                    message_id: prepared.message_id,
                    workspace_id: prepared.workspace_id,
                    channel,
                    actor,
                    body: request.text.clone(),
                    submitted_at: now,
                    context: prepared.context,
                },
                target.clone(),
                prepared.witness,
            )
            .map_err(|error| match error {
                // Only the atomic append-and-queue phase establishes definite rejection.
                error @ (OperationsError::Conflict(_) | OperationsError::Bounds(_)) => {
                    ChatError::AdmissionRejected(error)
                }
                error => ChatError::Operations(error),
            })?;
        Ok(ChatReceipt {
            message_id: prepared.message_id,
            provider_state: outcome.outbox().state(),
        })
    }
}

impl ResolvedChat {
    fn selected(&self, query: &ChatQuery) -> Option<&(ChatTargetFence, ProviderMessageTarget)> {
        if let Some(runtime) = query.runtime {
            self.targets
                .iter()
                .find(|(fence, _)| fence.runtime_session_id == runtime)
        } else if self.targets.len() == 1 {
            self.targets.first()
        } else {
            None
        }
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
    profile_guard: Result<std::sync::MutexGuard<'_, AgentProfileCatalog>, RuntimeError>,
    workspace_id: WorkspaceId,
    query: &ChatQuery,
    observation: Option<&ChatSourceObservation>,
    now: TimestampMillis,
) -> Result<ResolvedChat, ChatError> {
    if query.node_scope.is_some() {
        let profiles = profile_guard.map_err(map_catalog_guard_error)?;
        return node::resolve_node_chat(catalog, &profiles, workspace_id, query, observation, now);
    }
    let profiles = profile_guard.map_err(map_catalog_guard_error)?;
    resolve_chat(catalog, &profiles, workspace_id, query, now)
}

fn snapshot_from_resolved(
    catalog: &OperationsCatalog,
    workspace_id: WorkspaceId,
    query: &ChatQuery,
    resolved: ResolvedChat,
) -> Result<ChatSnapshot, ChatError> {
    let target = resolved.selected(query).map(|(fence, _)| fence.clone());
    let unavailable_reason = if target.is_some() {
        None
    } else {
        Some(
            resolved
                .unavailable
                .unwrap_or("Select an available conversation."),
        )
    };
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
        targets: resolved
            .targets
            .into_iter()
            .map(|(fence, _)| fence)
            .collect(),
        target,
        entries,
        next_before_sequence: page
            .as_ref()
            .and_then(heterarchy_orchestration_operations::MessagePage::next_cursor),
        unavailable_reason,
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

fn chat_profile(
    catalog: &OperationsCatalog,
    workspace_id: WorkspaceId,
    query: &ChatQuery,
    now: TimestampMillis,
) -> Result<(Option<AgentProfileId>, Option<i64>), ChatError> {
    if let Some(profile_id) = query.agent_profile_id {
        return Ok((Some(profile_id), None));
    }
    if catalog
        .leadership_view(workspace_id, None, now.get())?
        .is_some()
    {
        let Some(authority) =
            catalog.current_leadership_authority(LeadershipScope::workspace(workspace_id))?
        else {
            return Ok((None, None));
        };
        let participations = catalog.workspace_participations(workspace_id)?;
        if participations.truncated() {
            return Err(ChatError::TargetChanged);
        }
        let matches = participations
            .participations()
            .iter()
            .filter(|participation| {
                participation
                    .session_mappings()
                    .iter()
                    .any(|mapping| mapping.participant_id() == authority.participant_id())
            })
            .collect::<Vec<_>>();
        if matches.len() > 1 {
            return Err(ChatError::TargetChanged);
        }
        return Ok((
            matches.first().map(|value| value.profile_id()),
            Some(authority.leadership_revision()),
        ));
    }

    // Retain the pre-leadership first-joined orchestrator only when no
    // Workspace leadership record has ever been established.
    let participations = catalog.workspace_participations(workspace_id)?;
    if participations.truncated() {
        return Err(ChatError::TargetChanged);
    }
    let orchestrator = participations
        .participations()
        .iter()
        .filter(|participation| !participation.session_mappings().is_empty())
        .min_by_key(|participation| participation.created_at())
        .map(heterarchy_orchestration_operations::WorkspaceParticipationSnapshot::profile_id);
    Ok((orchestrator, None))
}

fn resolve_chat(
    catalog: &OperationsCatalog,
    profiles: &AgentProfileCatalog,
    workspace_id: WorkspaceId,
    query: &ChatQuery,
    now: TimestampMillis,
) -> Result<ResolvedChat, ChatError> {
    let (profile_id, leadership_revision) = chat_profile(catalog, workspace_id, query, now)?;
    let Some(profile_id) = profile_id else {
        return Ok(ResolvedChat {
            channel: Some(MessageChannel::Workspace),
            targets: Vec::new(),
            unavailable: Some(
                "No Workspace Orchestrator is available. Select an agent to message.",
            ),
        });
    };
    let Some(participation) = catalog.workspace_participation(workspace_id, profile_id)? else {
        return Ok(ResolvedChat {
            channel: None,
            targets: Vec::new(),
            unavailable: Some("This agent is not a participant in this Workspace."),
        });
    };
    let channel = if query.agent_profile_id.is_some() {
        MessageChannel::Direct(participation.participation_id())
    } else {
        MessageChannel::Workspace
    };
    let profile = profiles
        .resolve(profile_id)?
        .ok_or(ChatError::TargetChanged)?;
    if profile.lifecycle_state() != AgentProfileLifecycleState::Active {
        return Ok(ResolvedChat {
            channel: Some(channel),
            targets: Vec::new(),
            unavailable: Some("This agent is unavailable for new messages."),
        });
    }
    let routes = catalog.workspace_chat_targets(workspace_id, profile_id, CHAT_TARGET_LIMIT)?;
    if routes.truncated() {
        return Err(ChatError::TargetChanged);
    }
    let mut targets = Vec::new();
    for route in routes.targets() {
        if !route_is_activated(catalog, profiles, &participation, route)? {
            continue;
        }
        let Some(runtime) = profiles.runtime_session(route.runtime_session_id())? else {
            continue;
        };
        if runtime.profile_id() != profile_id
            || runtime.provider_conversation_id() != Some(route.provider_conversation_id())
            || !matches!(
                runtime.state(),
                AgentProfileRuntimeSessionState::Ready | AgentProfileRuntimeSessionState::Active
            )
        {
            continue;
        }
        let target = ProviderMessageTarget::new_with_project_route(
            workspace_id,
            profile_id,
            participation.participation_id(),
            AgentProfileProviderFamily::ChatGpt,
            runtime.id(),
            route.provider_conversation_id().to_owned(),
            route.project(),
        )?;
        targets.push((
            ChatTargetFence {
                operation_id: route.operation_id(),
                profile_id: profile_id.into_uuid(),
                profile_name: profile.display_name().to_owned(),
                participation_id: participation.participation_id().into_uuid(),
                participation_revision: participation.revision(),
                runtime_session_id: runtime.id().into_uuid(),
                runtime_revision: runtime.revision().get(),
                provider_conversation_id: route.provider_conversation_id().to_owned(),
                project: route.project(),
                leadership_revision,
                node_target: None,
            },
            target,
        ));
    }
    Ok(ResolvedChat {
        channel: Some(channel),
        unavailable: targets
            .is_empty()
            .then_some("No activated conversation is available for this agent in this Workspace."),
        targets,
    })
}

fn route_is_activated(
    catalog: &OperationsCatalog,
    profiles: &AgentProfileCatalog,
    participation: &heterarchy_orchestration_operations::WorkspaceParticipationSnapshot,
    route: &heterarchy_orchestration_operations::WorkspaceChatTarget,
) -> Result<bool, ChatError> {
    let Some(mapping) = participation
        .session_mappings()
        .iter()
        .find(|mapping| mapping.profile_mapping_request_id() == route.operation_id())
    else {
        return Ok(false);
    };
    let Some(link) = profiles.logical_session_link(mapping.session_id())? else {
        return Ok(false);
    };
    if link.request_id() != route.operation_id() || link.profile_id() != route.profile_id() {
        return Ok(false);
    }
    let identity = catalog
        .logical_identity_for_participant(participation.workspace_id(), mapping.participant_id())?;
    Ok(identity.is_some_and(|identity| {
        identity
            .effect_bindings()
            .iter()
            .any(|binding| binding.activity_id() == route.activity_id())
    }))
}

fn target_matches_fence(target: &ProviderMessageTarget, fence: &ChatTargetFence) -> bool {
    target.profile_id().into_uuid() == fence.profile_id
        && target.participation_id().into_uuid() == fence.participation_id
        && target.runtime_session_id().into_uuid() == fence.runtime_session_id
        && target.provider_conversation_id() == fence.provider_conversation_id
        && target.project() == fence.project
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

fn profile_catalog_guard<T>(result: Result<T, RuntimeError>) -> Result<T, ChatError> {
    result.map_err(map_catalog_guard_error)
}

fn map_catalog_guard_error(error: RuntimeError) -> ChatError {
    match error {
        RuntimeError::CatalogOwnerPoisoned => ChatError::CatalogLock,
        other => ChatError::Runtime(other),
    }
}
