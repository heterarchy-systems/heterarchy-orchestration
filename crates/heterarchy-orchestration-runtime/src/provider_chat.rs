//! Provider-worker chat operations over the existing durable outbox owner.

use forge_domain::{AuthenticatedPrincipal, TimestampMillis};
use heterarchy_orchestration_operations::{
    ChatGptProjectId, IncomingProviderMessageOutcome, MessageOrigin,
    ProviderMessageDeliveryReceipt, ProviderMessageDispatchAction, ProviderMessageInFlightPage,
    ProviderMessageOutbox, ProviderMessageSource, ProviderMessageState, ProviderMessageTarget,
    ProviderMessageUnknownIsolationCommand, ProviderMessageUnknownIsolationReceipt,
    RecordIncomingProviderMessageCommand, WorkspaceMessage, provider_message_text,
};
use heterarchy_orchestration_sessions::AgentProfileProviderFamily;
use uuid::Uuid;

use crate::{OrchestrationRuntime, RuntimeError, profile_error_to_runtime};

// Preserve existing incoming-turn identities across the transport cutover.
const INCOMING_ID_DOMAIN: &[u8] = b"heterarchy.orchestration.native-chat.incoming.v1\0";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderIncomingRole {
    Assistant,
    User,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderIncomingCommand {
    pub reply_to_message_id: Uuid,
    pub provider_conversation_id: String,
    pub provider_turn_id: String,
    pub role: ProviderIncomingRole,
    pub body: String,
}

/// A frozen action or a terminal/uncertain acknowledgement without an action.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderChatDispatch {
    pub outbox: ProviderMessageOutbox,
    pub action: Option<ProviderMessageDispatchAction>,
    pub body: Option<String>,
    pub expires_at_ms: Option<i64>,
}

impl OrchestrationRuntime {
    /// Observe bounded current pending/unknown rows without releasing actions.
    ///
    /// # Errors
    /// Rejects nonoperators, invalid cursors and corrupt scopes.
    pub fn provider_worker_in_flight_messages(
        &self,
        principal: AuthenticatedPrincipal,
        after_message_id: Option<Uuid>,
        limit: u32,
    ) -> Result<ProviderMessageInFlightPage, RuntimeError> {
        self.require_operator(principal)?;
        Ok(self
            .catalog()?
            .provider_message_in_flight_page(after_message_id, limit)?)
    }

    /// Recover a provider turn through its original reply route without writing.
    /// Absence does not prove nonadmission or permit retry.
    ///
    /// # Errors
    /// Rejects foreign routes, changed body/role/context and catalog failures.
    pub fn provider_worker_incoming_readback(
        &self,
        principal: AuthenticatedPrincipal,
        command: &ProviderIncomingCommand,
    ) -> Result<Option<WorkspaceMessage>, RuntimeError> {
        self.require_operator(principal)?;
        let outbox = self.provider_chat_outbox(command.reply_to_message_id)?;
        self.provider_chat_historical_target(outbox.target())?;
        if outbox.target().provider_conversation_id() != command.provider_conversation_id {
            return Err(RuntimeError::Conflict);
        }
        let catalog = self.catalog()?;
        let replied_to = catalog
            .workspace_message(command.reply_to_message_id)?
            .ok_or(RuntimeError::NotFound)?;
        if replied_to.origin() != &MessageOrigin::Internal {
            return Err(RuntimeError::Conflict);
        }
        let Some(observation) =
            catalog.incoming_provider_turn(outbox.target(), &command.provider_turn_id)?
        else {
            return Ok(None);
        };
        let message = observation.message;
        let role_matches = match (message.origin(), command.role) {
            (MessageOrigin::Internal, ProviderIncomingRole::User) => true,
            (
                MessageOrigin::Provider {
                    source:
                        ProviderMessageSource::Known {
                            profile_id,
                            participation_id,
                            ..
                        },
                    ..
                },
                ProviderIncomingRole::Assistant,
            ) => {
                *profile_id == outbox.target().profile_id()
                    && *participation_id == outbox.target().participation_id()
            }
            (
                MessageOrigin::Provider {
                    source: ProviderMessageSource::Unknown { label },
                    ..
                },
                ProviderIncomingRole::User,
            ) => label == "provider user turn",
            _ => false,
        };
        if !role_matches
            || observation.observed_body != command.body
            || message.workspace_id() != replied_to.workspace_id()
            || message.channel() != replied_to.channel()
            || message.context() != replied_to.context()
        {
            return Err(RuntimeError::Conflict);
        }
        Ok(Some(message))
    }

    /// Read exact worker evidence without changing state or admitting Execute.
    ///
    /// # Errors
    /// Rejects nonoperators, missing messages and changed historical targets.
    pub fn provider_worker_message_readback(
        &self,
        principal: AuthenticatedPrincipal,
        message_id: Uuid,
    ) -> Result<ProviderChatDispatch, RuntimeError> {
        self.require_operator(principal)?;
        let outbox = self.provider_chat_outbox(message_id)?;
        self.provider_chat_historical_target(outbox.target())?;
        let catalog = self.catalog()?;
        let expires_at_ms = catalog.provider_message_expiry(message_id)?;
        let message = catalog
            .workspace_message(message_id)?
            .ok_or(RuntimeError::NotFound)?;
        let body = provider_message_text(&message)?;
        Ok(ProviderChatDispatch {
            outbox,
            action: None,
            body: Some(body),
            expires_at_ms,
        })
    }

    /// Claim one bounded outbox action; pending/unknown work stays inspect-only.
    ///
    /// # Errors
    /// Rejects nonoperators and owner/fence/target failures. No provider effect occurs here.
    pub fn provider_worker_next_message(
        &self,
        principal: AuthenticatedPrincipal,
        now_ms: i64,
    ) -> Result<Option<ProviderChatDispatch>, RuntimeError> {
        self.require_operator(principal)?;
        let now = timestamp(now_ms)?;
        let outcome = self.catalog()?.next_provider_message_dispatch(now)?;
        outcome
            .map(|outcome| {
                self.provider_chat_dispatch(
                    outcome.outbox().clone(),
                    outcome.dispatch().action(),
                    now,
                )
            })
            .transpose()
    }

    /// Read one exact worker identity without admitting a new Execute.
    ///
    /// # Errors
    /// Rejects nonoperators, missing subjects and changed historical targets.
    pub fn provider_worker_inspect_message(
        &self,
        principal: AuthenticatedPrincipal,
        message_id: Uuid,
        now_ms: i64,
    ) -> Result<ProviderChatDispatch, RuntimeError> {
        self.require_operator(principal)?;
        let now = timestamp(now_ms)?;
        let outbox = self.provider_chat_outbox(message_id)?;
        self.provider_chat_historical_target(outbox.target())?;
        self.provider_chat_dispatch(outbox, ProviderMessageDispatchAction::Inspect, now)
    }

    /// Record uncertainty with the exact outbox revision and canonical target.
    ///
    /// # Errors
    /// Rejects nonoperators, changed targets and stale fences; never requeues work.
    pub fn provider_worker_mark_message_unknown(
        &self,
        principal: AuthenticatedPrincipal,
        message_id: Uuid,
        expected_revision: u64,
        now_ms: i64,
    ) -> Result<ProviderMessageOutbox, RuntimeError> {
        self.require_operator(principal)?;
        let outbox = self.provider_chat_outbox(message_id)?;
        self.provider_chat_historical_target(outbox.target())?;
        Ok(self.catalog()?.mark_provider_message_effect_unknown(
            message_id,
            expected_revision,
            outbox.target(),
            timestamp(now_ms)?,
        )?)
    }

    /// Preserve an exact UNKNOWN provider row and release unrelated runtimes.
    ///
    /// # Errors
    /// Rejects nonoperators, changed targets, stale revisions and owner failures.
    pub fn provider_worker_isolate_unknown_message(
        &self,
        principal: AuthenticatedPrincipal,
        command: &ProviderMessageUnknownIsolationCommand,
    ) -> Result<ProviderMessageUnknownIsolationReceipt, RuntimeError> {
        self.require_operator(principal)?;
        if command.principal() != principal {
            return Err(RuntimeError::Forbidden);
        }
        self.provider_chat_historical_target(command.expected_target())?;
        Ok(self.catalog()?.isolate_unknown_provider_message(command)?)
    }

    /// Read the exact immutable isolation receipt without changing provider state.
    ///
    /// # Errors
    /// Rejects nonoperators, nil identities and corrupt catalog data.
    pub fn provider_worker_unknown_message_isolation(
        &self,
        principal: AuthenticatedPrincipal,
        message_id: Uuid,
        request_id: Option<Uuid>,
    ) -> Result<Option<ProviderMessageUnknownIsolationReceipt>, RuntimeError> {
        self.require_operator(principal)?;
        Ok(self
            .catalog()?
            .provider_message_unknown_isolation(message_id, request_id)?)
    }

    /// Record a definite provider failure. Uncertainty must use the unknown path.
    ///
    /// # Errors
    /// Rejects nonoperators, changed targets and stale fences.
    pub fn provider_worker_fail_message(
        &self,
        principal: AuthenticatedPrincipal,
        message_id: Uuid,
        expected_revision: u64,
        now_ms: i64,
    ) -> Result<ProviderMessageOutbox, RuntimeError> {
        self.require_operator(principal)?;
        let outbox = self.provider_chat_outbox(message_id)?;
        self.provider_chat_historical_target(outbox.target())?;
        Ok(self.catalog()?.fail_provider_message_delivery(
            message_id,
            expected_revision,
            outbox.target(),
            timestamp(now_ms)?,
        )?)
    }

    /// Cancel an undispatched Queued provider message.
    ///
    /// # Errors
    /// Rejects nonoperators, changed targets and stale fences.
    pub fn provider_worker_cancel_queued_message(
        &self,
        principal: AuthenticatedPrincipal,
        message_id: Uuid,
        expected_revision: u64,
        now_ms: i64,
    ) -> Result<ProviderMessageOutbox, RuntimeError> {
        self.require_operator(principal)?;
        let outbox = self.provider_chat_outbox(message_id)?;
        self.provider_chat_historical_target(outbox.target())?;
        Ok(self.catalog()?.cancel_queued_provider_message(
            message_id,
            expected_revision,
            outbox.target(),
            timestamp(now_ms)?,
        )?)
    }

    /// Confirm the exact observed provider conversation, public turn and body.
    ///
    /// # Errors
    /// Rejects nonoperators, changed target/body/turn, stale fences and owner failures.
    #[allow(clippy::too_many_arguments)]
    pub fn provider_worker_confirm_message_delivery(
        &self,
        principal: AuthenticatedPrincipal,
        message_id: Uuid,
        expected_revision: u64,
        provider_conversation_id: &str,
        provider_turn_id: String,
        body: String,
        now_ms: i64,
    ) -> Result<ProviderMessageOutbox, RuntimeError> {
        self.provider_worker_confirm_message_delivery_with_project(
            principal,
            message_id,
            expected_revision,
            provider_conversation_id,
            None,
            provider_turn_id,
            body,
            now_ms,
        )
    }

    /// Confirm a delivered turn with the actual visible provider Project.
    /// Bound message routes require exact membership evidence; legacy messages
    /// preserve the old absent-field contract.
    ///
    /// # Errors
    /// Rejects nonoperators, changed target/body/turn, missing/mismatched Project
    /// evidence, stale fences and owner failures.
    #[allow(clippy::too_many_arguments)]
    pub fn provider_worker_confirm_message_delivery_with_project(
        &self,
        principal: AuthenticatedPrincipal,
        message_id: Uuid,
        expected_revision: u64,
        provider_conversation_id: &str,
        provider_project_id: Option<&str>,
        provider_turn_id: String,
        body: String,
        now_ms: i64,
    ) -> Result<ProviderMessageOutbox, RuntimeError> {
        self.require_operator(principal)?;
        let outbox = self.provider_chat_outbox(message_id)?;
        self.provider_chat_historical_target(outbox.target())?;
        if outbox.target().provider_conversation_id() != provider_conversation_id {
            return Err(RuntimeError::Conflict);
        }
        let provider_project_id = provider_project_id
            .map(ChatGptProjectId::try_from)
            .transpose()
            .map_err(|_| RuntimeError::InvalidInput)?;
        match (outbox.target().project(), provider_project_id) {
            (None, None) => {}
            (Some(expected), Some(actual)) if expected.project_id() == actual => {}
            _ => return Err(RuntimeError::Conflict),
        }
        let receipt = ProviderMessageDeliveryReceipt::new_with_project(
            outbox.target().clone(),
            provider_project_id,
            provider_turn_id,
            body,
        )?;
        Ok(self.catalog()?.confirm_provider_message_delivery(
            message_id,
            expected_revision,
            &receipt,
            timestamp(now_ms)?,
        )?)
    }

    /// Deduplicate an incoming public turn under its original frozen chat route.
    ///
    /// # Errors
    /// Rejects nonoperators, foreign conversations, changed replays and invalid bodies.
    pub fn provider_worker_record_incoming(
        &self,
        principal: AuthenticatedPrincipal,
        command: ProviderIncomingCommand,
        now_ms: i64,
    ) -> Result<IncomingProviderMessageOutcome, RuntimeError> {
        self.require_operator(principal)?;
        let now = timestamp(now_ms)?;
        if command.provider_turn_id.is_empty() || command.provider_turn_id.len() > 512 {
            return Err(RuntimeError::InvalidInput);
        }
        let existing = self.provider_worker_incoming_readback(principal, &command)?;
        let outbox = self.provider_chat_outbox(command.reply_to_message_id)?;
        self.provider_chat_historical_target(outbox.target())?;
        if outbox.target().provider_conversation_id() != command.provider_conversation_id {
            return Err(RuntimeError::Conflict);
        }
        let mut catalog = self.catalog()?;
        let replied_to = catalog
            .workspace_message(command.reply_to_message_id)?
            .ok_or(RuntimeError::NotFound)?;
        if replied_to.origin() != &MessageOrigin::Internal {
            return Err(RuntimeError::Conflict);
        }
        let profiles = self.profile_catalog_guard()?;
        let profile = profiles
            .resolve(outbox.target().profile_id())
            .map_err(profile_error_to_runtime)?
            .ok_or(RuntimeError::NotFound)?;
        let source = if let Some(MessageOrigin::Provider { source, .. }) =
            existing.as_ref().map(WorkspaceMessage::origin)
        {
            source.clone()
        } else {
            match command.role {
                ProviderIncomingRole::Assistant => ProviderMessageSource::known(
                    outbox.target().profile_id(),
                    outbox.target().participation_id(),
                    profile.display_name().to_owned(),
                )?,
                ProviderIncomingRole::User => {
                    ProviderMessageSource::unknown("provider user turn".to_owned())?
                }
            }
        };
        let mut hash = blake3::Hasher::new();
        hash.update(INCOMING_ID_DOMAIN);
        hash.update(replied_to.workspace_id().into_uuid().as_bytes());
        hash.update(outbox.target().runtime_session_id().into_uuid().as_bytes());
        hash.update(command.provider_turn_id.as_bytes());
        let mut bytes = [0_u8; 16];
        bytes.copy_from_slice(&hash.finalize().as_bytes()[..16]);
        bytes[6] = (bytes[6] & 0x0f) | 0x80;
        bytes[8] = (bytes[8] & 0x3f) | 0x80;
        let message_id = Uuid::from_bytes(bytes);
        let submitted_at = catalog
            .workspace_message(message_id)?
            .map_or(now, |message| message.submitted_at());
        Ok(
            catalog.record_incoming_provider_message(RecordIncomingProviderMessageCommand {
                message_id,
                workspace_id: replied_to.workspace_id(),
                channel: replied_to.channel(),
                target: outbox.target().clone(),
                source,
                provider_turn_id: command.provider_turn_id,
                body: command.body,
                submitted_at,
                context: replied_to.context(),
            })?,
        )
    }

    fn provider_chat_outbox(
        &self,
        message_id: Uuid,
    ) -> Result<ProviderMessageOutbox, RuntimeError> {
        if message_id.is_nil() {
            return Err(RuntimeError::InvalidInput);
        }
        self.catalog()?
            .provider_message_outbox(message_id)?
            .ok_or(RuntimeError::NotFound)
    }

    fn provider_chat_historical_target(
        &self,
        target: &ProviderMessageTarget,
    ) -> Result<(), RuntimeError> {
        let catalog = self.catalog()?;
        let routes =
            catalog.workspace_chat_targets(target.workspace_id(), target.profile_id(), 8)?;
        if routes.truncated()
            || !routes.targets().iter().any(|route| {
                route.profile_id() == target.profile_id()
                    && route.participation_id() == target.participation_id()
                    && route.runtime_session_id() == target.runtime_session_id()
                    && route.provider_conversation_id() == target.provider_conversation_id()
                    && route.project() == target.project()
            })
        {
            return Err(RuntimeError::Conflict);
        }
        let profiles = self.profile_catalog_guard()?;
        let profile = profiles
            .resolve(target.profile_id())
            .map_err(profile_error_to_runtime)?
            .ok_or(RuntimeError::NotFound)?;
        let runtime = profiles
            .runtime_session(target.runtime_session_id())
            .map_err(profile_error_to_runtime)?
            .ok_or(RuntimeError::NotFound)?;
        if profile.provider_family() != AgentProfileProviderFamily::ChatGpt
            || runtime.profile_id() != target.profile_id()
            || runtime.provider_conversation_id() != Some(target.provider_conversation_id())
        {
            return Err(RuntimeError::Conflict);
        }
        Ok(())
    }

    fn provider_chat_dispatch(
        &self,
        outbox: ProviderMessageOutbox,
        action: ProviderMessageDispatchAction,
        now: TimestampMillis,
    ) -> Result<ProviderChatDispatch, RuntimeError> {
        if !matches!(
            outbox.state(),
            ProviderMessageState::EffectPending | ProviderMessageState::Unknown
        ) {
            return Ok(ProviderChatDispatch {
                outbox,
                action: None,
                body: None,
                expires_at_ms: None,
            });
        }
        let catalog = self.catalog()?;
        let expires_at_ms = catalog.provider_message_expiry(outbox.message_id())?;
        let body = catalog
            .workspace_message(outbox.message_id())?
            .ok_or(RuntimeError::NotFound)
            .and_then(|message| provider_message_text(&message).map_err(Into::into));
        drop(catalog);
        let valid = match action {
            ProviderMessageDispatchAction::Execute => self
                .validate_provider_target(outbox.target(), now)
                .map_err(|error| RuntimeError::Chat(Box::new(error))),
            ProviderMessageDispatchAction::Inspect => self
                .provider_chat_historical_target(outbox.target())
                .map(|()| true),
        };
        let expired = action == ProviderMessageDispatchAction::Execute
            && expires_at_ms.is_some_and(|expires| now.get() >= expires);
        if expired || body.is_err() || !matches!(valid, Ok(true)) {
            let outbox = match action {
                ProviderMessageDispatchAction::Execute => {
                    self.catalog()?.fail_provider_message_delivery(
                        outbox.message_id(),
                        outbox.revision(),
                        outbox.target(),
                        now,
                    )?
                }
                ProviderMessageDispatchAction::Inspect
                    if outbox.state() == ProviderMessageState::Unknown =>
                {
                    outbox
                }
                ProviderMessageDispatchAction::Inspect => {
                    self.catalog()?.mark_provider_message_effect_unknown(
                        outbox.message_id(),
                        outbox.revision(),
                        outbox.target(),
                        now,
                    )?
                }
            };
            return Ok(ProviderChatDispatch {
                outbox,
                action: None,
                body: None,
                expires_at_ms,
            });
        }
        Ok(ProviderChatDispatch {
            outbox,
            action: Some(action),
            body: Some(body?),
            expires_at_ms,
        })
    }
}

fn timestamp(now_ms: i64) -> Result<TimestampMillis, RuntimeError> {
    TimestampMillis::new(now_ms).map_err(|_| RuntimeError::InvalidInput)
}

#[cfg(test)]
mod tests;
