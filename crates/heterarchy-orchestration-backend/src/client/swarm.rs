//! Bounded typed HTTP client for Native Swarm coordination.
//!
//! Each call presents its caller-supplied bearer credential, enforces strict
//! request validation and response correlation, and never retries automatically.

use std::time::Duration;

use heterarchy_orchestration_operations::{ExecutionTimeouts, MAX_SWARM_BOARD_PAGE_SIZE};
use hyper::Method;
use hyper::header::HeaderValue;
use uuid::Uuid;

use crate::wire::autonomy_loop::parse_review_verdict_outcome;
use crate::wire::swarm::{
    AbortSwarmRunRequest, AddSwarmMemberRequest, ClaimSwarmWakeupRequest, CompleteSwarmTurnRequest,
    ConvergeSwarmRequest, ConvergeSwarmResponse, CreateSwarmChannelRequest, CreateSwarmRunRequest,
    CreateSwarmRunResponse, CreateSwarmVerificationPanelRequest, DispatchSwarmTurnRequest,
    DispatchSwarmTurnResponse, FailSwarmTurnRequest, ForgeConvergeSwarmRequest,
    ForgeConvergeSwarmResponse, ForgeSettleSwarmRequest, ForgeSettleSwarmResponse,
    KillSwarmMemberRequest, KillSwarmMemberResponse, ListSwarmRunsQuery, PostSwarmMessageRequest,
    PostSwarmMessageResponse, ReadSwarmMessagesQuery, RecordSwarmVerdictRequest,
    SettleSwarmRequest, SubscribeSwarmChannelRequest, SubscribeSwarmChannelResponse,
    SwarmBoardPageResponse, SwarmChannelWire, SwarmMemberWire, SwarmModelSwitchResponse,
    SwarmRunInspectionResponse, SwarmRunListResponse, SwarmRunWire, SwarmStallCheckRequest,
    SwarmStallCheckResponse, SwarmVerdictWire, SwarmVerificationPanelWire, SwarmWakeupBatchWire,
    SwitchSwarmMemberModelRequest, SyncAutonomyOutboxRequest, SyncAutonomyOutboxResponse,
    parse_non_zero_digest_hex, parse_swarm_commit_oid, parse_swarm_convergence_target,
    parse_swarm_role_kind, parse_swarm_turn_failure_disposition, parse_swarm_workspace_mode,
};

use super::{
    ClientError, ExchangeLimits, OrchestrationHttpClient, ReadbackTarget, canonical_identity,
    require_identity,
};

/// Bounded response byte ceiling for Swarm snapshots and incremental board pages (`2 MiB`).
pub(super) const MAX_SWARM_RESPONSE_BYTES: usize = 2 * 1024 * 1024;

/// Hard upper bound (`15m`) for one supervised `swarm dispatch` HTTP exchange.
const MAX_SWARM_DISPATCH_TIMEOUT: Duration = Duration::from_mins(15);
/// Transport slack (`5s`) added on top of configured supervision timeouts during `swarm dispatch`.
const SWARM_DISPATCH_SLACK_MS: u64 = 5_000;

pub(super) fn is_swarm_path(path: &str) -> bool {
    (path.starts_with("/v1/workspaces/") && path.contains("/swarms"))
        || (path.starts_with("/internal/v1/workspaces/") && path.ends_with("/autonomy-outbox/sync"))
}

impl OrchestrationHttpClient {
    /// Create or idempotently replay a `Standalone` or `DagNodeHybrid` Swarm run.
    ///
    /// # Errors
    /// Rejects malformed identities/digests, bounded transport failures, typed
    /// service rejections, or uncertain creation outcomes.
    pub async fn create_swarm_run(
        &self,
        authorization: &HeaderValue,
        workspace_id: Uuid,
        request: &CreateSwarmRunRequest,
    ) -> Result<CreateSwarmRunResponse, ClientError> {
        require_identity(workspace_id)?;
        let nonce = validate_create_swarm_request(workspace_id, request)?;
        let readback = ReadbackTarget::SwarmRunCreate {
            workspace_id,
            request_id: nonce,
        };
        let response: CreateSwarmRunResponse = self
            .call(
                authorization,
                Method::POST,
                format!("/v1/workspaces/{workspace_id}/swarms"),
                Some(request),
                Some(readback.clone()),
            )
            .await?;
        if !valid_swarm_run(&response.run, workspace_id)
            || response.main_channel.workspace_id != workspace_id
            || response.main_channel.swarm_run_id != response.run.swarm_run_id
            || response.main_channel.channel_id.is_nil()
        {
            return Err(ClientError::UnknownOutcome { readback });
        }
        Ok(response)
    }

    /// List bounded Swarm runs in `workspace_id`.
    ///
    /// # Errors
    /// Rejects invalid `limit` bounds, bounded transport failures, typed service
    /// rejections, or foreign/malformed run records.
    pub async fn list_swarm_runs(
        &self,
        authorization: &HeaderValue,
        workspace_id: Uuid,
        query: &ListSwarmRunsQuery,
    ) -> Result<SwarmRunListResponse, ClientError> {
        require_identity(workspace_id)?;
        if let Some(limit) = query.limit
            && !(1..=MAX_SWARM_BOARD_PAGE_SIZE).contains(&limit)
        {
            return Err(ClientError::InvalidRequest);
        }
        let path = match query.limit {
            Some(limit) => format!("/v1/workspaces/{workspace_id}/swarms?limit={limit}"),
            None => format!("/v1/workspaces/{workspace_id}/swarms"),
        };
        let response: SwarmRunListResponse = self
            .call::<_, ()>(authorization, Method::GET, path, None, None)
            .await?;
        let max_len = query.limit.unwrap_or(MAX_SWARM_BOARD_PAGE_SIZE);
        if response.runs.len() > max_len
            || !response
                .runs
                .iter()
                .all(|run| valid_swarm_run(run, workspace_id))
        {
            return Err(ClientError::Protocol);
        }
        Ok(response)
    }

    /// Read the full consistent Swarm run inspection snapshot, roster, watermarks, and panel.
    ///
    /// # Errors
    /// Rejects nil identities, bounded transport failures, typed service rejections,
    /// or uncorrelated snapshot payloads.
    pub async fn get_swarm_run_snapshot(
        &self,
        authorization: &HeaderValue,
        workspace_id: Uuid,
        swarm_run_id: Uuid,
    ) -> Result<SwarmRunInspectionResponse, ClientError> {
        require_identity(workspace_id)?;
        require_identity(swarm_run_id)?;
        let response: SwarmRunInspectionResponse = self
            .call::<_, ()>(
                authorization,
                Method::GET,
                format!("/v1/workspaces/{workspace_id}/swarms/{swarm_run_id}"),
                None,
                None,
            )
            .await?;
        if response.run.swarm_run_id != swarm_run_id
            || !valid_swarm_run(&response.run, workspace_id)
            || response
                .channels
                .iter()
                .any(|ch| ch.swarm_run_id != swarm_run_id || ch.workspace_id != workspace_id)
            || response
                .members
                .iter()
                .any(|m| m.swarm_run_id != swarm_run_id || m.workspace_id != workspace_id)
        {
            return Err(ClientError::Protocol);
        }
        Ok(response)
    }

    /// Create an isolated area channel inside an active Swarm run.
    ///
    /// # Errors
    /// Rejects malformed identities/names, bounded transport failures, typed
    /// service rejections, or uncertain channel creation outcomes.
    pub async fn create_swarm_channel(
        &self,
        authorization: &HeaderValue,
        workspace_id: Uuid,
        swarm_run_id: Uuid,
        request: &CreateSwarmChannelRequest,
    ) -> Result<SwarmChannelWire, ClientError> {
        require_identity(workspace_id)?;
        require_identity(swarm_run_id)?;
        let channel_id = canonical_identity(&request.channel_id)?;
        if request.name.trim().is_empty() {
            return Err(ClientError::InvalidRequest);
        }
        if let Some(creator) = &request.created_by_member_id {
            let _creator_uuid = canonical_identity(creator)?;
        }
        let readback = ReadbackTarget::SwarmChannelCreate {
            workspace_id,
            swarm_run_id,
            channel_id,
        };
        let response: SwarmChannelWire = self
            .call(
                authorization,
                Method::POST,
                format!("/v1/workspaces/{workspace_id}/swarms/{swarm_run_id}/channels"),
                Some(request),
                Some(readback.clone()),
            )
            .await?;
        if response.channel_id != channel_id
            || response.swarm_run_id != swarm_run_id
            || response.workspace_id != workspace_id
        {
            return Err(ClientError::UnknownOutcome { readback });
        }
        Ok(response)
    }

    /// Subscribe an active Swarm member to an area channel.
    ///
    /// # Errors
    /// Rejects malformed identities, bounded transport failures, typed service
    /// rejections, or uncertain subscription outcomes.
    pub async fn subscribe_swarm_channel(
        &self,
        authorization: &HeaderValue,
        workspace_id: Uuid,
        swarm_run_id: Uuid,
        channel_id: Uuid,
        request: &SubscribeSwarmChannelRequest,
    ) -> Result<SubscribeSwarmChannelResponse, ClientError> {
        require_identity(workspace_id)?;
        require_identity(swarm_run_id)?;
        require_identity(channel_id)?;
        let member_id = canonical_identity(&request.member_id)?;
        let readback = ReadbackTarget::SwarmChannelSubscription {
            workspace_id,
            swarm_run_id,
            channel_id,
            member_id,
        };
        let response: SubscribeSwarmChannelResponse = self
            .call(
                authorization,
                Method::POST,
                format!(
                    "/v1/workspaces/{workspace_id}/swarms/{swarm_run_id}/channels/{channel_id}/subscriptions"
                ),
                Some(request),
                Some(readback.clone()),
            )
            .await?;
        if response.swarm_run_id != swarm_run_id
            || response.channel_id != channel_id
            || response.member_id != member_id
        {
            return Err(ClientError::UnknownOutcome { readback });
        }
        Ok(response)
    }

    /// Add a `Coordinator`, `Worker`, `Helper`, `Verifier`, or `Monitor` to a Swarm run.
    ///
    /// # Errors
    /// Rejects malformed identities/roles/modes, bounded transport failures, typed
    /// service rejections (`403`/`409`), or uncertain member admission outcomes.
    pub async fn add_swarm_member(
        &self,
        authorization: &HeaderValue,
        workspace_id: Uuid,
        swarm_run_id: Uuid,
        request: &AddSwarmMemberRequest,
    ) -> Result<SwarmMemberWire, ClientError> {
        require_identity(workspace_id)?;
        require_identity(swarm_run_id)?;
        let member_id = validate_add_member_request(request)?;
        let readback = ReadbackTarget::SwarmMemberMutation {
            workspace_id,
            swarm_run_id,
            member_id,
        };
        let response: SwarmMemberWire = self
            .call(
                authorization,
                Method::POST,
                format!("/v1/workspaces/{workspace_id}/swarms/{swarm_run_id}/members"),
                Some(request),
                Some(readback.clone()),
            )
            .await?;
        if !valid_swarm_member(&response, workspace_id, swarm_run_id, member_id) {
            return Err(ClientError::UnknownOutcome { readback });
        }
        Ok(response)
    }

    /// Kill a Swarm member and cascade-cancel its descendant helpers.
    ///
    /// # Errors
    /// Rejects nil identities, bounded transport failures, typed service rejections,
    /// or uncertain kill outcomes.
    pub async fn kill_swarm_member(
        &self,
        authorization: &HeaderValue,
        workspace_id: Uuid,
        swarm_run_id: Uuid,
        member_id: Uuid,
        request: &KillSwarmMemberRequest,
    ) -> Result<KillSwarmMemberResponse, ClientError> {
        require_identity(workspace_id)?;
        require_identity(swarm_run_id)?;
        require_identity(member_id)?;
        let readback = ReadbackTarget::SwarmMemberMutation {
            workspace_id,
            swarm_run_id,
            member_id,
        };
        let response: KillSwarmMemberResponse = self
            .call(
                authorization,
                Method::POST,
                format!(
                    "/v1/workspaces/{workspace_id}/swarms/{swarm_run_id}/members/{member_id}/kill"
                ),
                Some(request),
                Some(readback.clone()),
            )
            .await?;
        if !response
            .killed_members
            .iter()
            .any(|member| member.member_id == member_id)
            || response
                .killed_members
                .iter()
                .any(|m| m.workspace_id != workspace_id || m.swarm_run_id != swarm_run_id)
        {
            return Err(ClientError::UnknownOutcome { readback });
        }
        Ok(response)
    }

    /// Claim a single-flight coalesced wakeup batch and transition `member_id` into `Working`.
    ///
    /// # Errors
    /// Rejects malformed identities, bounded transport failures, typed service
    /// rejections, or uncertain wakeup claim outcomes.
    pub async fn claim_swarm_turn(
        &self,
        authorization: &HeaderValue,
        workspace_id: Uuid,
        swarm_run_id: Uuid,
        member_id: Uuid,
        request: &ClaimSwarmWakeupRequest,
    ) -> Result<SwarmWakeupBatchWire, ClientError> {
        require_identity(workspace_id)?;
        require_identity(swarm_run_id)?;
        require_identity(member_id)?;
        let snapshot_id = canonical_identity(&request.snapshot_id)?;
        let readback = ReadbackTarget::SwarmMemberMutation {
            workspace_id,
            swarm_run_id,
            member_id,
        };
        let response: SwarmWakeupBatchWire = self
            .call(
                authorization,
                Method::POST,
                format!(
                    "/v1/workspaces/{workspace_id}/swarms/{swarm_run_id}/members/{member_id}/turns/claim"
                ),
                Some(request),
                Some(readback.clone()),
            )
            .await?;
        if response.swarm_run_id != swarm_run_id
            || response.snapshot_id != snapshot_id
            || !valid_swarm_member(&response.member, workspace_id, swarm_run_id, member_id)
        {
            return Err(ClientError::UnknownOutcome { readback });
        }
        Ok(response)
    }

    /// Cleanly complete an in-flight `Working` member turn and commit `committed_seen_seq`.
    ///
    /// # Errors
    /// Rejects malformed snapshot/output digests, bounded transport failures, typed
    /// service rejections, or uncertain turn completion outcomes.
    pub async fn complete_swarm_turn(
        &self,
        authorization: &HeaderValue,
        workspace_id: Uuid,
        swarm_run_id: Uuid,
        member_id: Uuid,
        request: &CompleteSwarmTurnRequest,
    ) -> Result<SwarmMemberWire, ClientError> {
        require_identity(workspace_id)?;
        require_identity(swarm_run_id)?;
        require_identity(member_id)?;
        let _snapshot_id = canonical_identity(&request.expected_snapshot_id)?;
        request
            .to_turn_output()
            .map_err(|_| ClientError::InvalidRequest)?;
        let readback = ReadbackTarget::SwarmMemberMutation {
            workspace_id,
            swarm_run_id,
            member_id,
        };
        let response: SwarmMemberWire = self
            .call(
                authorization,
                Method::POST,
                format!(
                    "/v1/workspaces/{workspace_id}/swarms/{swarm_run_id}/members/{member_id}/turns/complete"
                ),
                Some(request),
                Some(readback.clone()),
            )
            .await?;
        if !valid_swarm_member(&response, workspace_id, swarm_run_id, member_id) {
            return Err(ClientError::UnknownOutcome { readback });
        }
        Ok(response)
    }

    /// Fail or suspend an in-flight `Working` member turn while preserving `committed_seen_seq`.
    ///
    /// # Errors
    /// Rejects malformed snapshot/disposition values, bounded transport failures,
    /// typed service rejections, or uncertain turn release outcomes.
    pub async fn fail_swarm_turn(
        &self,
        authorization: &HeaderValue,
        workspace_id: Uuid,
        swarm_run_id: Uuid,
        member_id: Uuid,
        request: &FailSwarmTurnRequest,
    ) -> Result<SwarmMemberWire, ClientError> {
        require_identity(workspace_id)?;
        require_identity(swarm_run_id)?;
        require_identity(member_id)?;
        let _snapshot_id = canonical_identity(&request.expected_snapshot_id)?;
        parse_swarm_turn_failure_disposition(&request.disposition)
            .map_err(|_| ClientError::InvalidRequest)?;
        let readback = ReadbackTarget::SwarmMemberMutation {
            workspace_id,
            swarm_run_id,
            member_id,
        };
        let response: SwarmMemberWire = self
            .call(
                authorization,
                Method::POST,
                format!(
                    "/v1/workspaces/{workspace_id}/swarms/{swarm_run_id}/members/{member_id}/turns/fail"
                ),
                Some(request),
                Some(readback.clone()),
            )
            .await?;
        if !valid_swarm_member(&response, workspace_id, swarm_run_id, member_id) {
            return Err(ClientError::UnknownOutcome { readback });
        }
        Ok(response)
    }

    /// Dispatch and supervise one single-flight `WakeQueued` Swarm member turn via the
    /// process-wide `SwarmRuntimeDispatcher`.
    ///
    /// # Errors
    /// Rejects malformed snapshot/lease/timeout fields, bounded transport failures,
    /// typed service rejections (`404`/`409`), or uncertain turn dispatch outcomes.
    pub async fn dispatch_swarm_member_turn(
        &self,
        authorization: &HeaderValue,
        workspace_id: Uuid,
        swarm_run_id: Uuid,
        member_id: Uuid,
        request: &DispatchSwarmTurnRequest,
    ) -> Result<DispatchSwarmTurnResponse, ClientError> {
        require_identity(workspace_id)?;
        require_identity(swarm_run_id)?;
        require_identity(member_id)?;
        let (snapshot_id, timeout_budget) = validate_dispatch_turn_request(request)?;
        let exchange_timeout = self
            .timeout
            .max(timeout_budget)
            .min(MAX_SWARM_DISPATCH_TIMEOUT);
        let readback = ReadbackTarget::SwarmMemberMutation {
            workspace_id,
            swarm_run_id,
            member_id,
        };
        let response: DispatchSwarmTurnResponse = self
            .call_with_custom_timeout(
                authorization,
                Method::POST,
                format!(
                    "/v1/workspaces/{workspace_id}/swarms/{swarm_run_id}/members/{member_id}/turns/dispatch"
                ),
                Some(request),
                Some(readback.clone()),
                ExchangeLimits {
                    max_response_bytes: MAX_SWARM_RESPONSE_BYTES,
                    timeout: exchange_timeout,
                },
            )
            .await?;
        if !valid_dispatch_turn_response(
            &response,
            workspace_id,
            swarm_run_id,
            member_id,
            snapshot_id,
        ) {
            return Err(ClientError::UnknownOutcome { readback });
        }
        Ok(response)
    }

    /// Switch a `SuspendedQuota` member's model or profile at an attempt boundary and requeue it.
    ///
    /// # Errors
    /// Rejects malformed model-switch targets, bounded transport failures, typed
    /// service rejections, or uncertain model-switch outcomes.
    pub async fn switch_swarm_member_model(
        &self,
        authorization: &HeaderValue,
        workspace_id: Uuid,
        swarm_run_id: Uuid,
        member_id: Uuid,
        request: &SwitchSwarmMemberModelRequest,
    ) -> Result<SwarmModelSwitchResponse, ClientError> {
        require_identity(workspace_id)?;
        require_identity(swarm_run_id)?;
        require_identity(member_id)?;
        request
            .to_target()
            .map_err(|_| ClientError::InvalidRequest)?;
        let readback = ReadbackTarget::SwarmMemberMutation {
            workspace_id,
            swarm_run_id,
            member_id,
        };
        let response: SwarmModelSwitchResponse = self
            .call(
                authorization,
                Method::POST,
                format!(
                    "/v1/workspaces/{workspace_id}/swarms/{swarm_run_id}/members/{member_id}/model-switch"
                ),
                Some(request),
                Some(readback.clone()),
            )
            .await?;
        if !valid_swarm_member(&response.member, workspace_id, swarm_run_id, member_id)
            || response.profile_id.is_nil()
            || response.profile_revision <= 0
        {
            return Err(ClientError::UnknownOutcome { readback });
        }
        Ok(response)
    }

    /// Append a `#seq` message to a Swarm channel and fire `@mention` doorbells.
    ///
    /// # Errors
    /// Rejects malformed identities/empty body, bounded transport failures, typed
    /// service rejections, or uncertain message append outcomes.
    pub async fn post_swarm_message(
        &self,
        authorization: &HeaderValue,
        workspace_id: Uuid,
        swarm_run_id: Uuid,
        request: &PostSwarmMessageRequest,
    ) -> Result<PostSwarmMessageResponse, ClientError> {
        require_identity(workspace_id)?;
        require_identity(swarm_run_id)?;
        let message_id = canonical_identity(&request.message_id)?;
        if request.body.trim().is_empty() {
            return Err(ClientError::InvalidRequest);
        }
        let expected_channel_id = request
            .channel_id
            .as_deref()
            .map(canonical_identity)
            .transpose()?;
        let expected_author_id = request
            .author_member_id
            .as_deref()
            .map(canonical_identity)
            .transpose()?;
        let readback = ReadbackTarget::SwarmBoardMessage {
            workspace_id,
            swarm_run_id,
            message_id,
        };
        let response: PostSwarmMessageResponse = self
            .call(
                authorization,
                Method::POST,
                format!("/v1/workspaces/{workspace_id}/swarms/{swarm_run_id}/messages"),
                Some(request),
                Some(readback.clone()),
            )
            .await?;
        if response.message.message_id != message_id
            || response.message.workspace_id != workspace_id
            || response.message.swarm_run_id != swarm_run_id
            || response.message.sequence == 0
            || expected_channel_id.is_some_and(|ch| response.message.channel_id != ch)
            || response.message.author_member_id != expected_author_id
        {
            return Err(ClientError::UnknownOutcome { readback });
        }
        Ok(response)
    }

    /// Incrementally read Swarm board messages ordered by monotonic `sequence`.
    ///
    /// # Errors
    /// Rejects malformed `channel_id` or `limit` bounds, bounded transport failures,
    /// typed service rejections, or misordered/foreign board pages.
    pub async fn read_swarm_messages(
        &self,
        authorization: &HeaderValue,
        workspace_id: Uuid,
        swarm_run_id: Uuid,
        query: &ReadSwarmMessagesQuery,
    ) -> Result<SwarmBoardPageResponse, ClientError> {
        require_identity(workspace_id)?;
        require_identity(swarm_run_id)?;
        let channel_id = query
            .channel_id
            .as_deref()
            .map(canonical_identity)
            .transpose()?;
        if let Some(limit) = query.limit
            && !(1..=MAX_SWARM_BOARD_PAGE_SIZE).contains(&limit)
        {
            return Err(ClientError::InvalidRequest);
        }
        let path = build_read_messages_path(workspace_id, swarm_run_id, query, channel_id);
        let response: SwarmBoardPageResponse = self
            .call::<_, ()>(authorization, Method::GET, path, None, None)
            .await?;
        if !valid_board_page(&response, workspace_id, swarm_run_id, query, channel_id) {
            return Err(ClientError::Protocol);
        }
        Ok(response)
    }

    /// Create an explicit lineage-separated Swarm verification panel.
    ///
    /// # Errors
    /// Rejects malformed identities/quorum, bounded transport failures, typed
    /// service rejections, or uncertain panel creation outcomes.
    pub async fn create_swarm_verification_panel(
        &self,
        authorization: &HeaderValue,
        workspace_id: Uuid,
        swarm_run_id: Uuid,
        request: &CreateSwarmVerificationPanelRequest,
    ) -> Result<SwarmVerificationPanelWire, ClientError> {
        require_identity(workspace_id)?;
        require_identity(swarm_run_id)?;
        let nonce = canonical_identity(&request.request_id)?;
        if request.required_quorum == 0
            || request.author_member_ids.is_empty()
            || request.verifier_member_ids.is_empty()
        {
            return Err(ClientError::InvalidRequest);
        }
        for raw in request
            .author_member_ids
            .iter()
            .chain(&request.verifier_member_ids)
        {
            let _member_id = canonical_identity(raw)?;
        }
        let readback = ReadbackTarget::SwarmVerificationPanel {
            workspace_id,
            swarm_run_id,
            request_id: nonce,
        };
        let response: SwarmVerificationPanelWire = self
            .call(
                authorization,
                Method::POST,
                format!("/v1/workspaces/{workspace_id}/swarms/{swarm_run_id}/panels"),
                Some(request),
                Some(readback.clone()),
            )
            .await?;
        if response.panel_id.is_nil()
            || response.workspace_id != workspace_id
            || response.swarm_run_id != swarm_run_id
            || response.required_quorum != request.required_quorum
        {
            return Err(ClientError::UnknownOutcome { readback });
        }
        Ok(response)
    }

    /// Record an immutable diff-digest-bound verdict from a lineage-separated `Verifier`.
    ///
    /// # Errors
    /// Rejects malformed identities/digests, bounded transport failures, typed
    /// service rejections, or uncertain verdict recording outcomes.
    pub async fn record_swarm_verdict(
        &self,
        authorization: &HeaderValue,
        workspace_id: Uuid,
        swarm_run_id: Uuid,
        request: &RecordSwarmVerdictRequest,
    ) -> Result<SwarmVerdictWire, ClientError> {
        require_identity(workspace_id)?;
        require_identity(swarm_run_id)?;
        let nonce = canonical_identity(&request.request_id)?;
        let verifier_id = canonical_identity(&request.verifier_member_id)?;
        let expected_panel_id = request
            .panel_id
            .as_deref()
            .map(canonical_identity)
            .transpose()?;
        parse_review_verdict_outcome(&request.outcome).map_err(|_| ClientError::InvalidRequest)?;
        parse_non_zero_digest_hex(&request.reviewed_diff_digest_hex)
            .map_err(|_| ClientError::InvalidRequest)?;
        if request.rationale.trim().is_empty() {
            return Err(ClientError::InvalidRequest);
        }
        let readback = ReadbackTarget::SwarmVerdict {
            workspace_id,
            swarm_run_id,
            request_id: nonce,
        };
        let response: SwarmVerdictWire = self
            .call(
                authorization,
                Method::POST,
                format!("/v1/workspaces/{workspace_id}/swarms/{swarm_run_id}/verdicts"),
                Some(request),
                Some(readback.clone()),
            )
            .await?;
        if response.verdict_id.is_nil()
            || response.request_id != nonce
            || response.swarm_run_id != swarm_run_id
            || response.verifier_member_id != verifier_id
            || expected_panel_id.is_some_and(|pid| response.panel_id != pid)
        {
            return Err(ClientError::UnknownOutcome { readback });
        }
        Ok(response)
    }

    /// Converge a Swarm run onto candidate digests, verify quorum, and settle if `Standalone`.
    ///
    /// # Errors
    /// Rejects malformed commit/tree/diff digests, bounded transport failures,
    /// typed service rejections, or uncertain convergence outcomes.
    pub async fn converge_swarm_run(
        &self,
        authorization: &HeaderValue,
        workspace_id: Uuid,
        swarm_run_id: Uuid,
        request: &ConvergeSwarmRequest,
    ) -> Result<ConvergeSwarmResponse, ClientError> {
        require_identity(workspace_id)?;
        require_identity(swarm_run_id)?;
        let nonce = validate_converge_request(request)?;
        let readback = ReadbackTarget::SwarmRunMutation {
            workspace_id,
            swarm_run_id,
            request_id: Some(nonce),
        };
        let response: ConvergeSwarmResponse = self
            .call(
                authorization,
                Method::POST,
                format!("/v1/workspaces/{workspace_id}/swarms/{swarm_run_id}/converge"),
                Some(request),
                Some(readback.clone()),
            )
            .await?;
        if response.run.swarm_run_id != swarm_run_id
            || !valid_swarm_run(&response.run, workspace_id)
        {
            return Err(ClientError::UnknownOutcome { readback });
        }
        Ok(response)
    }

    /// Explicitly settle an already-staged `CandidateReady` `Standalone` Swarm run.
    ///
    /// # Errors
    /// Rejects malformed identities, bounded transport failures, typed service
    /// rejections, or uncertain settlement outcomes.
    pub async fn settle_swarm_run(
        &self,
        authorization: &HeaderValue,
        workspace_id: Uuid,
        swarm_run_id: Uuid,
        request: &SettleSwarmRequest,
    ) -> Result<ConvergeSwarmResponse, ClientError> {
        require_identity(workspace_id)?;
        require_identity(swarm_run_id)?;
        let nonce = canonical_identity(&request.request_id)?;
        let readback = ReadbackTarget::SwarmRunMutation {
            workspace_id,
            swarm_run_id,
            request_id: Some(nonce),
        };
        let response: ConvergeSwarmResponse = self
            .call(
                authorization,
                Method::POST,
                format!("/v1/workspaces/{workspace_id}/swarms/{swarm_run_id}/settle"),
                Some(request),
                Some(readback.clone()),
            )
            .await?;
        if !response.settled
            || response.run.swarm_run_id != swarm_run_id
            || !valid_swarm_run(&response.run, workspace_id)
        {
            return Err(ClientError::UnknownOutcome { readback });
        }
        Ok(response)
    }

    /// Abort an active or candidate Swarm run.
    ///
    /// # Errors
    /// Rejects nil identities, bounded transport failures, typed service rejections,
    /// or uncertain abort outcomes.
    pub async fn abort_swarm_run(
        &self,
        authorization: &HeaderValue,
        workspace_id: Uuid,
        swarm_run_id: Uuid,
        request: &AbortSwarmRunRequest,
    ) -> Result<SwarmRunWire, ClientError> {
        require_identity(workspace_id)?;
        require_identity(swarm_run_id)?;
        let readback = ReadbackTarget::SwarmRunMutation {
            workspace_id,
            swarm_run_id,
            request_id: None,
        };
        let response: SwarmRunWire = self
            .call(
                authorization,
                Method::POST,
                format!("/v1/workspaces/{workspace_id}/swarms/{swarm_run_id}/abort"),
                Some(request),
                Some(readback.clone()),
            )
            .await?;
        if response.swarm_run_id != swarm_run_id || !valid_swarm_run(&response, workspace_id) {
            return Err(ClientError::UnknownOutcome { readback });
        }
        Ok(response)
    }

    /// Evaluate and remediate deterministic pure-Rust Stall & Livelock conditions.
    ///
    /// # Errors
    /// Rejects negative `observed_at_ms` or zero policy thresholds, bounded transport
    /// failures, typed service rejections, or uncertain remediation outcomes.
    pub async fn evaluate_swarm_stall_check(
        &self,
        authorization: &HeaderValue,
        workspace_id: Uuid,
        swarm_run_id: Uuid,
        request: &SwarmStallCheckRequest,
    ) -> Result<SwarmStallCheckResponse, ClientError> {
        require_identity(workspace_id)?;
        require_identity(swarm_run_id)?;
        if request.observed_at_ms.is_some_and(|ts| ts < 0)
            || request.stall_grace_ms.is_some_and(|ms| ms <= 0)
            || request.max_ping_pong_streak == Some(0)
            || request.max_member_turns == Some(0)
        {
            return Err(ClientError::InvalidRequest);
        }
        let readback = ReadbackTarget::SwarmRunMutation {
            workspace_id,
            swarm_run_id,
            request_id: None,
        };
        let response: SwarmStallCheckResponse = self
            .call(
                authorization,
                Method::POST,
                format!("/v1/workspaces/{workspace_id}/swarms/{swarm_run_id}/stall-check"),
                Some(request),
                Some(readback.clone()),
            )
            .await?;
        if response.action.is_empty()
            || response.run.as_ref().is_some_and(|run| {
                run.swarm_run_id != swarm_run_id || !valid_swarm_run(run, workspace_id)
            })
        {
            return Err(ClientError::UnknownOutcome { readback });
        }
        Ok(response)
    }

    /// Converge captured Swarm member candidates via Forge 3-way merge and stage `CandidateReady`.
    ///
    /// # Errors
    /// Rejects malformed identities, bounded transport failures, typed service rejections,
    /// or uncertain convergence outcomes.
    pub async fn forge_converge_swarm_run(
        &self,
        authorization: &HeaderValue,
        workspace_id: Uuid,
        swarm_run_id: Uuid,
        request: &ForgeConvergeSwarmRequest,
    ) -> Result<ForgeConvergeSwarmResponse, ClientError> {
        require_identity(workspace_id)?;
        require_identity(swarm_run_id)?;
        let nonce = canonical_identity(&request.request_id)?;
        let _session_id = canonical_identity(&request.session_id)?;
        for raw_member in &request.candidate_member_ids {
            let _member_id = canonical_identity(raw_member)?;
        }
        let readback = ReadbackTarget::SwarmRunMutation {
            workspace_id,
            swarm_run_id,
            request_id: Some(nonce),
        };
        let response: ForgeConvergeSwarmResponse = self
            .call(
                authorization,
                Method::POST,
                format!("/v1/workspaces/{workspace_id}/swarms/{swarm_run_id}/forge-converge"),
                Some(request),
                Some(readback.clone()),
            )
            .await?;
        if response.run.swarm_run_id != swarm_run_id
            || !valid_swarm_run(&response.run, workspace_id)
        {
            return Err(ClientError::UnknownOutcome { readback });
        }
        Ok(response)
    }

    /// Verify the `Verifier` quorum gate and settle a `Standalone` Swarm run across Forge and Orchestration.
    ///
    /// # Errors
    /// Rejects malformed identities, bounded transport failures, typed service rejections,
    /// or uncertain settlement outcomes.
    pub async fn forge_settle_swarm_run(
        &self,
        authorization: &HeaderValue,
        workspace_id: Uuid,
        swarm_run_id: Uuid,
        request: &ForgeSettleSwarmRequest,
    ) -> Result<ForgeSettleSwarmResponse, ClientError> {
        require_identity(workspace_id)?;
        require_identity(swarm_run_id)?;
        let nonce = canonical_identity(&request.request_id)?;
        let _session_id = canonical_identity(&request.session_id)?;
        let _coordinator_agent_id = canonical_identity(&request.coordinator_agent_id)?;
        if let Some(raw_activity) = &request.activity_id {
            let _activity_id = canonical_identity(raw_activity)?;
        }
        if let Some(raw_migration) = &request.outbox_migration_id {
            let _migration_id = canonical_identity(raw_migration)?;
        }
        let readback = ReadbackTarget::SwarmRunMutation {
            workspace_id,
            swarm_run_id,
            request_id: Some(nonce),
        };
        let response: ForgeSettleSwarmResponse = self
            .call(
                authorization,
                Method::POST,
                format!("/v1/workspaces/{workspace_id}/swarms/{swarm_run_id}/forge-settle"),
                Some(request),
                Some(readback.clone()),
            )
            .await?;
        if response.settlement_id != nonce
            || response.run.swarm_run_id != swarm_run_id
            || !valid_swarm_run(&response.run, workspace_id)
        {
            return Err(ClientError::UnknownOutcome { readback });
        }
        Ok(response)
    }

    /// Read one bounded page from Forge's autonomy outbox and reconcile `DAG_SETTLED_KIND = 5`
    /// and `SWARM_SETTLED_KIND = 6` facts in Orchestration.
    ///
    /// # Errors
    /// Rejects malformed identities or out-of-range limits (`0` or `> 64`), bounded transport
    /// failures, typed service rejections, or regressive sequence responses.
    pub async fn sync_autonomy_outbox(
        &self,
        authorization: &HeaderValue,
        workspace_id: Uuid,
        request: &SyncAutonomyOutboxRequest,
    ) -> Result<SyncAutonomyOutboxResponse, ClientError> {
        require_identity(workspace_id)?;
        let _migration_id = canonical_identity(&request.migration_id)?;
        if let Some(limit) = request.limit
            && !(1..=64).contains(&limit)
        {
            return Err(ClientError::InvalidRequest);
        }
        let response: SyncAutonomyOutboxResponse = self
            .call(
                authorization,
                Method::POST,
                format!("/internal/v1/workspaces/{workspace_id}/autonomy-outbox/sync"),
                Some(request),
                None,
            )
            .await?;
        if response.last_sequence < request.after_sequence {
            return Err(ClientError::Protocol);
        }
        Ok(response)
    }
}

fn validate_create_swarm_request(
    workspace_id: Uuid,
    request: &CreateSwarmRunRequest,
) -> Result<Uuid, ClientError> {
    let nonce = canonical_identity(&request.request_id)?;
    if request.title.trim().is_empty()
        || request.max_members == 0
        || request.max_total_turns == 0
        || request.required_verifier_quorum == 0
    {
        return Err(ClientError::InvalidRequest);
    }
    if let Some(scope) = &request.scope
        && (scope.workspace_id != workspace_id || scope.location_id.is_nil())
    {
        return Err(ClientError::InvalidRequest);
    }
    request
        .mode
        .to_domain()
        .map_err(|_| ClientError::InvalidRequest)?;
    parse_swarm_commit_oid(&request.baseline_commit_oid)
        .map_err(|_| ClientError::InvalidRequest)?;
    parse_non_zero_digest_hex(&request.baseline_tree_blake3_hex)
        .map_err(|_| ClientError::InvalidRequest)?;
    Ok(nonce)
}

fn validate_add_member_request(request: &AddSwarmMemberRequest) -> Result<Uuid, ClientError> {
    let member_id = canonical_identity(&request.member_id)?;
    if request.roster_name.trim().is_empty() {
        return Err(ClientError::InvalidRequest);
    }
    parse_swarm_role_kind(&request.role_kind).map_err(|_| ClientError::InvalidRequest)?;
    parse_swarm_workspace_mode(&request.workspace_mode).map_err(|_| ClientError::InvalidRequest)?;
    if let Some(parent) = &request.parent_member_id {
        let _parent_id = canonical_identity(parent)?;
    }
    request
        .identity
        .to_author_identity()
        .map_err(|_| ClientError::InvalidRequest)?;
    Ok(member_id)
}

fn validate_converge_request(request: &ConvergeSwarmRequest) -> Result<Uuid, ClientError> {
    let nonce = canonical_identity(&request.request_id)?;
    parse_swarm_commit_oid(&request.converged_commit_oid)
        .map_err(|_| ClientError::InvalidRequest)?;
    parse_non_zero_digest_hex(&request.converged_tree_blake3_hex)
        .map_err(|_| ClientError::InvalidRequest)?;
    parse_non_zero_digest_hex(&request.converged_diff_digest_hex)
        .map_err(|_| ClientError::InvalidRequest)?;
    if let Some(target_raw) = &request.target_state {
        let target =
            parse_swarm_convergence_target(target_raw).map_err(|_| ClientError::InvalidRequest)?;
        if request.stage_candidate_only
            && target == crate::wire::swarm::SwarmConvergenceTarget::Settled
        {
            return Err(ClientError::InvalidRequest);
        }
    }
    Ok(nonce)
}

fn build_read_messages_path(
    workspace_id: Uuid,
    swarm_run_id: Uuid,
    query: &ReadSwarmMessagesQuery,
    channel_id: Option<Uuid>,
) -> String {
    let mut params = Vec::with_capacity(3);
    if let Some(since) = query.since_sequence {
        params.push(format!("sinceSequence={since}"));
    }
    if let Some(ch) = channel_id {
        params.push(format!("channelId={ch}"));
    }
    if let Some(limit) = query.limit {
        params.push(format!("limit={limit}"));
    }
    let base = format!("/v1/workspaces/{workspace_id}/swarms/{swarm_run_id}/messages");
    if params.is_empty() {
        base
    } else {
        format!("{base}?{}", params.join("&"))
    }
}

fn valid_board_page(
    page: &SwarmBoardPageResponse,
    workspace_id: Uuid,
    swarm_run_id: Uuid,
    query: &ReadSwarmMessagesQuery,
    expected_channel_id: Option<Uuid>,
) -> bool {
    let max_len = query.limit.unwrap_or(MAX_SWARM_BOARD_PAGE_SIZE);
    if page.messages.len() > max_len {
        return false;
    }
    let since = query.since_sequence.unwrap_or(0);
    page.messages.iter().all(|msg| {
        !msg.message_id.is_nil()
            && msg.workspace_id == workspace_id
            && msg.swarm_run_id == swarm_run_id
            && msg.sequence > since
            && expected_channel_id.is_none_or(|ch| msg.channel_id == ch)
    }) && page
        .messages
        .windows(2)
        .all(|pair| pair[0].sequence < pair[1].sequence)
}

fn valid_swarm_run(run: &SwarmRunWire, workspace_id: Uuid) -> bool {
    !run.swarm_run_id.is_nil()
        && run.workspace_id == workspace_id
        && !run.title.is_empty()
        && run.revision > 0
        && run.created_at_ms >= 0
        && run.updated_at_ms >= run.created_at_ms
}

fn valid_swarm_member(
    member: &SwarmMemberWire,
    workspace_id: Uuid,
    swarm_run_id: Uuid,
    member_id: Uuid,
) -> bool {
    member.member_id == member_id
        && member.workspace_id == workspace_id
        && member.swarm_run_id == swarm_run_id
        && !member.roster_name.is_empty()
        && !member.member_state.is_empty()
}

fn validate_dispatch_turn_request(
    request: &DispatchSwarmTurnRequest,
) -> Result<(Uuid, Duration), ClientError> {
    let _snapshot_request_id = canonical_identity(&request.snapshot_request_id)?;
    let snapshot_id = canonical_identity(&request.snapshot_id)?;
    let _activity_id = canonical_identity(&request.activity_id)?;
    let _lease_id = canonical_identity(&request.lease_id)?;
    if let Some(raw_session_id) = &request.forge_session_id {
        let _forge_session_id = canonical_identity(raw_session_id)?;
    }
    if (request.default_worktree_path.trim().is_empty() && request.forge_session_id.is_none())
        || request.lease_revision == 0
        || i64::try_from(request.lease_revision).is_err()
        || request.lease_expires_at_ms < 0
        || request.dispatched_at_ms.is_some_and(|ts| ts < 0)
    {
        return Err(ClientError::InvalidRequest);
    }
    if let Some(grant) = &request.mcp_grant {
        let _token_id = canonical_identity(&grant.token_id)?;
        if grant.issued_at_ms < 0 || grant.expires_at_ms <= grant.issued_at_ms {
            return Err(ClientError::InvalidRequest);
        }
    }
    let timeouts = ExecutionTimeouts::new(
        request.timeouts.startup_timeout_ms,
        request.timeouts.execution_timeout_ms,
        request.timeouts.termination_grace_ms,
    )
    .map_err(|_| ClientError::InvalidRequest)?;
    match (
        &request.clean_exit_output_commit_oid,
        &request.clean_exit_output_tree_blake3_hex,
    ) {
        (None, None) => {}
        (Some(oid), Some(tree_hex)) => {
            parse_swarm_commit_oid(oid).map_err(|_| ClientError::InvalidRequest)?;
            parse_non_zero_digest_hex(tree_hex).map_err(|_| ClientError::InvalidRequest)?;
        }
        _ => return Err(ClientError::InvalidRequest),
    }
    let total_ms = timeouts
        .startup_timeout_ms()
        .saturating_add(timeouts.execution_timeout_ms())
        .saturating_add(timeouts.termination_grace_ms())
        .saturating_add(SWARM_DISPATCH_SLACK_MS);
    Ok((snapshot_id, Duration::from_millis(total_ms)))
}

fn valid_dispatch_turn_response(
    response: &DispatchSwarmTurnResponse,
    workspace_id: Uuid,
    swarm_run_id: Uuid,
    member_id: Uuid,
    snapshot_id: Uuid,
) -> bool {
    response.wakeup_batch.swarm_run_id == swarm_run_id
        && response.wakeup_batch.snapshot_id == snapshot_id
        && response.committed_snapshot.snapshot_id == snapshot_id
        && response.committed_snapshot.workspace_id == workspace_id
        && response.supervision.ledger.snapshot_id == snapshot_id
        && response.supervision.ledger.workspace_id == workspace_id
        && !response.supervision.status.is_empty()
        && !response.disposition.kind.is_empty()
        && valid_swarm_member(
            &response.wakeup_batch.member,
            workspace_id,
            swarm_run_id,
            member_id,
        )
        && valid_swarm_member(
            &response.disposition.member,
            workspace_id,
            swarm_run_id,
            member_id,
        )
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::time::Duration;

    use hyper::header::HeaderValue;

    use super::*;
    use crate::wire::swarm::SwarmRunModeWire;

    #[tokio::test]
    async fn create_swarm_run_validates_digests_and_retains_readback_on_loss()
    -> Result<(), Box<dyn Error>> {
        let client =
            OrchestrationHttpClient::new("127.0.0.1:1".parse()?, Duration::from_millis(50))?;
        let auth = HeaderValue::from_static("Bearer test-operator");
        let ws = Uuid::from_u128(0x101);
        let req_id = Uuid::from_u128(0x202);

        let mut req = CreateSwarmRunRequest {
            request_id: req_id.to_string(),
            title: "Swarm Client Test".to_owned(),
            mode: SwarmRunModeWire {
                kind: "STANDALONE".to_owned(),
                hybrid_target: None,
            },
            max_members: 8,
            max_spawn_depth: 2,
            max_total_turns: 32,
            required_verifier_quorum: 1,
            allow_worker_all_mention: false,
            baseline_commit_oid: "1".repeat(40),
            baseline_tree_blake3_hex: "00".repeat(32),
            scope: None,
        };
        assert_eq!(
            client.create_swarm_run(&auth, ws, &req).await,
            Err(ClientError::InvalidRequest)
        );

        req.baseline_tree_blake3_hex = "a1".repeat(32);
        assert_eq!(
            client.create_swarm_run(&auth, ws, &req).await,
            Err(ClientError::UnknownOutcome {
                readback: ReadbackTarget::SwarmRunCreate {
                    workspace_id: ws,
                    request_id: req_id,
                }
            })
        );
        Ok(())
    }
}
