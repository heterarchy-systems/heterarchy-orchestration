//! Thin Broker `authority.record` fence adapter for leadership confirmation.
//!
//! This module owns fixed-three-node routing, timeout, identifier projection,
//! and error classification only — no product-leadership policy lives here.
//! One call maps one prepared
//! [`LeadershipFenceRequest`](crate::leadership::LeadershipFenceRequest) onto
//! one protocol-v3 `authority.record` operation. Production routing probes all
//! three Broker operations endpoints concurrently, requires exactly one
//! write-ready node, and preserves the exact request frame across bounded
//! rediscovery.
//!
//! - `Ok`: the Broker durably recorded (or had already recorded, idempotent
//!   rule-2 readback) the exact requested revision.
//! - `Stale`: exact-request proof that the tuple is currently excluded. It
//!   does not establish whether an earlier attempt was recorded.
//! - `Conflict`: no exclusion proof; preserve the intent for reconciliation.
//! - `Unconfirmed`: absent/ambiguous Broker authority, transport failure,
//!   timeout, or an unresolved Broker disposition. The durable intent stays
//!   resumable and the surfaced error is fail-closed.
//!
//! The adapter does not invent retry identity. Static-cluster rediscovery
//! resends the same encoded frame; logical retry policy stays with the durable
//! intent journal in the operations catalog.
use std::time::Duration;

use agent_broker_application::BrokerErrorCode;
use agent_broker_client::{
    AsyncStaticClusterRouter, ClientError, StaticClusterNode, StaticClusterRoutingError,
};
use agent_broker_domain::{AuthorityFenceKey, AuthorityId, AuthorityOwnerId, AuthorityRevision};
use agent_broker_protocol::RequestId;
use thiserror::Error;

use crate::OperationsError;
use crate::broker_cluster::{BrokerClusterConfig, DEFAULT_BROKER_CLUSTER_TIMEOUT};
use crate::leadership::{
    LeadershipFenceExclusionProof, LeadershipFenceOutcome, LeadershipFenceRequest,
};

/// Historical leadership-fence deadline retained for source compatibility.
pub const DEFAULT_FENCE_TIMEOUT: Duration = DEFAULT_BROKER_CLUSTER_TIMEOUT;

/// Source-compatible leadership-fence configuration.
///
/// The actual router configuration is canonicalized through
/// [`BrokerClusterConfig`]. This wrapper only preserves the pre-existing
/// public `{ nodes, timeout }` construction surface.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BrokerFenceConfig {
    pub nodes: [StaticClusterNode; 3],
    pub timeout: Duration,
}

impl Default for BrokerFenceConfig {
    fn default() -> Self {
        let cluster = BrokerClusterConfig::default();
        Self {
            nodes: cluster.nodes,
            timeout: DEFAULT_FENCE_TIMEOUT,
        }
    }
}

impl From<BrokerFenceConfig> for BrokerClusterConfig {
    fn from(config: BrokerFenceConfig) -> Self {
        Self {
            nodes: config.nodes,
            timeout: config.timeout,
        }
    }
}

/// Bounded outcome of one attempted authority fence.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum LeadershipFenceError {
    #[error("Broker authority fence was superseded by a newer authority (STALE_FENCE)")]
    Stale(LeadershipFenceExclusionProof),
    #[error("Broker authority fence violated the recorded replay binding (CONFLICT)")]
    Conflict,
    #[error(
        "Broker authority fence outcome is unknown: Broker write authority is absent or ambiguous, transport failed, timed out, or left the disposition unresolved"
    )]
    Unconfirmed,
}

/// Production fence executor over the native async fixed-three-node Broker
/// router. No catalog lock is held across its network work.
pub struct BrokerAuthorityFence {
    router: AsyncStaticClusterRouter,
}

impl BrokerAuthorityFence {
    /// Compose the production fixed-three-node fence executor.
    ///
    /// # Errors
    /// Rejects invalid cluster topology, endpoint, timeout, or frame bounds.
    pub fn new(config: impl Into<BrokerClusterConfig>) -> Result<Self, OperationsError> {
        let router = AsyncStaticClusterRouter::new(config.into().router_config())
            .map_err(|_| OperationsError::Conflict("leadership fence cluster"))?;
        Ok(Self { router })
    }

    /// Perform one `authority.record` fence for the exact prepared request.
    /// The correlation ID is derived deterministically from the durable intent
    /// `request_id`; cluster rediscovery therefore preserves the same logical
    /// request and same encoded mutation frame.
    ///
    /// # Errors
    /// Returns the bounded [`LeadershipFenceError`] classification and never
    /// mutates Orchestration catalog state.
    pub async fn record(
        &self,
        request: &LeadershipFenceRequest,
    ) -> Result<LeadershipFenceOutcome, LeadershipFenceError> {
        let revision =
            u64::try_from(request.revision()).map_err(|_| LeadershipFenceError::Unconfirmed)?;
        let request_id = RequestId::new(format!("lc-{}", request.request_id().hyphenated()))
            .map_err(|_| LeadershipFenceError::Unconfirmed)?;
        let key = AuthorityFenceKey::new(request.key().to_owned())
            .map_err(|_| LeadershipFenceError::Unconfirmed)?;
        let revision =
            AuthorityRevision::new(revision).map_err(|_| LeadershipFenceError::Unconfirmed)?;
        let authority_id = AuthorityId::new(request.authority_id().hyphenated().to_string())
            .map_err(|_| LeadershipFenceError::Unconfirmed)?;
        let owner_instance_id = AuthorityOwnerId::new(request.owner_instance_id().to_owned())
            .map_err(|_| LeadershipFenceError::Unconfirmed)?;

        let recorded = self
            .router
            .authority_record_with_request_id(
                request_id,
                key,
                revision,
                authority_id,
                owner_instance_id,
            )
            .await
            .map_err(|error| classify_routing_error(error, request))?;
        let recorded_revision =
            i64::try_from(recorded.get()).map_err(|_| LeadershipFenceError::Unconfirmed)?;
        Ok(LeadershipFenceOutcome::from_request(
            request,
            recorded_revision,
        ))
    }
}

fn classify_routing_error(
    error: StaticClusterRoutingError,
    request: &LeadershipFenceRequest,
) -> LeadershipFenceError {
    match error {
        StaticClusterRoutingError::Client(error) => classify_client_error(error, request),
        StaticClusterRoutingError::InvalidConfiguration(_)
        | StaticClusterRoutingError::InvalidOperationsResponse { .. }
        | StaticClusterRoutingError::NoWriteReadyLeader
        | StaticClusterRoutingError::MultipleWriteReadyLeaders(_) => {
            LeadershipFenceError::Unconfirmed
        }
    }
}

fn classify_client_error(
    error: ClientError,
    request: &LeadershipFenceRequest,
) -> LeadershipFenceError {
    match error {
        ClientError::Broker(broker_error) => match broker_error.code() {
            BrokerErrorCode::StaleFence => {
                LeadershipFenceError::Stale(LeadershipFenceExclusionProof::stale_for(request))
            }
            BrokerErrorCode::Conflict => LeadershipFenceError::Conflict,
            // Other Broker conditions are not contract rejections of this
            // authority attempt. Preserve the durable intent for exact retry.
            _ => LeadershipFenceError::Unconfirmed,
        },
        ClientError::Transport(_)
        | ClientError::Protocol(_)
        | ClientError::UnexpectedPayload(_)
        | ClientError::RequestIdExhausted => LeadershipFenceError::Unconfirmed,
    }
}
