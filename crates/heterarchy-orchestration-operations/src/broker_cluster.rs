//! Shared fixed-three-node Broker routing configuration for Orchestration.
//!
//! Product policy does not live here. This module only keeps the physical
//! Broker topology and native async-router bounds identical for every
//! Orchestration authority adapter that depends on Broker write readiness.

use std::net::{SocketAddr, TcpStream};
use std::time::Duration;

use agent_broker_client::{
    BrokerClient, BrokerClientConfig, StaticClusterNode, StaticClusterRouterConfig,
    StaticClusterRoutingRetryPolicy,
};
use thiserror::Error;

/// Host-owned Broker routing deadline shared by owner and leadership fences.
pub const DEFAULT_BROKER_CLUSTER_TIMEOUT: Duration = Duration::from_secs(5);
/// Response frame bound matching the Broker runtime default.
pub(crate) const MAX_RESPONSE_FRAME_BYTES: usize = 128 * 1024;
/// Per-address reachability deadline used by the operator-facing probe.
pub const BROKER_PROBE_TIMEOUT: Duration = Duration::from_millis(1_500);

/// Bounded fixed-three-node Broker routing configuration.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BrokerClusterConfig {
    pub nodes: [StaticClusterNode; 3],
    pub timeout: Duration,
}

impl Default for BrokerClusterConfig {
    fn default() -> Self {
        let cluster = StaticClusterRouterConfig::default();
        Self {
            nodes: cluster.nodes,
            timeout: DEFAULT_BROKER_CLUSTER_TIMEOUT,
        }
    }
}

impl BrokerClusterConfig {
    /// Build the routing configuration from per-node Broker/Operations
    /// address pairs. Address policy (loopback-only today) stays with the
    /// Broker client's own validation.
    ///
    /// # Errors
    /// Returns [`BrokerClusterProbeError::InvalidAddress`] when the Broker
    /// client rejects the resolved topology.
    pub fn from_node_pairs(
        pairs: [(SocketAddr, SocketAddr); 3],
    ) -> Result<Self, BrokerClusterProbeError> {
        let nodes = [
            StaticClusterNode {
                node_id: 1,
                broker_address: pairs[0].0,
                operations_address: pairs[0].1,
            },
            StaticClusterNode {
                node_id: 2,
                broker_address: pairs[1].0,
                operations_address: pairs[1].1,
            },
            StaticClusterNode {
                node_id: 3,
                broker_address: pairs[2].0,
                operations_address: pairs[2].1,
            },
        ];
        Self {
            nodes,
            timeout: DEFAULT_BROKER_CLUSTER_TIMEOUT,
        }
        .validated()
    }

    fn validated(self) -> Result<Self, BrokerClusterProbeError> {
        StaticClusterRouterConfig {
            nodes: self.nodes,
            timeout: self.timeout,
            max_response_frame_bytes: MAX_RESPONSE_FRAME_BYTES,
            retry_policy: StaticClusterRoutingRetryPolicy::default(),
        }
        .validate()
        .map_err(|_| BrokerClusterProbeError::InvalidAddress)?;
        Ok(self)
    }

    pub(crate) fn router_config(self) -> StaticClusterRouterConfig {
        StaticClusterRouterConfig {
            nodes: self.nodes,
            timeout: self.timeout,
            max_response_frame_bytes: MAX_RESPONSE_FRAME_BYTES,
            retry_policy: StaticClusterRoutingRetryPolicy::default(),
        }
    }

    /// Probe every node: connect both addresses, then present a protocol
    /// Health request to the Broker surface. This is observational only and
    /// never acquires authority.
    #[must_use]
    pub fn probe(self) -> BrokerClusterProbe {
        let nodes = self.nodes.map(|node| {
            let broker_reachable = tcp_reachable(node.broker_address);
            let operations_reachable = tcp_reachable(node.operations_address);
            let health_ok = broker_reachable && Self::node_health(node.broker_address).is_ok();
            BrokerClusterNodeProbe {
                node_id: node.node_id,
                broker_address: node.broker_address.to_string(),
                operations_address: node.operations_address.to_string(),
                broker_reachable,
                operations_reachable,
                health_ok,
            }
        });
        BrokerClusterProbe {
            healthy: nodes.iter().all(|node| node.health_ok),
            nodes,
        }
    }

    fn node_health(address: std::net::SocketAddr) -> Result<(), BrokerClusterProbeError> {
        let mut client = BrokerClient::new(
            BrokerClientConfig {
                address,
                timeout: BROKER_PROBE_TIMEOUT,
                max_response_frame_bytes: MAX_RESPONSE_FRAME_BYTES,
            }
            .validate()
            .map_err(|_| BrokerClusterProbeError::InvalidAddress)?,
        )
        .map_err(|_| BrokerClusterProbeError::Unreachable)?;
        client
            .health()
            .map(|_| ())
            .map_err(|_| BrokerClusterProbeError::Unreachable)
    }
}

fn tcp_reachable(address: SocketAddr) -> bool {
    TcpStream::connect_timeout(&address, BROKER_PROBE_TIMEOUT).is_ok()
}

/// One node's observational probe result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrokerClusterNodeProbe {
    pub node_id: u64,
    pub broker_address: String,
    pub operations_address: String,
    pub broker_reachable: bool,
    pub operations_reachable: bool,
    pub health_ok: bool,
}

/// Observational probe result for the whole fixed-three-node cluster.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrokerClusterProbe {
    pub healthy: bool,
    pub nodes: [BrokerClusterNodeProbe; 3],
}

/// Failures raised while validating or probing a configured cluster.
#[derive(Debug, Error)]
pub enum BrokerClusterProbeError {
    #[error("Broker cluster address is outside the Broker client address policy")]
    InvalidAddress,
    #[error("Broker cluster node is unreachable")]
    Unreachable,
}
