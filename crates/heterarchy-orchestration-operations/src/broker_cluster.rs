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
    /// Build a single-node Broker routing configuration from one Broker address.
    ///
    /// When no explicit operations port is provided, defaults to `port + 1000`
    /// (or `port + 1` if `port + 1000` overflows `u16`). Single-node routing
    /// communicates directly with `broker_address` via [`agent_broker_client::AsyncBrokerClient`].
    ///
    /// # Errors
    /// Returns [`BrokerClusterProbeError::InvalidAddress`] when the Broker
    /// client rejects the address.
    pub fn from_broker_address(
        broker_address: SocketAddr,
    ) -> Result<Self, BrokerClusterProbeError> {
        let default_ops_port = broker_address
            .port()
            .checked_add(1_000)
            .or_else(|| broker_address.port().checked_add(1))
            .unwrap_or(broker_address.port());
        let operations_address = SocketAddr::new(broker_address.ip(), default_ops_port);
        Self::from_single_node(broker_address, operations_address)
    }

    /// Build a single-node Broker routing configuration from one Broker/Operations
    /// address pair.
    ///
    /// # Errors
    /// Returns [`BrokerClusterProbeError::InvalidAddress`] when the Broker
    /// client rejects the address.
    pub fn from_single_node(
        broker_address: SocketAddr,
        operations_address: SocketAddr,
    ) -> Result<Self, BrokerClusterProbeError> {
        let node = StaticClusterNode {
            node_id: 1,
            broker_address,
            operations_address,
        };
        Self {
            nodes: [node, node, node],
            timeout: DEFAULT_BROKER_CLUSTER_TIMEOUT,
        }
        .validated()
    }

    /// Return whether this configuration targets a single Broker node rather
    /// than a three-node cluster.
    #[must_use]
    pub fn is_single_node(&self) -> bool {
        self.nodes[0].broker_address == self.nodes[1].broker_address
            && self.nodes[1].broker_address == self.nodes[2].broker_address
            && self.nodes[0].operations_address == self.nodes[1].operations_address
            && self.nodes[1].operations_address == self.nodes[2].operations_address
    }

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
        if pairs[0] == pairs[1] && pairs[1] == pairs[2] {
            return Self::from_single_node(pairs[0].0, pairs[0].1);
        }
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

    pub(crate) fn validated(self) -> Result<Self, BrokerClusterProbeError> {
        if self.is_single_node() {
            if !self.nodes[0].operations_address.ip().is_loopback()
                || self.nodes[0].operations_address.port() == 0
            {
                return Err(BrokerClusterProbeError::InvalidAddress);
            }
            self.client_config()
                .validate()
                .map_err(|_| BrokerClusterProbeError::InvalidAddress)?;
            return Ok(self);
        }
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

    pub(crate) fn client_config(self) -> BrokerClientConfig {
        BrokerClientConfig {
            address: self.nodes[0].broker_address,
            timeout: self.timeout,
            max_response_frame_bytes: MAX_RESPONSE_FRAME_BYTES,
        }
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
        if self.is_single_node() {
            let node = self.nodes[0];
            let broker_reachable = tcp_reachable(node.broker_address);
            let operations_reachable = tcp_reachable(node.operations_address);
            let health_ok = broker_reachable && Self::node_health(node.broker_address).is_ok();
            let single = BrokerClusterNodeProbe {
                node_id: node.node_id,
                broker_address: node.broker_address.to_string(),
                operations_address: node.operations_address.to_string(),
                broker_reachable,
                operations_reachable,
                health_ok,
            };
            return BrokerClusterProbe {
                healthy: health_ok,
                nodes: [single.clone(), single.clone(), single],
            };
        }
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
