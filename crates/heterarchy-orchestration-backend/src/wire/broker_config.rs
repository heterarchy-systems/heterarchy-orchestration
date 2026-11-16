//! HTTP wire contracts for inspecting and updating the Broker connection.

use heterarchy_orchestration_operations::{BrokerClusterConfig, BrokerClusterProbe};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct BrokerNodeAddressWire {
    pub broker_address: String,
    #[serde(default)]
    pub operations_address: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct BrokerConfigUpdateRequest {
    /// Single-node Broker address (e.g. `"127.0.0.1:8811"`).
    /// Mutually exclusive with `nodes`.
    #[serde(default)]
    pub broker_address: Option<String>,
    /// Optional single-node operations address (defaults to `port + 1000`).
    #[serde(default)]
    pub operations_address: Option<String>,
    /// Full three-node Broker cluster addresses. Mutually exclusive with `broker_address`.
    #[serde(default)]
    pub nodes: Option<[BrokerNodeAddressWire; 3]>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BrokerNodeConfigWire {
    pub node_id: u64,
    pub broker_address: String,
    pub operations_address: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BrokerNodeProbeWire {
    pub node_id: u64,
    pub broker_address: String,
    pub operations_address: String,
    pub broker_reachable: bool,
    pub operations_reachable: bool,
    pub health_ok: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BrokerClusterProbeWire {
    pub healthy: bool,
    pub nodes: [BrokerNodeProbeWire; 3],
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BrokerConfigResponse {
    /// `"single_node"` when connected to one Broker, `"cluster"` when connected to three nodes.
    pub mode: String,
    pub broker_address: String,
    pub operations_address: String,
    pub nodes: [BrokerNodeConfigWire; 3],
    pub probe: Option<BrokerClusterProbeWire>,
}

impl BrokerConfigResponse {
    #[must_use]
    pub fn from_config_and_probe(
        config: BrokerClusterConfig,
        probe: Option<BrokerClusterProbe>,
    ) -> Self {
        let mode = if config.is_single_node() {
            "single_node".to_owned()
        } else {
            "cluster".to_owned()
        };
        let primary = config.nodes[0];
        let nodes = config.nodes.map(|node| BrokerNodeConfigWire {
            node_id: node.node_id,
            broker_address: node.broker_address.to_string(),
            operations_address: node.operations_address.to_string(),
        });
        let probe = probe.map(|probe| BrokerClusterProbeWire {
            healthy: probe.healthy,
            nodes: probe.nodes.map(|node| BrokerNodeProbeWire {
                node_id: node.node_id,
                broker_address: node.broker_address,
                operations_address: node.operations_address,
                broker_reachable: node.broker_reachable,
                operations_reachable: node.operations_reachable,
                health_ok: node.health_ok,
            }),
        });
        Self {
            mode,
            broker_address: primary.broker_address.to_string(),
            operations_address: primary.operations_address.to_string(),
            nodes,
            probe,
        }
    }
}
