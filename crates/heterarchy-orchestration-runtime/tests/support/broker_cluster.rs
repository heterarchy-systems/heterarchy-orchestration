//! Reusable real isolated three-node Broker cluster for Orchestration E2E tests.
//!
//! Each node is a real `agentbrokerd serve-cluster` process with its own redb
//! store, Raft TLS fixture, and ephemeral loopback ports (never 8811-8813).
//! Only processes spawned here are ever killed; state is removed on drop.
//! Consumers include this file with `#[path]` and must set `AGENT_BROKERD_BIN`.
#![allow(dead_code)]

use std::collections::BTreeMap;
use std::error::Error;
use std::net::{Ipv4Addr, SocketAddr, TcpListener};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use agent_broker_client::{
    AsyncStaticClusterRouter, StaticClusterNode, StaticClusterRouterConfig,
    StaticClusterRoutingRetryPolicy,
};
use heterarchy_orchestration_operations::BrokerClusterConfig;
use uuid::Uuid;

#[path = "../../../../../heterarchy-broker/crates/agent-broker-consensus/test_support/tls_fixture.rs"]
mod tls_fixture;

pub type TestResult = Result<(), Box<dyn Error>>;

pub const WAIT: Duration = Duration::from_secs(30);
pub const POLL: Duration = Duration::from_millis(50);
pub const CLIENT_TIMEOUT: Duration = Duration::from_secs(1);
pub const MAX_FRAME_BYTES: usize = 128 * 1024;

pub struct Cluster {
    pub root: PathBuf,
    pub binary: PathBuf,
    pub broker_ports: [u16; 3],
    pub operations_ports: [u16; 3],
    pub raft_ports: [u16; 3],
    pub children: BTreeMap<u64, Child>,
}

impl Drop for Cluster {
    fn drop(&mut self) {
        for child in self.children.values_mut() {
            let _ = child.kill();
            let _ = child.wait();
        }
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

impl Cluster {
    pub async fn start() -> Result<Self, Box<dyn Error>> {
        let binary = PathBuf::from(
            std::env::var_os("AGENT_BROKERD_BIN")
                .ok_or("AGENT_BROKERD_BIN must point at a built agentbrokerd binary")?,
        );
        let root = std::env::temp_dir().join(format!("orch-broker-owner-e2e-{}", Uuid::now_v7()));
        std::fs::create_dir_all(&root)?;
        tls_fixture::write_cluster_tls_fixture(&root.join("tls"), &[1, 2, 3])?;
        let ports = reserve_ports(9)?;
        let mut cluster = Self {
            root,
            binary,
            broker_ports: [ports[0], ports[1], ports[2]],
            operations_ports: [ports[3], ports[4], ports[5]],
            raft_ports: [ports[6], ports[7], ports[8]],
            children: BTreeMap::new(),
        };
        for node_id in [2_u64, 3] {
            cluster.start_node(node_id)?;
            cluster.wait_for_raft_listener(node_id).await?;
        }
        cluster.start_node(1)?;
        cluster.wait_for_leader().await?;
        Ok(cluster)
    }

    pub fn index(node_id: u64) -> Result<usize, Box<dyn Error>> {
        Ok(usize::try_from(node_id - 1)?)
    }

    pub fn start_node(&mut self, node_id: u64) -> TestResult {
        let index = Self::index(node_id)?;
        let mut command = Command::new(&self.binary);
        command
            .args(["serve-cluster", "--node-id", &node_id.to_string()])
            .args(["--host", "127.0.0.1"])
            .args(["--port", &self.broker_ports[index].to_string()])
            .args([
                "--operations-port",
                &self.operations_ports[index].to_string(),
            ])
            .args(["--raft-host", "127.0.0.1"])
            .args(["--raft-port", &self.raft_ports[index].to_string()]);
        for peer in 1_u64..=3 {
            command.arg("--raft-node").arg(format!(
                "{peer}=127.0.0.1:{}",
                self.raft_ports[Self::index(peer)?]
            ));
        }
        command
            .arg("--raft-tls-dir")
            .arg(self.root.join("tls"))
            .arg("--state-path")
            .arg(self.root.join(format!("node-{node_id}.redb")))
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        if node_id == 1 {
            command.arg("--bootstrap");
        }
        self.children.insert(node_id, command.spawn()?);
        Ok(())
    }

    pub fn stop_node(&mut self, node_id: u64) -> TestResult {
        let mut child = self
            .children
            .remove(&node_id)
            .ok_or("node was not running")?;
        child.kill()?;
        child.wait()?;
        Ok(())
    }

    pub async fn wait_for_raft_listener(&mut self, node_id: u64) -> TestResult {
        let address =
            SocketAddr::from((Ipv4Addr::LOCALHOST, self.raft_ports[Self::index(node_id)?]));
        let deadline = Instant::now() + WAIT;
        loop {
            if let Some(child) = self.children.get_mut(&node_id)
                && let Some(status) = child.try_wait()?
            {
                return Err(format!("node {node_id} exited early: {status}").into());
            }
            if tokio::net::TcpStream::connect(address).await.is_ok() {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(format!("timed out waiting for Raft listener {address}").into());
            }
            tokio::time::sleep(POLL).await;
        }
    }

    pub fn node(&self, node_id: u64) -> Result<StaticClusterNode, Box<dyn Error>> {
        let index = Self::index(node_id)?;
        Ok(StaticClusterNode {
            node_id,
            broker_address: SocketAddr::from((Ipv4Addr::LOCALHOST, self.broker_ports[index])),
            operations_address: SocketAddr::from((
                Ipv4Addr::LOCALHOST,
                self.operations_ports[index],
            )),
        })
    }

    /// Host-owned Orchestration cluster configuration (shared by owner and
    /// leadership fences), with a short routing deadline for fast failures.
    pub fn orchestration_config(&self) -> Result<BrokerClusterConfig, Box<dyn Error>> {
        let pair = |id| -> Result<(SocketAddr, SocketAddr), Box<dyn Error>> {
            let node = self.node(id)?;
            Ok((node.broker_address, node.operations_address))
        };
        let mut config = BrokerClusterConfig::from_node_pairs([pair(1)?, pair(2)?, pair(3)?])?;
        config.timeout = CLIENT_TIMEOUT;
        Ok(config)
    }

    pub fn router(&self) -> Result<AsyncStaticClusterRouter, Box<dyn Error>> {
        Ok(AsyncStaticClusterRouter::new(StaticClusterRouterConfig {
            nodes: [self.node(1)?, self.node(2)?, self.node(3)?],
            timeout: CLIENT_TIMEOUT,
            max_response_frame_bytes: MAX_FRAME_BYTES,
            retry_policy: StaticClusterRoutingRetryPolicy::default(),
        })?)
    }

    pub async fn wait_for_leader(&mut self) -> Result<StaticClusterNode, Box<dyn Error>> {
        let router = self.router()?;
        let deadline = Instant::now() + WAIT;
        loop {
            for (node_id, child) in &mut self.children {
                if let Some(status) = child.try_wait()? {
                    return Err(format!("node {node_id} exited: {status}").into());
                }
            }
            if let Ok(leader) = router.discover_write_leader().await {
                return Ok(leader);
            }
            if Instant::now() >= deadline {
                return Err("timed out waiting for a write-ready leader".into());
            }
            tokio::time::sleep(POLL).await;
        }
    }

    pub async fn wait_for_new_leader(&mut self, excluded: u64) -> TestResult {
        let router = self.router()?;
        let deadline = Instant::now() + WAIT;
        loop {
            if let Ok(leader) = router.discover_write_leader().await
                && leader.node_id != excluded
            {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err("timed out waiting for leader failover".into());
            }
            tokio::time::sleep(POLL).await;
        }
    }
}

pub fn reserve_ports(count: usize) -> Result<Vec<u16>, Box<dyn Error>> {
    let listeners = (0..count)
        .map(|_| TcpListener::bind((Ipv4Addr::LOCALHOST, 0)))
        .collect::<Result<Vec<_>, _>>()?;
    let ports = listeners
        .iter()
        .map(|listener| listener.local_addr().map(|address| address.port()))
        .collect::<Result<Vec<_>, _>>()?;
    drop(listeners);
    Ok(ports)
}
