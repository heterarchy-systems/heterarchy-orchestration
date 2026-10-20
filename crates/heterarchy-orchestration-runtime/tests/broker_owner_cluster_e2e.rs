//! Real isolated three-node Broker cluster proof for Orchestration's Broker
//! owner authority.
//!
//! Every Broker node is a real `agentbrokerd serve-cluster` process with its
//! own durable redb store, Raft TLS fixture, and ephemeral loopback ports. The
//! test only ever kills processes it spawned itself.
//!
//! Run (requires the Broker binary built from `heterarchy-broker`):
//! ```text
//! AGENT_BROKERD_BIN=/Volumes/256.SSD/RustBuild/broker/debug/agentbrokerd \
//!   .codex/rust-ssd orchestration cargo test --locked \
//!   -p heterarchy-orchestration-runtime --test broker_owner_cluster_e2e -- --ignored
//! ```
use std::error::Error;
use std::path::Path;

use agent_broker_application::{
    BrokerErrorCode, CommandIdentity, CommandSequence, CommandSessionId, SessionOwnerEpoch,
    SessionOwnerInstanceId,
};
use agent_broker_client::{ClientError, StaticClusterRoutingError};
use agent_broker_domain::NamespaceId;
use agent_broker_protocol::{BrokerRequest, EnsureNamespaceRequest, RequestId, SuccessPayload};
use heterarchy_orchestration_operations::{
    BrokerClusterConfig, BrokerOwnerAuthority, BrokerOwnerAuthorityError,
};
use heterarchy_orchestration_runtime::OrchestrationRuntime;
use uuid::Uuid;

#[path = "support/broker_cluster.rs"]
mod broker_cluster;

use broker_cluster::{Cluster, TestResult};

fn authority(
    directory: &Path,
    file: &str,
    destination: Uuid,
    process: u128,
    config: BrokerClusterConfig,
) -> Result<BrokerOwnerAuthority, Box<dyn Error>> {
    Ok(BrokerOwnerAuthority::new_with_cluster(
        directory.join(file),
        destination,
        Uuid::from_u128(process),
        config,
    )?)
}

fn mutation_identity(
    destination: Uuid,
    epoch: u64,
    process: u128,
    sequence: u64,
) -> Result<CommandIdentity, Box<dyn Error>> {
    Ok(CommandIdentity::new_with_owner(
        CommandSessionId::new(format!("orch-control-{}", destination.as_simple()))?,
        SessionOwnerEpoch::new(epoch)?,
        SessionOwnerInstanceId::new(format!(
            "orch-process-{}",
            Uuid::from_u128(process).as_simple()
        ))?,
        CommandSequence::new(sequence)?,
    ))
}

fn namespace_request(label: &str) -> Result<BrokerRequest, Box<dyn Error>> {
    Ok(BrokerRequest::EnsureNamespace(EnsureNamespaceRequest {
        request_id: RequestId::new(format!("orch-owner-e2e-{label}"))?,
        namespace_id: NamespaceId::new("orch-owner-e2e-namespace")?,
    }))
}

fn is_stale_fence(error: &StaticClusterRoutingError) -> bool {
    matches!(
        error,
        StaticClusterRoutingError::Client(ClientError::Broker(broker))
            if broker.code() == BrokerErrorCode::StaleFence
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires AGENT_BROKERD_BIN pointing at a built agentbrokerd"]
async fn owner_authority_over_real_three_node_cluster() -> TestResult {
    let mut cluster = Cluster::start().await?;
    direct_owner_scenarios(&mut cluster).await?;
    runtime_scenarios(&mut cluster).await
}

/// Owner acquisition, revalidation, competition, fencing, failure and
/// unknown-outcome recovery against the real cluster.
async fn direct_owner_scenarios(cluster: &mut Cluster) -> TestResult {
    let config = cluster.orchestration_config()?;
    let router = cluster.router()?;
    let work = cluster.root.join("work");
    std::fs::create_dir_all(&work)?;
    let destination = Uuid::from_u128(0xB0_0001);

    // 1. Initial acquisition: first owner epoch is exactly 1.
    let mut first = authority(&work, "owner.json", destination, 0xA1, config)?;
    let lease = first.ensure_current_owner().await?;
    assert_eq!(lease.owner_epoch(), 1);

    // 2. Re-validation by the existing owner never advances the epoch.
    assert_eq!(first.ensure_current_owner().await?.owner_epoch(), 1);

    // 3a. A competing local host on the same durable state is refused locally.
    let mut local_competitor = authority(&work, "owner.json", destination, 0xA2, config)?;
    assert!(matches!(
        local_competitor.ensure_current_owner().await,
        Err(BrokerOwnerAuthorityError::LocalOwnerBusy)
    ));
    drop(local_competitor);

    // 3b. A competing process (own durable state, same destination) that
    // still holds the initial CAS expectation takes over N -> N+1 while the
    // Broker is at epoch 1. Concurrent ownership is therefore resolved by the
    // Broker, never by the processes.
    let mut second_owner = authority(&work, "competitor.json", destination, 0xA3, config)?;
    assert_eq!(second_owner.ensure_current_owner().await?.owner_epoch(), 2);

    // The displaced owner fails closed on revalidation and on mutation.
    assert!(matches!(
        first.ensure_current_owner().await,
        Err(BrokerOwnerAuthorityError::Stale)
    ));
    let displaced = router
        .execute_owned(
            &mutation_identity(destination, 1, 0xA1, 1)?,
            &namespace_request("displaced")?,
        )
        .await;
    assert!(
        displaced.as_ref().is_err_and(is_stale_fence),
        "displaced owner epoch mutation must be StaleFence, got {displaced:?}"
    );
    drop(first);

    // A further process with the stale initial expectation cannot guess its
    // way past the Broker-held epoch.
    let mut late = authority(&work, "late.json", destination, 0xA6, config)?;
    assert!(matches!(
        late.ensure_current_owner().await,
        Err(BrokerOwnerAuthorityError::Stale | BrokerOwnerAuthorityError::Rejected)
    ));
    drop(late);

    // 5a. A Raft leader failure is hidden by routing: epoch stays 2.
    let leader = cluster.wait_for_leader().await?;
    cluster.stop_node(leader.node_id)?;
    cluster.wait_for_new_leader(leader.node_id).await?;
    assert_eq!(second_owner.ensure_current_owner().await?.owner_epoch(), 2);
    cluster.start_node(leader.node_id)?;
    cluster.wait_for_raft_listener(leader.node_id).await?;
    cluster.wait_for_leader().await?;

    // 4. Process restart takes over N -> N+1; the old epoch is fenced.
    drop(second_owner);
    let mut second = authority(&work, "competitor.json", destination, 0xA4, config)?;
    assert_eq!(second.ensure_current_owner().await?.owner_epoch(), 3);

    let stale = router
        .execute_owned(
            &mutation_identity(destination, 2, 0xA3, 1)?,
            &namespace_request("stale")?,
        )
        .await;
    assert!(
        stale.as_ref().is_err_and(is_stale_fence),
        "old owner epoch mutation must be rejected with StaleFence, got {stale:?}"
    );
    let current = router
        .execute_owned(
            &mutation_identity(destination, 3, 0xA4, 1)?,
            &namespace_request("current")?,
        )
        .await?;
    assert!(matches!(current, SuccessPayload::Namespace { .. }));

    // 5b. Quorum loss: owner revalidation fails closed without advancing.
    let survivors: Vec<u64> = cluster.children.keys().copied().collect();
    cluster.stop_node(survivors[0])?;
    cluster.stop_node(survivors[1])?;
    assert!(matches!(
        second.ensure_current_owner().await,
        Err(BrokerOwnerAuthorityError::Unconfirmed)
    ));

    // 6/7. Unknown outcome: a new process acquisition during quorum loss stays
    // pending in durable state; same-instance retry after recovery resolves
    // that exact acquisition (epoch 4), never a duplicate (epoch 5).
    drop(second);
    let mut third = authority(&work, "competitor.json", destination, 0xA5, config)?;
    assert!(matches!(
        third.ensure_current_owner().await,
        Err(BrokerOwnerAuthorityError::Unconfirmed)
    ));
    drop(third);
    cluster.start_node(survivors[0])?;
    cluster.start_node(survivors[1])?;
    cluster.wait_for_raft_listener(survivors[0]).await?;
    cluster.wait_for_raft_listener(survivors[1]).await?;
    cluster.wait_for_leader().await?;
    let mut third_again = authority(&work, "competitor.json", destination, 0xA5, config)?;
    let recovered = third_again.ensure_current_owner().await?;
    assert_eq!(recovered.owner_epoch(), 4);
    assert_eq!(third_again.ensure_current_owner().await?.owner_epoch(), 4);
    drop(third_again);
    Ok(())
}

/// Orchestration runtime composition over the same real cluster.
async fn runtime_scenarios(cluster: &mut Cluster) -> TestResult {
    let config = cluster.orchestration_config()?;
    let work = cluster.root.join("work");
    // Orchestration runtime integration: the host composes the same cluster
    // contract, admits only with Broker-confirmed ownership, and fails closed.
    let runtime_root = cluster.root.join("runtime");
    std::fs::create_dir_all(&runtime_root)?;
    let runtime = OrchestrationRuntime::open_with_broker_cluster(&runtime_root, config)?;
    runtime.ensure_broker_owner().await?;
    runtime.ensure_broker_owner().await?;

    let leader = cluster.wait_for_leader().await?;
    let followers: Vec<u64> = cluster
        .children
        .keys()
        .copied()
        .filter(|id| *id != leader.node_id)
        .collect();
    cluster.stop_node(followers[0])?;
    cluster.stop_node(leader.node_id)?;
    assert!(matches!(
        runtime.ensure_broker_owner().await,
        Err(BrokerOwnerAuthorityError::Unconfirmed)
    ));
    cluster.start_node(followers[0])?;
    cluster.start_node(leader.node_id)?;
    cluster.wait_for_raft_listener(followers[0]).await?;
    cluster.wait_for_raft_listener(leader.node_id).await?;
    cluster.wait_for_leader().await?;
    runtime.ensure_broker_owner().await?;

    // Restarted runtime process (new instance) takes over 1 -> 2.
    let runtime_destination = runtime.destination_id();
    drop(runtime);
    let reopened = OrchestrationRuntime::open_with_broker_cluster(&runtime_root, config)?;
    reopened.ensure_broker_owner().await?;
    reopened.ensure_broker_owner().await?;

    // A competing process with the initial CAS expectation is now stale.
    let mut runtime_competitor = authority(
        &work,
        "runtime-competitor.json",
        runtime_destination,
        0xB1,
        config,
    )?;
    assert!(matches!(
        runtime_competitor.ensure_current_owner().await,
        Err(BrokerOwnerAuthorityError::Stale | BrokerOwnerAuthorityError::Rejected)
    ));
    reopened.ensure_broker_owner().await?;
    Ok(())
}
