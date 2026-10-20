//! Phase 10-B: official read-only Broker `owner.status` and operator-approved
//! stale-owner recovery over a real isolated three-node Broker cluster.
//!
//! ```text
//! AGENT_BROKERD_BIN=/Volumes/256.SSD/RustBuild/broker/debug/agentbrokerd \
//!   ../.codex/rust-ssd orchestration cargo test --offline \
//!   -p heterarchy-orchestration-runtime --test broker_owner_status_e2e -- --ignored
//! ```
use std::error::Error;
use std::path::Path;

use heterarchy_orchestration_operations::{
    BrokerClusterConfig, BrokerOwnerAuthority, BrokerOwnerAuthorityError,
    BrokerOwnerRecoveryApproval, BrokerOwnerRecoveryError,
};
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

fn instance(process: u128) -> String {
    format!("orch-process-{}", Uuid::from_u128(process).as_simple())
}

fn is_stale(result: &Result<impl Sized, BrokerOwnerAuthorityError>) -> bool {
    matches!(
        result,
        Err(BrokerOwnerAuthorityError::Stale | BrokerOwnerAuthorityError::Rejected)
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires AGENT_BROKERD_BIN pointing at a built agentbrokerd"]
async fn owner_status_and_approved_recovery_over_real_cluster() -> TestResult {
    let mut cluster = Cluster::start().await?;
    let config = cluster.orchestration_config()?;
    let work = cluster.root.join("status");
    std::fs::create_dir_all(&work)?;
    let destination = Uuid::from_u128(0xC0_0001);

    // 1. Unknown session: linearizable status reports absence, creates nothing.
    let mut x = authority(&work, "x.json", destination, 0xD1, config)?;
    let unknown = x.owner_status().await?;
    assert_eq!(unknown.owner_epoch(), None);
    assert_eq!(unknown.owner_instance_id(), None);
    assert!(unknown.applied_index() > 0);
    assert!(
        !work.join("x.json").exists(),
        "status must not create local state"
    );
    let absent_again = x.owner_status().await?;
    assert_eq!(
        absent_again.owner_epoch(),
        None,
        "a read must not create a session"
    );

    // 2. Normal path: X acquires epoch 1 and status names exactly that owner.
    assert_eq!(x.ensure_current_owner().await?.owner_epoch(), 1);
    let first = x.owner_status().await?;
    assert_eq!(first.owner_epoch(), Some(1));
    assert_eq!(first.owner_instance_id(), Some(instance(0xD1).as_str()));

    // 3. Reads never append log entries, never advance the epoch.
    let second = x.owner_status().await?;
    assert_eq!(
        second, first,
        "repeated status is identical and writes nothing"
    );
    assert_eq!(x.ensure_current_owner().await?.owner_epoch(), 1);

    // 4. Competing acquisition: Y (own receipt, same destination) takes epoch 2.
    let mut y = authority(&work, "y.json", destination, 0xD2, config)?;
    assert_eq!(y.ensure_current_owner().await?.owner_epoch(), 2);
    let after_competition = y.owner_status().await?;
    assert_eq!(after_competition.owner_epoch(), Some(2));
    assert_eq!(
        after_competition.owner_instance_id(),
        Some(instance(0xD2).as_str())
    );
    assert!(after_competition.applied_index() > first.applied_index());

    // 5. Stale-owner status: X may read, sees Y/2, and gains no authority.
    let stale_view = x.owner_status().await?;
    assert_eq!(stale_view.owner_epoch(), Some(2));
    assert_eq!(
        stale_view.owner_instance_id(),
        Some(instance(0xD2).as_str())
    );
    assert!(is_stale(&x.ensure_current_owner().await));
    let still = x.owner_status().await?;
    assert_eq!(
        still.owner_epoch(),
        Some(2),
        "reading did not hand X ownership"
    );
    assert_eq!(still.owner_instance_id(), Some(instance(0xD2).as_str()));

    // 6. Restart of the stale owner: new process, same local receipt (epoch 1).
    //    The gate stays closed; the stuck pending acquisition is retained.
    drop(x);
    let mut x2 = authority(&work, "x.json", destination, 0xD3, config)?;
    assert!(is_stale(&x2.ensure_current_owner().await));
    assert!(is_stale(&x2.ensure_current_owner().await));

    // 7. Recovery refusals: an approval naming anything but the live owner.
    let wrong_epoch = BrokerOwnerRecoveryApproval::new(1, instance(0xD2));
    assert!(matches!(
        x2.recover_stale_owner(&wrong_epoch).await,
        Err(BrokerOwnerRecoveryError::ApprovalMismatch)
    ));
    let wrong_owner = BrokerOwnerRecoveryApproval::new(2, instance(0xEE));
    assert!(matches!(
        x2.recover_stale_owner(&wrong_owner).await,
        Err(BrokerOwnerRecoveryError::ApprovalMismatch)
    ));
    assert_eq!(
        y.owner_status().await?.owner_epoch(),
        Some(2),
        "refused recovery must leave the live owner untouched"
    );
    assert!(is_stale(&x2.ensure_current_owner().await));

    quorum_and_recovery(&mut cluster, &work, config, destination, x2, y).await
}

/// Quorum loss, approved recovery, restart and failover.
async fn quorum_and_recovery(
    cluster: &mut Cluster,
    work: &Path,
    config: BrokerClusterConfig,
    destination: Uuid,
    mut x2: BrokerOwnerAuthority,
    mut y: BrokerOwnerAuthority,
) -> TestResult {
    // 8. Quorum loss: status and recovery fail closed; nothing changes.
    let leader = cluster.wait_for_leader().await?.node_id;
    let other = if leader == 1 { 2 } else { 1 };
    cluster.stop_node(leader)?;
    cluster.stop_node(other)?;
    assert!(matches!(
        y.owner_status().await,
        Err(BrokerOwnerAuthorityError::Unconfirmed)
    ));
    let approval = BrokerOwnerRecoveryApproval::new(2, instance(0xD2));
    assert!(matches!(
        x2.recover_stale_owner(&approval).await,
        Err(BrokerOwnerRecoveryError::Authority(
            BrokerOwnerAuthorityError::Unconfirmed
        ))
    ));

    // 9. Quorum recovery: status is available again and unchanged.
    cluster.start_node(leader)?;
    cluster.wait_for_raft_listener(leader).await?;
    cluster.start_node(other)?;
    cluster.wait_for_raft_listener(other).await?;
    cluster.wait_for_leader().await?;
    let recovered_view = y.owner_status().await?;
    assert_eq!(recovered_view.owner_epoch(), Some(2));
    assert_eq!(
        recovered_view.owner_instance_id(),
        Some(instance(0xD2).as_str())
    );

    // 10. Operator-approved recovery: official CAS with the Broker-observed
    //     epoch. X2 becomes owner at epoch 3, Y is fenced.
    let lease = x2.recover_stale_owner(&approval).await?;
    assert_eq!(lease.owner_epoch(), 3);
    assert_eq!(lease.owner_instance_id(), instance(0xD3));
    let after_recovery = x2.owner_status().await?;
    assert_eq!(after_recovery.owner_epoch(), Some(3));
    assert_eq!(
        after_recovery.owner_instance_id(),
        Some(instance(0xD3).as_str())
    );
    assert!(is_stale(&y.ensure_current_owner().await));
    assert_eq!(x2.ensure_current_owner().await?.owner_epoch(), 3);

    // 11. Recovery is not repeatable and not needed once receipts agree.
    let current = BrokerOwnerRecoveryApproval::new(3, instance(0xD3));
    assert!(matches!(
        x2.recover_stale_owner(&current).await,
        Err(BrokerOwnerRecoveryError::NotRequired)
    ));

    // 12. Restart of the recovered owner (new process): normal takeover from a
    //     true local receipt is the regular path and needs no approval.
    drop(x2);
    let mut x3 = authority(work, "x.json", destination, 0xD4, config)?;
    assert_eq!(x3.ensure_current_owner().await?.owner_epoch(), 4);
    assert_eq!(x3.owner_status().await?.owner_epoch(), Some(4));

    // 13. Leader failure: status survives failover with the same committed owner.
    let leader = cluster.wait_for_leader().await?.node_id;
    cluster.stop_node(leader)?;
    cluster.wait_for_new_leader(leader).await?;
    let after_failover = x3.owner_status().await?;
    assert_eq!(after_failover.owner_epoch(), Some(4));
    assert_eq!(
        after_failover.owner_instance_id(),
        Some(instance(0xD4).as_str())
    );

    // 14. A destination the Broker never saw: recovery has nothing to approve.
    let mut unseen = authority(
        work,
        "unseen.json",
        Uuid::from_u128(0xC0_0002),
        0xD5,
        config,
    )?;
    assert!(matches!(
        unseen
            .recover_stale_owner(&BrokerOwnerRecoveryApproval::new(1, instance(0xD5)))
            .await,
        Err(BrokerOwnerRecoveryError::ReceiptMismatch)
    ));
    Ok(())
}
