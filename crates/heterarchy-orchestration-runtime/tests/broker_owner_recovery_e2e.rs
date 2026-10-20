//! Phase 8: Broker owner competition, displaced-owner recovery and fencing over
//! a real isolated three-node Broker cluster.
//!
//! Facts established here (see docs/e2e/claude-broker-owner-recovery-reliability-2026-10-10.md):
//! * A: a second host that shares the destination but not the local owner state
//!   can take over `N -> N+1` only while its CAS expectation is still true.
//! * B: a displaced owner stays `Stale` across restarts (Broker has no owner
//!   readback operation), until an operator adopts the latest Broker-confirmed
//!   durable owner state, after which the official acquisition API recovers.
//! * Wrong epoch / wrong owner instance never produce a successful receipt.
//!
//! ```text
//! AGENT_BROKERD_BIN=/Volumes/256.SSD/RustBuild/broker/debug/agentbrokerd \
//!   ../.codex/rust-ssd orchestration cargo test --offline \
//!   -p heterarchy-orchestration-runtime --test broker_owner_recovery_e2e -- --ignored
//! ```
use std::error::Error;
use std::path::Path;

use agent_broker_application::{
    CommandIdentity, CommandSequence, CommandSessionId, SessionOwnerEpoch, SessionOwnerInstanceId,
};
use agent_broker_client::AsyncDurableClientSessionStore;
use agent_broker_domain::NamespaceId;
use agent_broker_protocol::{BrokerRequest, EnsureNamespaceRequest, RequestId};
use forge_domain::{ForgeBackendId, OrchestrationIssuerId};
use heterarchy_orchestration_operations::{
    BrokerClusterConfig, BrokerOwnerAuthority, BrokerOwnerAuthorityError,
    ConfigureAdmissionIssuerCommand,
};
use heterarchy_orchestration_runtime::OrchestrationRuntime;
use uuid::Uuid;

#[path = "support/broker_cluster.rs"]
mod broker_cluster;

use broker_cluster::{Cluster, TestResult};

const STATE_FILE: &str = "broker-command-session.json";
const DESTINATION_FILE: &str = "agent-operations-destination";

fn session_id(destination: Uuid) -> Result<CommandSessionId, Box<dyn Error>> {
    Ok(CommandSessionId::new(format!(
        "orch-control-{}",
        destination.as_simple()
    ))?)
}

/// Durable owner receipt (Broker-confirmed epoch + instance) of one local state.
async fn durable_owner(root: &Path, destination: Uuid) -> Result<(u64, String), Box<dyn Error>> {
    let store = AsyncDurableClientSessionStore::open_or_create(
        root.join(STATE_FILE),
        session_id(destination)?,
    )
    .await?;
    let owner = store.owner().await?.ok_or("no confirmed owner receipt")?;
    Ok((
        owner.owner_epoch().get(),
        owner.owner_instance_id().as_str().to_owned(),
    ))
}

fn data_root(base: &Path, name: &str) -> Result<std::path::PathBuf, Box<dyn Error>> {
    let root = base.join(name);
    std::fs::create_dir_all(&root)?;
    Ok(root)
}

fn is_stale(result: &Result<(), BrokerOwnerAuthorityError>) -> bool {
    matches!(
        result,
        Err(BrokerOwnerAuthorityError::Stale | BrokerOwnerAuthorityError::Rejected)
    )
}

/// A and B through the real `OrchestrationRuntime` owner gate.
async fn competition_and_recovery(cluster: &Cluster, config: BrokerClusterConfig) -> TestResult {
    let base = cluster.root.join("recovery");
    let root_x = data_root(&base, "host-x")?;
    let root_y = data_root(&base, "host-y")?;

    // Normal acquisition by host X: receipt names epoch 1 and X's instance.
    let host_x = OrchestrationRuntime::open_with_broker_cluster(&root_x, config)?;
    host_x.ensure_broker_owner().await?;
    host_x.ensure_broker_owner().await?;
    let destination = host_x.destination_id();

    // A: host Y shares the destination (restored copy) but not X's local owner
    // state. Its initial CAS expectation (epoch 1) is still true, so the Broker
    // lets it take over; this is Broker CAS, not a Y-side decision.
    std::fs::copy(root_x.join(DESTINATION_FILE), root_y.join(DESTINATION_FILE))?;
    let host_y = OrchestrationRuntime::open_with_broker_cluster(&root_y, config)?;
    assert_eq!(host_y.destination_id(), destination);
    host_y.ensure_broker_owner().await?;
    host_y.ensure_broker_owner().await?;

    // The displaced owner can no longer pass the owner gate: its mutations stop.
    assert!(is_stale(&host_x.ensure_broker_owner().await));

    // B: X restarts (new process, old local epoch). Still fenced, also after Y
    // has stopped: the Broker offers no owner readback, so X cannot guess the
    // epoch and must not silently re-take ownership.
    drop(host_x);
    let restarted_x = OrchestrationRuntime::open_with_broker_cluster(&root_x, config)?;
    assert!(is_stale(&restarted_x.ensure_broker_owner().await));
    drop(host_y);
    assert!(is_stale(&restarted_x.ensure_broker_owner().await));
    assert!(is_stale(&restarted_x.ensure_broker_owner().await));
    drop(restarted_x);

    // Receipts stay truthful: X's receipt still says epoch 1, Y's says epoch 2.
    let (x_epoch, _) = durable_owner(&root_x, destination).await?;
    let (y_epoch, y_instance) = durable_owner(&root_y, destination).await?;
    assert_eq!((x_epoch, y_epoch), (1, 2));

    // Official-API recovery: the operator makes X adopt the newest Broker-
    // confirmed durable owner state (Y's receipt, epoch 2). X's fresh process
    // then acquires with a *true* expectation and advances 2 -> 3. X's pending
    // stale acquisition is superseded by the adopted state.
    std::fs::copy(root_y.join(STATE_FILE), root_x.join(STATE_FILE))?;
    let recovered_x = OrchestrationRuntime::open_with_broker_cluster(&root_x, config)?;
    recovered_x.ensure_broker_owner().await?;
    recovered_x.ensure_broker_owner().await?;
    drop(recovered_x);
    let (recovered_epoch, recovered_instance) = durable_owner(&root_x, destination).await?;
    assert_eq!(recovered_epoch, 3);
    assert_ne!(recovered_instance, y_instance);

    // Y (stale epoch 2) is now the fenced one and stays fenced after restart.
    let restarted_y = OrchestrationRuntime::open_with_broker_cluster(&root_y, config)?;
    assert!(is_stale(&restarted_y.ensure_broker_owner().await));
    Ok(())
}

fn identity(
    destination: Uuid,
    epoch: u64,
    instance: &str,
    sequence: u64,
) -> Result<CommandIdentity, Box<dyn Error>> {
    Ok(CommandIdentity::new_with_owner(
        session_id(destination)?,
        SessionOwnerEpoch::new(epoch)?,
        SessionOwnerInstanceId::new(instance.to_owned())?,
        CommandSequence::new(sequence)?,
    ))
}

fn namespace(label: &str) -> Result<BrokerRequest, Box<dyn Error>> {
    Ok(BrokerRequest::EnsureNamespace(EnsureNamespaceRequest {
        request_id: RequestId::new(format!("orch-recovery-{label}"))?,
        namespace_id: NamespaceId::new("orch-recovery-namespace")?,
    }))
}

/// Wrong epoch or wrong owner instance can never become a successful receipt.
async fn wrong_identity_is_never_a_receipt(
    cluster: &Cluster,
    config: BrokerClusterConfig,
) -> TestResult {
    let router = cluster.router()?;
    let dir = data_root(&cluster.root, "identity")?;
    let destination = Uuid::from_u128(0xB8_0001);
    let mut owner = BrokerOwnerAuthority::new_with_cluster(
        dir.join("owner.json"),
        destination,
        Uuid::from_u128(0xC1),
        config,
    )?;
    let lease = owner.ensure_current_owner().await?;
    assert_eq!(lease.owner_epoch(), 1);
    let instance = lease.owner_instance_id().to_owned();

    let wrong_instance = router
        .execute_owned(
            &identity(destination, 1, "orch-process-impostor", 1)?,
            &namespace("impostor")?,
        )
        .await;
    assert!(
        wrong_instance.is_err(),
        "impostor instance was accepted: {wrong_instance:?}"
    );
    let future_epoch = router
        .execute_owned(
            &identity(destination, 2, &instance, 1)?,
            &namespace("future")?,
        )
        .await;
    assert!(
        future_epoch.is_err(),
        "unissued epoch was accepted: {future_epoch:?}"
    );
    // Rejected attempts left the real owner fully usable and unchanged.
    router
        .execute_owned(
            &identity(destination, 1, &instance, 1)?,
            &namespace("genuine")?,
        )
        .await?;
    // Exact replay of the genuine, already-committed request is idempotent.
    router
        .execute_owned(
            &identity(destination, 1, &instance, 1)?,
            &namespace("genuine")?,
        )
        .await?;
    assert_eq!(owner.ensure_current_owner().await?.owner_epoch(), 1);
    Ok(())
}

/// Deterministic (no sleeps) proof of exactly what the owner gate does and does
/// not guarantee. The gate (`ensure_broker_owner`) and the local SQLite commit
/// are separate steps:
///
/// 1. same data root: a second process cannot even open the Runtime (local owner
///    lock), so a displaced writer cannot share one catalog;
/// 2. restored copy (two roots, one destination): once B displaces A, an
///    operation A had already admitted *does* still commit into A's own catalog
///    (the commit carries no Broker epoch), but A's next admission fails closed
///    and B's catalog never sees A's write.
async fn gate_versus_commit_window(cluster: &Cluster, config: BrokerClusterConfig) -> TestResult {
    let base = cluster.root.join("toctou");
    let root_a = data_root(&base, "a")?;
    let root_b = data_root(&base, "b")?;
    let a = OrchestrationRuntime::open_with_broker_cluster(&root_a, config)?;
    assert!(
        OrchestrationRuntime::open_with_broker_cluster(&root_a, config).is_err(),
        "a second process must not share a live data root"
    );

    // A passes the gate and is "paused" before its catalog commit.
    a.ensure_broker_owner().await?;
    std::fs::copy(root_a.join(DESTINATION_FILE), root_b.join(DESTINATION_FILE))?;
    let b = OrchestrationRuntime::open_with_broker_cluster(&root_b, config)?;
    b.ensure_broker_owner().await?;

    // A resumes: the already-admitted commit lands in A's own catalog only.
    let command = ConfigureAdmissionIssuerCommand {
        issuer: OrchestrationIssuerId::from_uuid(Uuid::from_u128(0x7001)),
        backend_id: ForgeBackendId::from_uuid(Uuid::from_u128(0x7002)),
        admission_ttl_ms: 300_000,
        now_ms: 1_000,
    };
    let receipt = a.ensure_admission_issuer(command)?;
    assert_eq!(receipt.issuer(), command.issuer);
    // Every *subsequent* admission by the displaced owner is refused.
    assert!(is_stale(&a.ensure_broker_owner().await));
    drop(a);
    drop(b);
    let b_view = OrchestrationRuntime::open_existing_operations_read_only(&root_b)?;
    assert!(
        b_view.admission_issuer_config()?.is_none(),
        "the displaced owner's late commit must not appear in the new owner's catalog"
    );
    let a_view = OrchestrationRuntime::open_existing_operations_read_only(&root_a)?;
    assert!(a_view.admission_issuer_config()?.is_some());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires AGENT_BROKERD_BIN pointing at a built agentbrokerd"]
async fn owner_competition_recovery_and_fencing_over_real_cluster() -> TestResult {
    let cluster = Cluster::start().await?;
    let config = cluster.orchestration_config()?;
    competition_and_recovery(&cluster, config).await?;
    wrong_identity_is_never_a_receipt(&cluster, config).await?;
    gate_versus_commit_window(&cluster, config).await
}
