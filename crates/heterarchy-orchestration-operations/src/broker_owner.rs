//! Broker-backed process ownership for Orchestration coordination admission.
//!
//! This is deliberately separate from product leadership. Broker command-session
//! ownership fences competing Orchestration process incarnations; it never
//! selects a Workspace or Activity leader. Raft leader changes are hidden by
//! the fixed-three-node router and therefore do not advance the owner epoch.
use std::path::{Path, PathBuf};

use agent_broker_application::{
    BrokerErrorCode, BrokerErrorDisposition, CommandSessionId, SessionOwnerEpoch,
    SessionOwnerInstanceId,
};
use agent_broker_client::{
    AsyncDurableClientSessionStore, AsyncStaticClusterRouter, ClientError, ClientSessionStoreError,
    DurableSessionOwner, PendingOwnerAcquisition, StaticClusterRoutingError,
};
use agent_broker_protocol::RequestId;
use sha2::{Digest, Sha256};
use thiserror::Error;
use uuid::Uuid;

use crate::broker_cluster::BrokerClusterConfig;

const REQUEST_DOMAIN: &[u8] = b"heterarchy-orchestration:broker-owner:v1";

/// Broker-confirmed ownership used only as an Orchestration process fence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrokerOwnerLease {
    owner_epoch: u64,
    owner_instance_id: String,
}

impl BrokerOwnerLease {
    #[must_use]
    pub const fn owner_epoch(&self) -> u64 {
        self.owner_epoch
    }

    #[must_use]
    pub fn owner_instance_id(&self) -> &str {
        &self.owner_instance_id
    }
}

/// Fail-closed owner admission outcomes.
#[derive(Debug, Error)]
pub enum BrokerOwnerAuthorityError {
    #[error("Broker owner state is unavailable: {0}")]
    LocalState(#[source] ClientSessionStoreError),
    #[error("Broker owner state is already owned by another local Orchestration process")]
    LocalOwnerBusy,
    #[error("Broker owner routing configuration is invalid")]
    InvalidConfiguration,
    #[error("Broker owner authority is stale")]
    Stale,
    #[error("Broker definitively rejected owner authority")]
    Rejected,
    #[error("Broker owner authority outcome is unconfirmed")]
    Unconfirmed,
}

/// Linearizable, read-only Broker owner evidence. It carries no mutation
/// authority; the Broker is the only source of these values.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrokerOwnerObservation {
    owner_epoch: Option<u64>,
    owner_instance_id: Option<String>,
    applied_index: u64,
}

impl BrokerOwnerObservation {
    /// Committed owner epoch, absent when the Broker has never seen this session.
    #[must_use]
    pub const fn owner_epoch(&self) -> Option<u64> {
        self.owner_epoch
    }

    /// Committed owner instance, absent for an unknown or ownerless session.
    #[must_use]
    pub fn owner_instance_id(&self) -> Option<&str> {
        self.owner_instance_id.as_deref()
    }

    /// Replicated-log index applied when the linearizable read was confirmed.
    #[must_use]
    pub const fn applied_index(&self) -> u64 {
        self.applied_index
    }
}

/// Explicit operator approval to replace exactly one Broker-confirmed owner.
///
/// The approval names the epoch and instance the operator inspected. Recovery
/// re-reads Broker status and refuses when the live owner differs, so an
/// approval can never be reused against a different owner.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrokerOwnerRecoveryApproval {
    owner_epoch: u64,
    owner_instance_id: String,
}

impl BrokerOwnerRecoveryApproval {
    #[must_use]
    pub fn new(owner_epoch: u64, owner_instance_id: impl Into<String>) -> Self {
        Self {
            owner_epoch,
            owner_instance_id: owner_instance_id.into(),
        }
    }
}

/// Fail-closed recovery outcomes. Authority failures keep their original
/// classification; no variant authorizes automatic retry with a new request.
#[derive(Debug, Error)]
pub enum BrokerOwnerRecoveryError {
    #[error(transparent)]
    Authority(#[from] BrokerOwnerAuthorityError),
    #[error("Broker owner state matches the local receipt; recovery is not required")]
    NotRequired,
    #[error("Broker owner state is inconsistent with the local receipt")]
    ReceiptMismatch,
    #[error("recovery approval does not match the live Broker owner")]
    ApprovalMismatch,
    #[error("an unresolved local command blocks owner recovery")]
    Blocked,
}

/// One host-owned durable Broker command session. The local state file is the
/// write-ahead source for exact acquisition recovery; the Broker remains the
/// authority for the epoch itself.
pub struct BrokerOwnerAuthority {
    session_id: CommandSessionId,
    process_instance_id: SessionOwnerInstanceId,
    state_path: PathBuf,
    store: Option<AsyncDurableClientSessionStore>,
    router: AsyncStaticClusterRouter,
}

impl BrokerOwnerAuthority {
    /// Compose a process fence for one durable Orchestration destination.
    ///
    /// The stable command-session ID derives from the destination identity,
    /// while each host process receives a fresh instance ID. Durable owner
    /// state is opened lazily when an authoritative mutation is admitted, so a
    /// competing read-only host can still compose without becoming a writer.
    ///
    /// # Errors
    /// Fails closed for invalid paths/identifiers or invalid fixed-cluster
    /// configuration. Durable-state corruption/contention is reported at
    /// authoritative mutation admission.
    pub fn new(
        state_path: impl AsRef<Path>,
        destination_id: Uuid,
        process_instance_id: Uuid,
    ) -> Result<Self, BrokerOwnerAuthorityError> {
        Self::new_with_cluster(
            state_path,
            destination_id,
            process_instance_id,
            BrokerClusterConfig::default(),
        )
    }

    /// Compose the same process fence with one host-owned Broker cluster
    /// configuration. This is a composition seam, not a caller-controlled wire
    /// input; owner and leadership fencing can therefore share the exact same
    /// topology and routing bounds.
    ///
    /// # Errors
    /// Fails closed for invalid paths/identifiers or invalid fixed-cluster
    /// configuration.
    pub fn new_with_cluster(
        state_path: impl AsRef<Path>,
        destination_id: Uuid,
        process_instance_id: Uuid,
        broker_cluster: BrokerClusterConfig,
    ) -> Result<Self, BrokerOwnerAuthorityError> {
        let state_path = state_path.as_ref().to_path_buf();
        if !state_path.is_absolute() || state_path.parent().is_none_or(|parent| !parent.is_dir()) {
            return Err(BrokerOwnerAuthorityError::InvalidConfiguration);
        }
        let session_id =
            CommandSessionId::new(format!("orch-control-{}", destination_id.as_simple()))
                .map_err(|_| BrokerOwnerAuthorityError::InvalidConfiguration)?;
        let process_instance_id = SessionOwnerInstanceId::new(format!(
            "orch-process-{}",
            process_instance_id.as_simple()
        ))
        .map_err(|_| BrokerOwnerAuthorityError::InvalidConfiguration)?;
        let router = AsyncStaticClusterRouter::new(broker_cluster.router_config())
            .map_err(|_| BrokerOwnerAuthorityError::InvalidConfiguration)?;
        Ok(Self {
            session_id,
            process_instance_id,
            state_path,
            store: None,
            router,
        })
    }

    /// Require Broker-confirmed ownership for this process before an
    /// authoritative Orchestration mutation may proceed.
    ///
    /// A crash-persisted acquisition is always recovered first with the exact
    /// same fields. Only after that ambiguity resolves may a new process take
    /// over and advance N -> N+1. A process that already owns N merely verifies
    /// N and never increments it because of a Raft leader change.
    ///
    /// # Errors
    /// Returns a bounded fail-closed classification. Stale/rejected pending
    /// acquisition is intentionally retained; without authoritative owner
    /// readback it would be unsafe to discard it and guess a newer epoch.
    pub async fn ensure_current_owner(
        &mut self,
    ) -> Result<BrokerOwnerLease, BrokerOwnerAuthorityError> {
        let store = self.store().await?;
        if let Some(pending) = store
            .pending_owner_acquisition()
            .await
            .map_err(BrokerOwnerAuthorityError::LocalState)?
        {
            let epoch = self.submit_pending(&pending).await?;
            store
                .confirm_owner_acquisition(epoch)
                .await
                .map_err(BrokerOwnerAuthorityError::LocalState)?;
        }
        let owner = store
            .owner()
            .await
            .map_err(BrokerOwnerAuthorityError::LocalState)?;
        match owner {
            Some(owner) if owner.owner_instance_id() == &self.process_instance_id => {
                self.verify_current_owner(&owner).await
            }
            Some(_) | None => self.acquire_for_current_process(&store).await,
        }
    }

    /// Read the Broker-committed owner for this destination's command session.
    ///
    /// The read is linearizable, never touches local owner state, never
    /// advances an epoch, and grants no mutation authority. A stale local
    /// process may call it.
    ///
    /// # Errors
    /// Fails closed with `Unconfirmed` when the Broker cannot establish a
    /// linearizable read (for example quorum loss or a protocol-incompatible
    /// node).
    pub async fn owner_status(&self) -> Result<BrokerOwnerObservation, BrokerOwnerAuthorityError> {
        let status = self
            .router
            .owner_status(self.session_id.clone(), self.process_instance_id.clone())
            .await
            .map_err(classify_routing_error)?;
        Ok(BrokerOwnerObservation {
            owner_epoch: status.owner.as_ref().map(|owner| owner.owner_epoch.get()),
            owner_instance_id: status
                .owner
                .and_then(|owner| owner.owner_instance_id)
                .map(|id| id.as_str().to_owned()),
            applied_index: status.applied_index,
        })
    }

    /// Operator-approved recovery of a stale local owner receipt.
    ///
    /// Order of authority: resolve any pending acquisition by its exact
    /// original request, read linearizable Broker status, compare it with the
    /// local durable receipt, require the explicit approval to name the live
    /// owner, then perform the official compare-and-set acquisition using the
    /// Broker-observed epoch as the expectation. The epoch is never guessed and
    /// no new request identity is used for an unknown outcome.
    ///
    /// # Errors
    /// Refuses (`ReceiptMismatch`, `ApprovalMismatch`, `NotRequired`,
    /// `Blocked`) or fails closed with the Broker authority classification.
    pub async fn recover_stale_owner(
        &mut self,
        approval: &BrokerOwnerRecoveryApproval,
    ) -> Result<BrokerOwnerLease, BrokerOwnerRecoveryError> {
        let store = self.store().await?;
        if store
            .in_flight()
            .await
            .map_err(BrokerOwnerAuthorityError::LocalState)?
            .is_some()
        {
            return Err(BrokerOwnerRecoveryError::Blocked);
        }
        if let Some(pending) = store
            .pending_owner_acquisition()
            .await
            .map_err(BrokerOwnerAuthorityError::LocalState)?
        {
            match self.submit_pending(&pending).await {
                Ok(epoch) => {
                    let owner = store
                        .confirm_owner_acquisition(epoch)
                        .await
                        .map_err(BrokerOwnerAuthorityError::LocalState)?;
                    return Ok(owner_lease(&owner));
                }
                Err(BrokerOwnerAuthorityError::Stale | BrokerOwnerAuthorityError::Rejected) => {
                    store
                        .release_rejected_owner_acquisition(pending)
                        .await
                        .map_err(BrokerOwnerAuthorityError::LocalState)?;
                }
                Err(other) => return Err(other.into()),
            }
        }
        let status = self.owner_status().await?;
        let (Some(broker_epoch), Some(broker_instance)) =
            (status.owner_epoch, status.owner_instance_id)
        else {
            return Err(BrokerOwnerRecoveryError::ReceiptMismatch);
        };
        if let Some(local) = store
            .owner()
            .await
            .map_err(BrokerOwnerAuthorityError::LocalState)?
        {
            let local_epoch = local.owner_epoch().get();
            if broker_epoch < local_epoch {
                return Err(BrokerOwnerRecoveryError::ReceiptMismatch);
            }
            if broker_epoch == local_epoch {
                return if broker_instance == local.owner_instance_id().as_str() {
                    Err(BrokerOwnerRecoveryError::NotRequired)
                } else {
                    Err(BrokerOwnerRecoveryError::ReceiptMismatch)
                };
            }
        }
        if broker_instance == self.process_instance_id.as_str() {
            return Err(BrokerOwnerRecoveryError::ReceiptMismatch);
        }
        if approval.owner_epoch != broker_epoch || approval.owner_instance_id != broker_instance {
            return Err(BrokerOwnerRecoveryError::ApprovalMismatch);
        }
        let observed = SessionOwnerEpoch::new(broker_epoch)
            .map_err(|_| BrokerOwnerRecoveryError::ReceiptMismatch)?;
        let pending = store
            .begin_owner_acquisition_at_observed_epoch(self.process_instance_id.clone(), observed)
            .await
            .map_err(BrokerOwnerAuthorityError::LocalState)?;
        let epoch = self.submit_pending(&pending).await?;
        let owner = store
            .confirm_owner_acquisition(epoch)
            .await
            .map_err(BrokerOwnerAuthorityError::LocalState)?;
        Ok(owner_lease(&owner))
    }

    async fn store(&mut self) -> Result<AsyncDurableClientSessionStore, BrokerOwnerAuthorityError> {
        if let Some(store) = &self.store {
            return Ok(store.clone());
        }
        let store = AsyncDurableClientSessionStore::open_or_create(
            &self.state_path,
            self.session_id.clone(),
        )
        .await
        .map_err(classify_store_error)?;
        self.store = Some(store.clone());
        Ok(store)
    }

    async fn acquire_for_current_process(
        &self,
        store: &AsyncDurableClientSessionStore,
    ) -> Result<BrokerOwnerLease, BrokerOwnerAuthorityError> {
        let pending = store
            .begin_owner_acquisition(self.process_instance_id.clone())
            .await
            .map_err(BrokerOwnerAuthorityError::LocalState)?;
        let epoch = self.submit_pending(&pending).await?;
        let owner = store
            .confirm_owner_acquisition(epoch)
            .await
            .map_err(BrokerOwnerAuthorityError::LocalState)?;
        Ok(owner_lease(&owner))
    }

    async fn submit_pending(
        &self,
        pending: &PendingOwnerAcquisition,
    ) -> Result<SessionOwnerEpoch, BrokerOwnerAuthorityError> {
        let request_id = owner_request_id(
            "acquire",
            pending.session_id(),
            pending.expected_owner_epoch(),
            pending.owner_instance_id(),
        )?;
        self.router
            .acquire_command_session_owner_with_request_id(
                request_id,
                pending.session_id().clone(),
                pending.expected_owner_epoch(),
                pending.owner_instance_id().clone(),
            )
            .await
            .map_err(classify_routing_error)
    }

    async fn verify_current_owner(
        &self,
        owner: &DurableSessionOwner,
    ) -> Result<BrokerOwnerLease, BrokerOwnerAuthorityError> {
        let request_id = owner_request_id(
            "verify",
            &self.session_id,
            owner.owner_epoch(),
            owner.owner_instance_id(),
        )?;
        let observed = self
            .router
            .acquire_command_session_owner_with_request_id(
                request_id,
                self.session_id.clone(),
                owner.owner_epoch(),
                owner.owner_instance_id().clone(),
            )
            .await
            .map_err(classify_routing_error)?;
        if observed != owner.owner_epoch() {
            return Err(BrokerOwnerAuthorityError::Rejected);
        }
        Ok(owner_lease(owner))
    }
}

fn classify_store_error(error: ClientSessionStoreError) -> BrokerOwnerAuthorityError {
    match error {
        ClientSessionStoreError::StateAlreadyOwned => BrokerOwnerAuthorityError::LocalOwnerBusy,
        other => BrokerOwnerAuthorityError::LocalState(other),
    }
}

fn owner_lease(owner: &DurableSessionOwner) -> BrokerOwnerLease {
    BrokerOwnerLease {
        owner_epoch: owner.owner_epoch().get(),
        owner_instance_id: owner.owner_instance_id().as_str().to_owned(),
    }
}

fn owner_request_id(
    phase: &str,
    session_id: &CommandSessionId,
    owner_epoch: SessionOwnerEpoch,
    owner_instance_id: &SessionOwnerInstanceId,
) -> Result<RequestId, BrokerOwnerAuthorityError> {
    let mut hash = Sha256::new();
    hash.update(REQUEST_DOMAIN);
    hash.update([0]);
    hash.update(phase.as_bytes());
    hash.update([0]);
    hash.update(session_id.as_str().as_bytes());
    hash.update([0]);
    hash.update(owner_epoch.get().to_be_bytes());
    hash.update([0]);
    hash.update(owner_instance_id.as_str().as_bytes());
    let digest = hash.finalize();
    let digest_id = Uuid::from_slice(&digest[..16])
        .map_err(|_| BrokerOwnerAuthorityError::InvalidConfiguration)?;
    RequestId::new(format!("orch-owner-{phase}-{digest_id}"))
        .map_err(|_| BrokerOwnerAuthorityError::InvalidConfiguration)
}

fn classify_routing_error(error: StaticClusterRoutingError) -> BrokerOwnerAuthorityError {
    match error {
        StaticClusterRoutingError::Client(ClientError::Broker(error))
            if error.code() == BrokerErrorCode::StaleFence =>
        {
            BrokerOwnerAuthorityError::Stale
        }
        StaticClusterRoutingError::Client(ClientError::Broker(error))
            if error.disposition() == BrokerErrorDisposition::Rejected =>
        {
            BrokerOwnerAuthorityError::Rejected
        }
        StaticClusterRoutingError::InvalidConfiguration(_) => {
            BrokerOwnerAuthorityError::InvalidConfiguration
        }
        StaticClusterRoutingError::InvalidOperationsResponse { .. }
        | StaticClusterRoutingError::NoWriteReadyLeader
        | StaticClusterRoutingError::MultipleWriteReadyLeaders(_)
        | StaticClusterRoutingError::Client(_) => BrokerOwnerAuthorityError::Unconfirmed,
    }
}

#[cfg(test)]
mod tests {
    use std::net::{Ipv4Addr, SocketAddr, TcpListener};
    use std::time::Duration;

    use agent_broker_client::StaticClusterNode;

    use super::{BrokerOwnerAuthority, BrokerOwnerAuthorityError};
    use crate::broker_cluster::BrokerClusterConfig;

    fn isolated_cluster()
    -> Result<(BrokerClusterConfig, Vec<TcpListener>), Box<dyn std::error::Error>> {
        let listeners = (0..6)
            .map(|_| TcpListener::bind((Ipv4Addr::LOCALHOST, 0)))
            .collect::<Result<Vec<_>, _>>()?;
        let addresses = listeners
            .iter()
            .map(TcpListener::local_addr)
            .collect::<Result<Vec<SocketAddr>, _>>()?;
        Ok((
            BrokerClusterConfig {
                nodes: [
                    StaticClusterNode {
                        node_id: 1,
                        broker_address: addresses[0],
                        operations_address: addresses[1],
                    },
                    StaticClusterNode {
                        node_id: 2,
                        broker_address: addresses[2],
                        operations_address: addresses[3],
                    },
                    StaticClusterNode {
                        node_id: 3,
                        broker_address: addresses[4],
                        operations_address: addresses[5],
                    },
                ],
                timeout: Duration::from_millis(25),
            },
            listeners,
        ))
    }

    #[tokio::test]
    async fn competing_host_composes_but_mutation_fails_on_local_owner_lock()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = std::env::temp_dir().join(format!(
            "heterarchy-orchestration-broker-owner-contention-{}",
            uuid::Uuid::now_v7()
        ));
        std::fs::create_dir(&directory)?;
        let state_path = directory.join("broker-owner.json");
        let destination = uuid::Uuid::from_u128(0xB011);
        let (cluster, _listeners) = isolated_cluster()?;
        let mut first = BrokerOwnerAuthority::new_with_cluster(
            &state_path,
            destination,
            uuid::Uuid::from_u128(0xB012),
            cluster,
        )?;
        let mut second = BrokerOwnerAuthority::new_with_cluster(
            &state_path,
            destination,
            uuid::Uuid::from_u128(0xB013),
            cluster,
        )?;

        assert!(matches!(
            first.ensure_current_owner().await,
            Err(BrokerOwnerAuthorityError::Unconfirmed)
        ));
        assert!(matches!(
            second.ensure_current_owner().await,
            Err(BrokerOwnerAuthorityError::LocalOwnerBusy)
        ));

        drop(second);
        drop(first);
        std::fs::remove_dir_all(directory)?;
        Ok(())
    }

    #[tokio::test]
    async fn absent_cluster_preserves_exact_pending_owner_acquisition()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = std::env::temp_dir().join(format!(
            "heterarchy-orchestration-broker-owner-{}",
            uuid::Uuid::now_v7()
        ));
        std::fs::create_dir(&directory)?;
        let state_path = directory.join("broker-owner.json");
        let (cluster, _listeners) = isolated_cluster()?;
        let mut authority = BrokerOwnerAuthority::new_with_cluster(
            &state_path,
            uuid::Uuid::from_u128(0xB001),
            uuid::Uuid::from_u128(0xB002),
            cluster,
        )?;

        assert!(matches!(
            authority.ensure_current_owner().await,
            Err(BrokerOwnerAuthorityError::Unconfirmed)
        ));
        assert!(matches!(
            authority.ensure_current_owner().await,
            Err(BrokerOwnerAuthorityError::Unconfirmed)
        ));
        assert!(state_path.is_file());
        drop(authority);
        std::fs::remove_dir_all(directory)?;
        Ok(())
    }
}
