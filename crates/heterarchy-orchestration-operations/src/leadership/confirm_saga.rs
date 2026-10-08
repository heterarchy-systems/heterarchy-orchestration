//! Broker-fenced leadership confirmation saga (transport-neutral).
//!
//! The confirm saga is the durable-side orchestration the MCP transport used
//! to own: prepare the confirm intent under the catalog lock, record the
//! exact authority fence on the Broker (network work with no lock held),
//! then commit on the recorded outcome or exclude on a stale-fence proof,
//! resuming once through re-prepare when the local commit was uncertain.
//! Every step's semantics — the CAS fences, the resume-by-readback rule,
//! the never-blind-retry rule — are the durable authority's; this module
//! only sequences them so any transport (MCP, backend) runs one identical
//! saga.
//!
//! The fence executor is injected. Production callers pass
//! [`ProductionFence`] (wrapping `BrokerAuthorityFence`); tests inject a
//! canned executor to drive the full decision table without a live Broker.
//! No catalog lock is held across the fence await.

use std::future::Future;

use uuid::Uuid;

use crate::broker_cluster::BrokerClusterConfig;
use crate::{
    BrokerAuthorityFence, LeadershipConfirmIntentState, LeadershipConfirmReadback,
    LeadershipFenceError, LeadershipFenceExclusionProof, LeadershipFenceOutcome,
    LeadershipFenceRequest, LeadershipMutationReceipt, LeadershipScope, OperationsCatalog,
    OperationsError, PrepareLeadershipConfirmCommand, PrepareLeadershipConfirmOutcome,
    PreparedLeadershipConfirm,
};

/// One authority-fence attempt over a prepared leadership confirm.
pub trait LeadershipFenceExecutor: Send + Sync {
    /// Record the exact prepared request on the Broker authority.
    fn record(
        &self,
        request: &LeadershipFenceRequest,
    ) -> impl Future<Output = Result<LeadershipFenceOutcome, LeadershipFenceError>> + Send;
}

impl LeadershipFenceExecutor for BrokerAuthorityFence {
    async fn record(
        &self,
        request: &LeadershipFenceRequest,
    ) -> Result<LeadershipFenceOutcome, LeadershipFenceError> {
        BrokerAuthorityFence::record(self, request).await
    }
}

/// Production fence executor for a configured Broker cluster.
pub struct ProductionFence {
    inner: BrokerAuthorityFence,
}

impl ProductionFence {
    /// Compose the production fence from the runtime's Broker cluster config.
    ///
    /// # Errors
    /// Rejects invalid cluster topology, endpoint, timeout, or frame bounds.
    pub fn new(config: impl Into<BrokerClusterConfig>) -> Result<Self, OperationsError> {
        Ok(Self {
            inner: BrokerAuthorityFence::new(config)?,
        })
    }
}

impl LeadershipFenceExecutor for ProductionFence {
    async fn record(
        &self,
        request: &LeadershipFenceRequest,
    ) -> Result<LeadershipFenceOutcome, LeadershipFenceError> {
        self.inner.record(request).await
    }
}

/// Immutable identity of one confirm saga run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FencedConfirmCommand {
    /// Exact Workspace, Activity or Team scope of the elected leadership.
    pub scope: LeadershipScope,
    /// Election that elected the leader.
    pub election_id: Uuid,
    /// Exact leadership revision the election CAS established.
    pub expected_revision: i64,
    /// Exact durable confirm-intent identity (replay + fence correlation).
    pub request_id: Uuid,
}

/// Outcome of one completed saga run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FencedConfirmOutcome {
    /// The commit transaction succeeded durably.
    Committed(Box<LeadershipMutationReceipt>),
    /// The intent was already durably resolved (exact replay or superseded).
    Resolved(Box<LeadershipConfirmReadback>),
}

/// Run the fenced confirm saga to completion.
///
/// Sequence (identical to the MCP transport's saga, now transport-neutral):
///
/// 1. prepare under the catalog lock → `Prepared` or `Resolved`;
/// 2. record the authority fence with no catalog lock held;
/// 3. on `Ok` commit, resuming once through re-prepare + re-commit when the
///    local commit was uncertain (`LeadershipCommitUnconfirmed`);
/// 4. on `Stale(proof)` exclude and return the durable readback, falling
///    back to the confirmation snapshot when the exclusion is not durably
///    resolved.
///
/// A `Conflict`/`Unconfirmed` fence outcome leaves the intent resumable and
/// surfaces the typed unconfirmed error; the caller reads back, never
/// blind-retries.
///
/// The caller must hold the Broker owner authority for the whole run.
///
/// # Errors
/// Returns the typed catalog/fence failures above; never collapses them
/// into a generic failure.
pub async fn run_fenced_confirm<F: LeadershipFenceExecutor>(
    catalog: &mut OperationsCatalog,
    fence: &F,
    command: FencedConfirmCommand,
) -> Result<FencedConfirmOutcome, OperationsError> {
    let prepared = match catalog.prepare_leadership_confirm(PrepareLeadershipConfirmCommand {
        scope: command.scope,
        election_id: command.election_id,
        expected_revision: command.expected_revision,
        request_id: command.request_id,
        now_ms: saga_now()?,
    })? {
        PrepareLeadershipConfirmOutcome::Prepared(prepared) => prepared,
        PrepareLeadershipConfirmOutcome::Resolved(readback) => {
            return Ok(FencedConfirmOutcome::Resolved(Box::new(readback)));
        }
    };
    let outcome = fence.record(prepared.fence_request()).await;
    complete_fenced_confirm(catalog, &prepared, outcome)
}

/// Apply an already-observed Broker outcome under the catalog owner's short
/// synchronous transaction boundary. The network attempt belongs outside that
/// boundary; commit, uncertain-commit readback and exclusion remain shared.
///
/// # Errors
/// Preserves exact intent/revision failures and unresolved fence outcomes.
pub fn complete_fenced_confirm(
    catalog: &mut OperationsCatalog,
    prepared: &PreparedLeadershipConfirm,
    outcome: Result<LeadershipFenceOutcome, LeadershipFenceError>,
) -> Result<FencedConfirmOutcome, OperationsError> {
    match outcome {
        Ok(outcome) => commit_with_resume(catalog, prepared, outcome),
        Err(LeadershipFenceError::Stale(proof)) => {
            match exclude_with_readback_fallback(catalog, prepared, proof) {
                Ok(readback) => Ok(FencedConfirmOutcome::Resolved(Box::new(readback))),
                Err(error) => Err(error),
            }
        }
        Err(LeadershipFenceError::Conflict | LeadershipFenceError::Unconfirmed) => {
            Err(OperationsError::LeadershipFenceUnconfirmed {
                workspace_id: prepared.intent().scope().workspace_id().into_uuid(),
                election_id: prepared.intent().election_id(),
                request_id: prepared.intent().request_id(),
            })
        }
    }
}

/// Re-run one already-prepared confirm intent through the fence and commit
/// (or exclude on stale). Used by recovery paths that hold a prepared
/// recovery confirm.
///
/// # Errors
/// Returns exact catalog/fence failures, including unresolved commits.
pub async fn replay_fenced_confirm<F: LeadershipFenceExecutor>(
    catalog: &mut OperationsCatalog,
    fence: &F,
    prepared: &PreparedLeadershipConfirm,
) -> Result<LeadershipConfirmReadback, OperationsError> {
    match fence.record(prepared.fence_request()).await {
        Ok(outcome) => match commit_with_resume(catalog, prepared, outcome)? {
            FencedConfirmOutcome::Committed(_) => readback_after_commit(
                catalog,
                prepared.intent().request_id(),
                prepared.intent().scope(),
            ),
            FencedConfirmOutcome::Resolved(readback) => Ok(*readback),
        },
        Err(LeadershipFenceError::Stale(proof)) => {
            let excluded = catalog.exclude_leadership_confirm(prepared, proof, saga_now()?)?;
            Ok(excluded)
        }
        Err(LeadershipFenceError::Conflict | LeadershipFenceError::Unconfirmed) => {
            Err(OperationsError::LeadershipFenceUnconfirmed {
                workspace_id: prepared.intent().scope().workspace_id().into_uuid(),
                election_id: prepared.intent().election_id(),
                request_id: prepared.intent().request_id(),
            })
        }
    }
}

fn commit_with_resume(
    catalog: &mut OperationsCatalog,
    prepared: &PreparedLeadershipConfirm,
    outcome: LeadershipFenceOutcome,
) -> Result<FencedConfirmOutcome, OperationsError> {
    let intent = prepared.intent();
    let scope = intent.scope();
    let election_id = intent.election_id();
    let expected_revision = intent.expected_local_revision();
    let request_id = intent.request_id();
    match catalog.commit_leadership_confirm(prepared, outcome.clone(), saga_now()?) {
        Ok(receipt) => Ok(FencedConfirmOutcome::Committed(Box::new(receipt))),
        Err(OperationsError::LeadershipCommitUnconfirmed { .. }) => {
            // The local commit outcome is unknown: re-prepare (idempotent
            // against the durable intent) and re-commit exactly once. The
            // fence outcome is replayed, never re-recorded.
            match catalog.prepare_leadership_confirm(PrepareLeadershipConfirmCommand {
                scope,
                election_id,
                expected_revision,
                request_id,
                now_ms: saga_now()?,
            })? {
                PrepareLeadershipConfirmOutcome::Resolved(readback) => {
                    Ok(FencedConfirmOutcome::Resolved(Box::new(readback)))
                }
                PrepareLeadershipConfirmOutcome::Prepared(reprepared) => {
                    let receipt =
                        catalog.commit_leadership_confirm(&reprepared, outcome, saga_now()?)?;
                    Ok(FencedConfirmOutcome::Committed(Box::new(receipt)))
                }
            }
        }
        Err(error) => Err(error),
    }
}

fn exclude_with_readback_fallback(
    catalog: &mut OperationsCatalog,
    prepared: &PreparedLeadershipConfirm,
    proof: LeadershipFenceExclusionProof,
) -> Result<LeadershipConfirmReadback, OperationsError> {
    let request_id = prepared.intent().request_id();
    let scope = prepared.intent().scope();
    match catalog.exclude_leadership_confirm(prepared, proof, saga_now()?) {
        Ok(readback) => Ok(readback),
        Err(error) => {
            let snapshot = catalog.leadership_confirmation_snapshot(request_id)?;
            if let Some((intent, readback)) = snapshot
                && intent.scope() == scope
                && matches!(
                    intent.state(),
                    LeadershipConfirmIntentState::Committed
                        | LeadershipConfirmIntentState::Superseded
                        | LeadershipConfirmIntentState::Rejected
                )
            {
                return Ok(readback);
            }
            Err(error)
        }
    }
}

fn readback_after_commit(
    catalog: &mut OperationsCatalog,
    request_id: Uuid,
    scope: LeadershipScope,
) -> Result<LeadershipConfirmReadback, OperationsError> {
    let snapshot = catalog.leadership_confirmation_snapshot(request_id)?;
    match snapshot {
        Some((intent, readback)) if intent.scope() == scope => Ok(readback),
        _ => Err(OperationsError::Conflict("leadership confirm intent")),
    }
}

fn saga_now() -> Result<i64, OperationsError> {
    let elapsed = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| OperationsError::Bounds("leadership saga clock"))?;
    i64::try_from(elapsed.as_millis()).map_err(|_| OperationsError::Bounds("leadership saga clock"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::leadership::governance::tests::{NOW, fixture};

    struct FakeFence {
        mode: FakeMode,
    }

    enum FakeMode {
        Echo,
        Stale,
        Unconfirmed,
    }

    impl LeadershipFenceExecutor for FakeFence {
        fn record(
            &self,
            request: &LeadershipFenceRequest,
        ) -> impl Future<Output = Result<LeadershipFenceOutcome, LeadershipFenceError>> + Send
        {
            std::future::ready(match self.mode {
                FakeMode::Echo => {
                    let revision = request.revision();
                    Ok(LeadershipFenceOutcome::from_request(request, revision))
                }
                FakeMode::Stale => Err(LeadershipFenceError::Stale(
                    LeadershipFenceExclusionProof::stale_for(request),
                )),
                FakeMode::Unconfirmed => Err(LeadershipFenceError::Unconfirmed),
            })
        }
    }

    /// Seed a v-shaped confirm scenario: leadership row in `AUTHORITY_PENDING`
    /// at revision 1 pointing at a `CLOSED_ELECTED` election, plus a live
    /// pending confirm intent for the elected candidate.
    fn seed_pending_confirm(
        fixture: &mut crate::leadership::governance::tests::Fixture,
        seed: u128,
    ) -> Result<Uuid, Box<dyn std::error::Error>> {
        let workspace = fixture.workspace;
        let election_id = Uuid::from_u128(seed | 0xE000);
        let now = NOW;
        fixture.catalog.connection.execute(
            "UPDATE ops_leadership SET state='AUTHORITY_PENDING',current_election_id=?2 WHERE workspace_id=?1 AND scope_kind=0",
            rusqlite::params![
                workspace.into_uuid().as_bytes(),
                election_id.as_bytes()
            ],
        )?;
        fixture.catalog.connection.execute(
            "INSERT INTO ops_leadership_elections(election_id,workspace_id,scope_kind,activity_id,team_id,kind,state,incumbent_participant_id,candidate_participant_id,ballot_deadline_ms,elected_participant_id,tally_yes,tally_no,tally_abstain,created_at_ms,closed_at_ms) VALUES(?1,?2,0,NULL,NULL,'RECALL','CLOSED_ELECTED',NULL,?3,?4,?3,2,0,0,?5,?4)",
            rusqlite::params![
                election_id.as_bytes(),
                workspace.into_uuid().as_bytes(),
                fixture.candidate.as_bytes(),
                now,
                now
            ],
        )?;
        fixture.catalog.connection.execute(
            "INSERT INTO ops_election_electorate(election_id,participant_id) VALUES(?1,?2)",
            rusqlite::params![election_id.as_bytes(), fixture.leader.as_bytes()],
        )?;
        fixture.catalog.connection.execute(
            "INSERT INTO ops_leadership_confirm_intents(request_id,kind,workspace_id,scope_kind,activity_id,team_id,election_id,elected_participant_id,expected_local_revision,requested_fence_revision,owner_instance_id,remote_recorded_revision,confirmed_at_ms,state,created_at_ms,resolved_at_ms) VALUES(?1,'election_confirm',?2,0,NULL,NULL,?3,?4,1,1,?5,NULL,NULL,'pending',?6,NULL)",
            rusqlite::params![
                Uuid::from_u128(seed | 0xD000).as_bytes(),
                workspace.into_uuid().as_bytes(),
                election_id.as_bytes(),
                fixture.candidate.as_bytes(),
                Uuid::now_v7().as_bytes(),
                now
            ],
        )?;
        Ok(election_id)
    }

    #[tokio::test]
    async fn echoed_fence_commits_the_elected_leader() -> Result<(), Box<dyn std::error::Error>> {
        let seed = 0xC100;
        let mut fixture = fixture(seed)?;
        let election_id = seed_pending_confirm(&mut fixture, seed)?;
        let fence = FakeFence {
            mode: FakeMode::Echo,
        };
        let outcome = run_fenced_confirm(
            &mut fixture.catalog,
            &fence,
            FencedConfirmCommand {
                scope: LeadershipScope::workspace(fixture.workspace),
                election_id,
                expected_revision: 1,
                request_id: Uuid::from_u128(seed | 0xD000),
            },
        )
        .await;
        match outcome {
            Ok(FencedConfirmOutcome::Committed(receipt)) => {
                assert_eq!(receipt.state(), crate::LeadershipState::Stabilizing);
            }
            Ok(FencedConfirmOutcome::Resolved(readback)) => {
                assert_eq!(
                    readback.intent_state(),
                    crate::LeadershipConfirmIntentState::Committed
                );
            }
            Err(error) => return Err(error.into()),
        }
        Ok(())
    }

    #[tokio::test]
    async fn stale_fence_excludes_and_resolves_the_intent() -> Result<(), Box<dyn std::error::Error>>
    {
        let seed = 0xC200;
        let mut fixture = fixture(seed)?;
        let election_id = seed_pending_confirm(&mut fixture, seed)?;
        let fence = FakeFence {
            mode: FakeMode::Stale,
        };
        let outcome = run_fenced_confirm(
            &mut fixture.catalog,
            &fence,
            FencedConfirmCommand {
                scope: LeadershipScope::workspace(fixture.workspace),
                election_id,
                expected_revision: 1,
                request_id: Uuid::from_u128(seed | 0xD000),
            },
        )
        .await;
        match outcome {
            Ok(FencedConfirmOutcome::Resolved(readback)) => {
                assert_ne!(
                    readback.intent_state(),
                    crate::LeadershipConfirmIntentState::Committed
                );
            }
            Ok(FencedConfirmOutcome::Committed(_)) => {
                return Err("stale fence must not commit".into());
            }
            Err(error) => return Err(error.into()),
        }
        Ok(())
    }

    #[tokio::test]
    async fn narrowed_confirmation_cannot_commit_the_workspace_leader()
    -> Result<(), Box<dyn std::error::Error>> {
        for (seed, activity_scope) in [(0xC400, true), (0xC500, false)] {
            let mut fixture = fixture(seed)?;
            let election_id = seed_pending_confirm(&mut fixture, seed)?;
            let scope = if activity_scope {
                LeadershipScope::activity(
                    fixture.workspace,
                    forge_domain::ActivityId::from_uuid(Uuid::from_u128(seed + 1)),
                )
            } else {
                LeadershipScope::team(fixture.workspace, Uuid::from_u128(seed + 1))
            };
            let outcome = run_fenced_confirm(
                &mut fixture.catalog,
                &FakeFence {
                    mode: FakeMode::Echo,
                },
                FencedConfirmCommand {
                    scope,
                    election_id,
                    expected_revision: 1,
                    request_id: Uuid::from_u128(seed | 0xD000),
                },
            )
            .await;
            assert!(matches!(outcome, Err(OperationsError::Conflict(_))));
            let workspace = fixture
                .catalog
                .leadership_scope_view(LeadershipScope::workspace(fixture.workspace), NOW)?
                .ok_or("workspace leadership missing")?;
            assert_eq!(workspace.state(), crate::LeadershipState::AuthorityPending);
        }
        Ok(())
    }

    #[tokio::test]
    async fn unconfirmed_fence_surfaces_the_typed_unconfirmed_error()
    -> Result<(), Box<dyn std::error::Error>> {
        let seed = 0xC300;
        let mut fixture = fixture(seed)?;
        let election_id = seed_pending_confirm(&mut fixture, seed)?;
        let fence = FakeFence {
            mode: FakeMode::Unconfirmed,
        };
        let outcome = run_fenced_confirm(
            &mut fixture.catalog,
            &fence,
            FencedConfirmCommand {
                scope: LeadershipScope::workspace(fixture.workspace),
                election_id,
                expected_revision: 1,
                request_id: Uuid::from_u128(seed | 0xD000),
            },
        )
        .await;
        assert!(matches!(
            outcome,
            Err(OperationsError::LeadershipFenceUnconfirmed { .. })
        ));
        Ok(())
    }
}
