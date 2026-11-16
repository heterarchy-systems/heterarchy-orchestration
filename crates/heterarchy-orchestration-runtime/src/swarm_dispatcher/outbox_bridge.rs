use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use forge_domain::{ActivityId, AgentInstanceId, AgentSessionId, SwarmRunId, WorkspaceId};
use heterarchy_orchestration_operations::{
    EffectFactConsumerProgress, EffectFactInput, OperationsCatalog, OperationsError,
    SettleStandaloneSwarmCommand, SwarmError, SwarmRunMode, SwarmRunRecord,
};
use uuid::Uuid;

use crate::OrchestrationRuntime;

use super::{DAG_SETTLED_KIND, SWARM_SETTLED_KIND, SwarmDispatchError};

/// Discriminant for Forge `effect_fact_outbox_entries.fact_kind` values (`0..=6`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AutonomyOutboxFactKind {
    ExecutionTerminal,
    ChangeSetTerminal,
    AssignmentReviewed,
    AssignmentOffered,
    AssignmentSubmitted,
    DagSettled,
    SwarmSettled,
}

impl AutonomyOutboxFactKind {
    /// Classify a raw `fact_kind` integer from Forge's outbox (`DAG_SETTLED_KIND = 5`, `SWARM_SETTLED_KIND = 6`).
    ///
    /// # Errors
    /// Rejects unknown `fact_kind` tags outside `0..=6`.
    pub const fn from_raw_kind(raw_kind: i64) -> Result<Self, SwarmDispatchError> {
        match raw_kind {
            0 => Ok(Self::ExecutionTerminal),
            1 => Ok(Self::ChangeSetTerminal),
            2 => Ok(Self::AssignmentReviewed),
            3 => Ok(Self::AssignmentOffered),
            4 => Ok(Self::AssignmentSubmitted),
            DAG_SETTLED_KIND => Ok(Self::DagSettled),
            SWARM_SETTLED_KIND => Ok(Self::SwarmSettled),
            other => Err(SwarmDispatchError::UnsupportedOutboxFactKind(other)),
        }
    }

    #[must_use]
    pub const fn raw_kind(self) -> i64 {
        match self {
            Self::ExecutionTerminal => 0,
            Self::ChangeSetTerminal => 1,
            Self::AssignmentReviewed => 2,
            Self::AssignmentOffered => 3,
            Self::AssignmentSubmitted => 4,
            Self::DagSettled => DAG_SETTLED_KIND,
            Self::SwarmSettled => SWARM_SETTLED_KIND,
        }
    }
}

/// Transport-neutral `SWARM_SETTLED_KIND = 6` outbox fact emitted when a `Standalone` Swarm run settles.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SwarmSettledFact {
    pub sequence: u64,
    pub migration_id: Uuid,
    pub destination_id: Uuid,
    pub settlement_id: Uuid,
    pub workspace_id: WorkspaceId,
    pub activity_id: Option<ActivityId>,
    pub swarm_run_id: SwarmRunId,
    pub settled_file_count: u64,
    pub settled_tree_digest: [u8; 32],
    pub converged_diff_digest: [u8; 32],
    pub coordinator_agent_id: AgentInstanceId,
    pub coordinator_session_id: Option<AgentSessionId>,
    pub settled_at_ms: i64,
}

impl SwarmSettledFact {
    fn validate(&self) -> Result<(), OperationsError> {
        if self.sequence == 0
            || self.migration_id.is_nil()
            || self.destination_id.is_nil()
            || self.settlement_id.is_nil()
            || self.workspace_id.into_uuid().is_nil()
            || self.swarm_run_id.into_uuid().is_nil()
            || self.coordinator_agent_id.into_uuid().is_nil()
            || self.activity_id.is_some_and(|id| id.into_uuid().is_nil())
            || self
                .coordinator_session_id
                .is_some_and(|id| id.into_uuid().is_nil())
            || self.settled_tree_digest == [0_u8; 32]
            || self.converged_diff_digest == [0_u8; 32]
            || self.settled_at_ms < 0
            || i64::try_from(self.settled_file_count).is_err()
        {
            return Err(OperationsError::Bounds("swarm settled outbox fact"));
        }
        Ok(())
    }
}

/// Unified outbox input recognized by [`AutonomyOutboxBridge`], pairing core effect facts
/// (including `DAG_SETTLED_KIND = 5`) with `SWARM_SETTLED_KIND = 6` facts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AutonomySettlementOutboxFact<'a> {
    CoreEffect(EffectFactInput<'a>),
    SwarmSettled(SwarmSettledFact),
}

impl AutonomySettlementOutboxFact<'_> {
    /// Return the typed [`AutonomyOutboxFactKind`] for this outbox entry.
    #[must_use]
    pub const fn kind(&self) -> AutonomyOutboxFactKind {
        match self {
            Self::CoreEffect(EffectFactInput::Execution(_)) => {
                AutonomyOutboxFactKind::ExecutionTerminal
            }
            Self::CoreEffect(EffectFactInput::ChangeSet(_)) => {
                AutonomyOutboxFactKind::ChangeSetTerminal
            }
            Self::CoreEffect(EffectFactInput::AssignmentReviewed(_)) => {
                AutonomyOutboxFactKind::AssignmentReviewed
            }
            Self::CoreEffect(EffectFactInput::AssignmentOffered(_)) => {
                AutonomyOutboxFactKind::AssignmentOffered
            }
            Self::CoreEffect(EffectFactInput::AssignmentSubmitted(_)) => {
                AutonomyOutboxFactKind::AssignmentSubmitted
            }
            Self::CoreEffect(EffectFactInput::DagSettled(_)) => AutonomyOutboxFactKind::DagSettled,
            Self::SwarmSettled(_) => AutonomyOutboxFactKind::SwarmSettled,
        }
    }

    /// Return the numeric `fact_kind` (`5` for `DagSettled`, `6` for `SwarmSettled`, etc.).
    #[must_use]
    pub const fn raw_fact_kind(&self) -> i64 {
        self.kind().raw_kind()
    }
}

/// Verified reconciliation receipt for one consumed `SWARM_SETTLED_KIND = 6` outbox entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SwarmSettledProjectionReceipt {
    pub sequence: u64,
    pub settlement_id: Uuid,
    pub run: SwarmRunRecord,
    pub replayed: bool,
}

/// Summary returned when [`AutonomyOutboxBridge`] consumes a mixed page of outbox facts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AutonomyOutboxApplyReport {
    pub core_progress: Option<EffectFactConsumerProgress>,
    pub swarm_settlements: Vec<SwarmSettledProjectionReceipt>,
}

#[derive(Default)]
struct MigrationSwarmSettlementCursor {
    last_sequence: u64,
    seen_by_seq: BTreeMap<u64, Uuid>,
}

/// Runtime outbox consumer bridge recognizing `SWARM_SETTLED_KIND = 6` alongside
/// `DAG_SETTLED_KIND = 5` (and kinds `0..=4`).
pub struct AutonomyOutboxBridge {
    operations: Arc<Mutex<OperationsCatalog>>,
    owner_pin: Option<Arc<Mutex<rusqlite::Connection>>>,
    destination_id: Uuid,
    swarm_cursors: Arc<Mutex<BTreeMap<Uuid, MigrationSwarmSettlementCursor>>>,
}

impl Clone for AutonomyOutboxBridge {
    fn clone(&self) -> Self {
        Self {
            operations: Arc::clone(&self.operations),
            owner_pin: self.owner_pin.as_ref().map(Arc::clone),
            destination_id: self.destination_id,
            swarm_cursors: Arc::clone(&self.swarm_cursors),
        }
    }
}

impl std::fmt::Debug for AutonomyOutboxBridge {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AutonomyOutboxBridge")
            .field("destination_id", &self.destination_id)
            .finish_non_exhaustive()
    }
}

impl AutonomyOutboxBridge {
    /// Construct an [`AutonomyOutboxBridge`] bound to `destination_id`.
    #[must_use]
    pub fn new(operations: Arc<Mutex<OperationsCatalog>>, destination_id: Uuid) -> Self {
        Self {
            operations,
            owner_pin: None,
            destination_id,
            swarm_cursors: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    /// Construct an [`AutonomyOutboxBridge`] retaining the [`OrchestrationRuntime`] owner pin.
    #[must_use]
    pub fn from_runtime(runtime: &OrchestrationRuntime) -> Self {
        let handle = runtime.operations_catalog();
        Self {
            operations: handle.catalog,
            owner_pin: Some(handle.owner_pin),
            destination_id: runtime.destination_id(),
            swarm_cursors: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    /// Read the highest `SWARM_SETTLED_KIND = 6` outbox sequence consumed for `migration_id`.
    ///
    /// # Errors
    /// Returns [`SwarmDispatchError::InflightGuardPoisoned`] if the cursor lock is poisoned.
    pub fn last_swarm_settled_sequence(
        &self,
        migration_id: Uuid,
    ) -> Result<u64, SwarmDispatchError> {
        let cursors = self
            .swarm_cursors
            .lock()
            .map_err(|_| SwarmDispatchError::InflightGuardPoisoned)?;
        Ok(cursors
            .get(&migration_id)
            .map_or(0, |cursor| cursor.last_sequence))
    }

    /// Consume a bounded slice of outbox facts (`DAG_SETTLED_KIND = 5`, `SWARM_SETTLED_KIND = 6`, etc.).
    ///
    /// Core facts (`0..=5`) delegate to [`OperationsCatalog::apply_effect_fact_inputs`];
    /// `SWARM_SETTLED_KIND = 6` facts validate destination/sequence/digest fences and reconcile
    /// `Standalone` Swarm settlement idempotently via [`OperationsCatalog::settle_standalone_swarm_run`].
    ///
    /// # Errors
    /// Fails closed on destination mismatches, sequence regressions, `DagNodeHybrid` settlement bypasses,
    /// digest mismatches, or missing verifier quorum gates.
    pub fn apply_outbox_facts(
        &self,
        migration_id: Uuid,
        facts: &[AutonomySettlementOutboxFact<'_>],
    ) -> Result<AutonomyOutboxApplyReport, SwarmDispatchError> {
        if migration_id.is_nil() {
            return Err(OperationsError::Bounds("outbox migration_id").into());
        }

        let mut core_inputs = Vec::new();
        let mut swarm_facts = Vec::new();
        for item in facts {
            match item {
                AutonomySettlementOutboxFact::CoreEffect(core) => core_inputs.push(*core),
                AutonomySettlementOutboxFact::SwarmSettled(swarm) => swarm_facts.push(*swarm),
            }
        }

        let mut catalog = self
            .operations
            .lock()
            .map_err(|_| SwarmDispatchError::OperationsCatalogPoisoned)?;

        let core_progress = if core_inputs.is_empty() {
            None
        } else {
            Some(catalog.apply_effect_fact_inputs(migration_id, &core_inputs)?)
        };

        let mut swarm_settlements = Vec::with_capacity(swarm_facts.len());
        for fact in swarm_facts {
            let receipt = self.consume_single_swarm_settled(&mut catalog, migration_id, &fact)?;
            swarm_settlements.push(receipt);
        }

        Ok(AutonomyOutboxApplyReport {
            core_progress,
            swarm_settlements,
        })
    }

    fn consume_single_swarm_settled(
        &self,
        catalog: &mut OperationsCatalog,
        migration_id: Uuid,
        fact: &SwarmSettledFact,
    ) -> Result<SwarmSettledProjectionReceipt, SwarmDispatchError> {
        fact.validate()?;
        if fact.migration_id != migration_id {
            return Err(OperationsError::Conflict("swarm settled migration_id mismatch").into());
        }
        if fact.destination_id != self.destination_id {
            return Err(SwarmDispatchError::OutboxDestinationMismatch {
                expected: self.destination_id,
                actual: fact.destination_id,
            });
        }

        {
            let cursors = self
                .swarm_cursors
                .lock()
                .map_err(|_| SwarmDispatchError::InflightGuardPoisoned)?;
            if let Some(cursor) = cursors.get(&migration_id) {
                if let Some(&recorded_settlement_id) = cursor.seen_by_seq.get(&fact.sequence) {
                    if recorded_settlement_id != fact.settlement_id {
                        return Err(OperationsError::Conflict(
                            "swarm settled sequence replay conflict",
                        )
                        .into());
                    }
                } else if fact.sequence <= cursor.last_sequence {
                    return Err(SwarmDispatchError::OutboxSequenceRegression {
                        migration_id,
                        last_sequence: cursor.last_sequence,
                        got_sequence: fact.sequence,
                    });
                }
            }
        }

        let swarm_run_uuid = fact.swarm_run_id.into_uuid();
        let run = catalog
            .get_swarm_run(fact.workspace_id, swarm_run_uuid)?
            .ok_or(SwarmError::NotFound("swarm run for outbox settlement"))?;
        if !matches!(run.mode(), SwarmRunMode::Standalone) {
            return Err(SwarmError::DagNodeHybridCannotSettleDirectly.into());
        }
        if run.converged_tree_blake3() != Some(fact.settled_tree_digest)
            || run.converged_diff_digest() != Some(fact.converged_diff_digest)
        {
            return Err(SwarmDispatchError::OutboxDigestMismatch);
        }

        let settled_run = catalog.settle_standalone_swarm_run(SettleStandaloneSwarmCommand {
            request_id: fact.settlement_id,
            workspace_id: fact.workspace_id,
            swarm_run_id: swarm_run_uuid,
            settled_at_ms: fact.settled_at_ms,
        })?;
        let replayed = settled_run.replayed();

        let mut cursors = self
            .swarm_cursors
            .lock()
            .map_err(|_| SwarmDispatchError::InflightGuardPoisoned)?;
        let cursor = cursors.entry(migration_id).or_default();
        if fact.sequence > cursor.last_sequence {
            cursor.last_sequence = fact.sequence;
        }
        cursor.seen_by_seq.insert(fact.sequence, fact.settlement_id);

        Ok(SwarmSettledProjectionReceipt {
            sequence: fact.sequence,
            settlement_id: fact.settlement_id,
            run: settled_run,
            replayed,
        })
    }
}
