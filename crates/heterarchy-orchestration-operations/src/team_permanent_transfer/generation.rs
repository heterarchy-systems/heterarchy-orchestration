use forge_domain::{TimestampMillis, WorkspaceId};
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use uuid::Uuid;

use crate::OperationsError;

const MAX_ORDINAL: u8 = 7;

const ANCESTRY_SQL: &str = "CREATE TABLE ops_team_permanent_transfer_generation_ancestry(
    workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16 AND workspace_id!=zeroblob(16)),
    transfer_id BLOB NOT NULL CHECK(length(transfer_id)=16 AND transfer_id!=zeroblob(16)),
    destination_decision_id BLOB NOT NULL CHECK(length(destination_decision_id)=16 AND destination_decision_id!=zeroblob(16)),
    destination_ordinal INTEGER NOT NULL CHECK(destination_ordinal BETWEEN 0 AND 7),
    generation INTEGER NOT NULL CHECK(generation>=1),
    predecessor_transfer_id BLOB CHECK(predecessor_transfer_id IS NULL OR (length(predecessor_transfer_id)=16 AND predecessor_transfer_id!=zeroblob(16))),
    recorded_at_ms INTEGER NOT NULL CHECK(recorded_at_ms>=0),
    CHECK((generation=1 AND predecessor_transfer_id IS NULL) OR (generation>1 AND predecessor_transfer_id IS NOT NULL)),
    PRIMARY KEY(workspace_id,transfer_id),
    UNIQUE(workspace_id,destination_decision_id,destination_ordinal,generation),
    UNIQUE(workspace_id,predecessor_transfer_id),
    UNIQUE(workspace_id,transfer_id,destination_decision_id,destination_ordinal,generation),
    FOREIGN KEY(workspace_id,transfer_id) REFERENCES ops_team_permanent_transfer_intents(workspace_id,transfer_id),
    FOREIGN KEY(workspace_id,predecessor_transfer_id) REFERENCES ops_team_permanent_transfer_generation_ancestry(workspace_id,transfer_id)
)";

const HEADS_SQL: &str = "CREATE TABLE ops_team_permanent_transfer_generation_heads(
    workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16 AND workspace_id!=zeroblob(16)),
    destination_decision_id BLOB NOT NULL CHECK(length(destination_decision_id)=16 AND destination_decision_id!=zeroblob(16)),
    destination_ordinal INTEGER NOT NULL CHECK(destination_ordinal BETWEEN 0 AND 7),
    transfer_id BLOB NOT NULL CHECK(length(transfer_id)=16 AND transfer_id!=zeroblob(16)),
    generation INTEGER NOT NULL CHECK(generation>=1),
    PRIMARY KEY(workspace_id,destination_decision_id,destination_ordinal),
    FOREIGN KEY(workspace_id,transfer_id,destination_decision_id,destination_ordinal,generation)
        REFERENCES ops_team_permanent_transfer_generation_ancestry(workspace_id,transfer_id,destination_decision_id,destination_ordinal,generation)
)";

const ANCESTRY_NO_UPDATE_SQL: &str = "CREATE TRIGGER ops_team_permanent_transfer_generation_ancestry_no_update BEFORE UPDATE ON ops_team_permanent_transfer_generation_ancestry BEGIN SELECT RAISE(ABORT,'permanent Team transfer generation ancestry is immutable'); END";
const ANCESTRY_NO_DELETE_SQL: &str = "CREATE TRIGGER ops_team_permanent_transfer_generation_ancestry_no_delete BEFORE DELETE ON ops_team_permanent_transfer_generation_ancestry BEGIN SELECT RAISE(ABORT,'permanent Team transfer generation ancestry is immutable'); END";
const HEADS_NO_DELETE_SQL: &str = "CREATE TRIGGER ops_team_permanent_transfer_generation_heads_no_delete BEFORE DELETE ON ops_team_permanent_transfer_generation_heads BEGIN SELECT RAISE(ABORT,'permanent Team transfer generation heads cannot be deleted'); END";

const SLOT_HISTORY_CORRUPTION_SQL: &str = r"SELECT
    EXISTS(
        SELECT 1
        FROM ops_team_permanent_transfer_generation_ancestry AS a
        LEFT JOIN ops_team_permanent_transfer_intents AS i
          ON i.workspace_id=a.workspace_id AND i.transfer_id=a.transfer_id
        LEFT JOIN ops_team_permanent_transfer_generation_ancestry AS p
          ON p.workspace_id=a.workspace_id AND p.transfer_id=a.predecessor_transfer_id
        WHERE a.workspace_id=?1
          AND a.destination_decision_id=?2 AND a.destination_ordinal=?3
          AND (
              i.transfer_id IS NULL
              OR a.destination_decision_id!=i.destination_decision_id
              OR a.destination_ordinal!=i.destination_ordinal
              OR a.recorded_at_ms!=i.recorded_at_ms
              OR (a.generation=1 AND a.predecessor_transfer_id IS NOT NULL)
              OR (a.generation>1 AND (
                  a.predecessor_transfer_id IS NULL
                  OR p.transfer_id IS NULL
                  OR p.destination_decision_id!=a.destination_decision_id
                  OR p.destination_ordinal!=a.destination_ordinal
                  OR p.generation!=a.generation-1
                  OR p.recorded_at_ms>a.recorded_at_ms
              ))
          )
    )
    OR (
        SELECT COUNT(*) FROM ops_team_permanent_transfer_generation_ancestry
        WHERE workspace_id=?1 AND destination_decision_id=?2 AND destination_ordinal=?3
    ) != COALESCE((
        SELECT MAX(generation) FROM ops_team_permanent_transfer_generation_ancestry
        WHERE workspace_id=?1 AND destination_decision_id=?2 AND destination_ordinal=?3
    ),0)
    OR (
        SELECT COUNT(*) FROM ops_team_permanent_transfer_generation_heads
        WHERE workspace_id=?1 AND destination_decision_id=?2 AND destination_ordinal=?3
    ) != 1
    OR NOT EXISTS(
        SELECT 1
        FROM ops_team_permanent_transfer_generation_heads AS h
        JOIN ops_team_permanent_transfer_generation_ancestry AS a
          ON a.workspace_id=h.workspace_id AND a.transfer_id=h.transfer_id
         AND a.destination_decision_id=h.destination_decision_id
         AND a.destination_ordinal=h.destination_ordinal
         AND a.generation=h.generation
        WHERE h.workspace_id=?1 AND h.destination_decision_id=?2
          AND h.destination_ordinal=?3
          AND h.generation=(
              SELECT MAX(generation)
              FROM ops_team_permanent_transfer_generation_ancestry
              WHERE workspace_id=?1 AND destination_decision_id=?2
                AND destination_ordinal=?3
          )
    )
    OR EXISTS(
        SELECT 1
        FROM ops_team_permanent_transfer_generation_heads AS h
        JOIN ops_team_permanent_transfer_generation_ancestry AS a
          ON a.workspace_id=h.workspace_id AND a.transfer_id=h.transfer_id
        WHERE a.workspace_id=?1 AND a.destination_decision_id=?2
          AND a.destination_ordinal=?3
          AND (h.destination_decision_id!=a.destination_decision_id
               OR h.destination_ordinal!=a.destination_ordinal
               OR h.generation!=a.generation)
    )";

const SLOT_UNCLASSIFIED_INTENT_SQL: &str = r"SELECT EXISTS(
    SELECT 1
    FROM ops_team_permanent_transfer_intents AS i
    WHERE i.workspace_id=?1 AND i.destination_decision_id=?2
      AND i.destination_ordinal=?3
      AND NOT EXISTS(
          SELECT 1
          FROM ops_team_permanent_transfer_generation_ancestry AS a
          WHERE a.workspace_id=i.workspace_id AND a.transfer_id=i.transfer_id
            AND a.destination_decision_id=i.destination_decision_id
            AND a.destination_ordinal=i.destination_ordinal
      )
      AND (?4 IS NULL OR i.transfer_id!=?4)
)
OR (?4 IS NOT NULL AND NOT EXISTS(
    SELECT 1
    FROM ops_team_permanent_transfer_intents AS i
    WHERE i.workspace_id=?1 AND i.transfer_id=?4
      AND i.destination_decision_id=?2 AND i.destination_ordinal=?3
      AND NOT EXISTS(
          SELECT 1 FROM ops_team_permanent_transfer_generation_ancestry AS a
          WHERE a.workspace_id=i.workspace_id AND a.transfer_id=i.transfer_id
      )
))";

pub(crate) const fn schema_objects() -> &'static [(&'static str, &'static str, &'static str)] {
    &[
        (
            "table",
            "ops_team_permanent_transfer_generation_ancestry",
            ANCESTRY_SQL,
        ),
        (
            "table",
            "ops_team_permanent_transfer_generation_heads",
            HEADS_SQL,
        ),
        (
            "trigger",
            "ops_team_permanent_transfer_generation_ancestry_no_update",
            ANCESTRY_NO_UPDATE_SQL,
        ),
        (
            "trigger",
            "ops_team_permanent_transfer_generation_ancestry_no_delete",
            ANCESTRY_NO_DELETE_SQL,
        ),
        (
            "trigger",
            "ops_team_permanent_transfer_generation_heads_no_delete",
            HEADS_NO_DELETE_SQL,
        ),
    ]
}

pub(crate) fn create_schema(connection: &Connection) -> Result<(), OperationsError> {
    for &(_, _, sql) in schema_objects() {
        connection.execute_batch(sql)?;
    }
    Ok(())
}

/// Classify every schema-v65 intent as an intent-only generation root.
///
/// The caller owns the surrounding migration transaction. The timestamp is
/// copied from the immutable proposal; this does not assert source dispatch.
pub(crate) fn backfill_v65_roots(connection: &Connection) -> Result<(), OperationsError> {
    connection.execute(
        "INSERT INTO ops_team_permanent_transfer_generation_ancestry(
             workspace_id,transfer_id,destination_decision_id,destination_ordinal,
             generation,predecessor_transfer_id,recorded_at_ms
         )
         SELECT workspace_id,transfer_id,destination_decision_id,destination_ordinal,
                1,NULL,recorded_at_ms
         FROM ops_team_permanent_transfer_intents",
        [],
    )?;
    connection.execute(
        "INSERT INTO ops_team_permanent_transfer_generation_heads(
             workspace_id,destination_decision_id,destination_ordinal,transfer_id,generation
         )
         SELECT workspace_id,destination_decision_id,destination_ordinal,transfer_id,1
         FROM ops_team_permanent_transfer_intents",
        [],
    )?;

    let corrupt: bool = connection.query_row(
        "SELECT
             EXISTS(
                 SELECT 1
                 FROM ops_team_permanent_transfer_intents AS i
                 LEFT JOIN ops_team_permanent_transfer_generation_ancestry AS a
                   ON a.workspace_id=i.workspace_id AND a.transfer_id=i.transfer_id
                 LEFT JOIN ops_team_permanent_transfer_generation_heads AS h
                   ON h.workspace_id=i.workspace_id
                  AND h.destination_decision_id=i.destination_decision_id
                  AND h.destination_ordinal=i.destination_ordinal
                 WHERE a.transfer_id IS NULL
                    OR a.destination_decision_id!=i.destination_decision_id
                    OR a.destination_ordinal!=i.destination_ordinal
                    OR a.generation!=1
                    OR a.predecessor_transfer_id IS NOT NULL
                    OR a.recorded_at_ms!=i.recorded_at_ms
                    OR h.transfer_id IS NULL OR h.transfer_id!=i.transfer_id
                    OR h.generation!=1
             )
             OR EXISTS(
                 SELECT 1
                 FROM ops_team_permanent_transfer_generation_ancestry AS a
                 LEFT JOIN ops_team_permanent_transfer_intents AS i
                   ON i.workspace_id=a.workspace_id AND i.transfer_id=a.transfer_id
                 WHERE i.transfer_id IS NULL OR a.generation!=1
                    OR a.predecessor_transfer_id IS NOT NULL
                    OR a.recorded_at_ms!=i.recorded_at_ms
             )
             OR (SELECT COUNT(*) FROM ops_team_permanent_transfer_intents)
                  != (SELECT COUNT(*) FROM ops_team_permanent_transfer_generation_ancestry)
             OR (SELECT COUNT(*) FROM ops_team_permanent_transfer_intents)
                  != (SELECT COUNT(*) FROM ops_team_permanent_transfer_generation_heads)",
        [],
        |row| row.get(0),
    )?;
    if corrupt {
        return Err(OperationsError::Corrupt(
            "permanent Team transfer v65 generation roots",
        ));
    }
    Ok(())
}

/// One immutable generation ancestry row and its derived current-head state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct LedgerGeneration {
    workspace_id: WorkspaceId,
    transfer_id: Uuid,
    decision_id: Uuid,
    ordinal: u8,
    generation: u64,
    predecessor_transfer_id: Option<Uuid>,
    recorded_at: TimestampMillis,
    head_transfer_id: Uuid,
    head_generation: u64,
}

impl LedgerGeneration {
    #[must_use]
    pub(super) const fn workspace_id(&self) -> WorkspaceId {
        self.workspace_id
    }

    #[must_use]
    pub(super) const fn transfer_id(&self) -> Uuid {
        self.transfer_id
    }

    #[must_use]
    pub(super) const fn decision_id(&self) -> Uuid {
        self.decision_id
    }

    #[must_use]
    pub(super) const fn ordinal(&self) -> u8 {
        self.ordinal
    }

    #[must_use]
    pub(super) const fn generation(&self) -> u64 {
        self.generation
    }

    #[must_use]
    pub(super) const fn predecessor_transfer_id(&self) -> Option<Uuid> {
        self.predecessor_transfer_id
    }

    #[must_use]
    pub(super) const fn recorded_at(&self) -> TimestampMillis {
        self.recorded_at
    }

    #[must_use]
    pub(super) const fn head_transfer_id(&self) -> Uuid {
        self.head_transfer_id
    }

    #[must_use]
    pub(super) const fn head_generation(&self) -> u64 {
        self.head_generation
    }

    #[must_use]
    pub(super) fn active(&self) -> bool {
        self.head_transfer_id == self.transfer_id && self.head_generation == self.generation
    }

    #[must_use]
    pub(super) fn superseded(&self) -> bool {
        !self.active()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct IntentSlot {
    decision_id: Uuid,
    ordinal: u8,
    recorded_at: TimestampMillis,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct AncestryRow {
    transfer_id: Uuid,
    decision_id: Uuid,
    ordinal: u8,
    generation: u64,
    predecessor_transfer_id: Option<Uuid>,
    recorded_at: TimestampMillis,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct GenerationHead {
    transfer_id: Uuid,
    generation: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UnclassifiedIntentAllowance {
    None,
    ReplacementCandidate(Uuid),
}

pub(super) fn read_generation(
    connection: &Connection,
    workspace_id: WorkspaceId,
    transfer_id: Uuid,
) -> Result<Option<LedgerGeneration>, OperationsError> {
    read_generation_checked(
        connection,
        workspace_id,
        transfer_id,
        UnclassifiedIntentAllowance::None,
    )
}

fn read_generation_for_replacement(
    connection: &Connection,
    workspace_id: WorkspaceId,
    predecessor_transfer_id: Uuid,
    candidate_transfer_id: Uuid,
) -> Result<Option<LedgerGeneration>, OperationsError> {
    read_generation_checked(
        connection,
        workspace_id,
        predecessor_transfer_id,
        UnclassifiedIntentAllowance::ReplacementCandidate(candidate_transfer_id),
    )
}

fn read_generation_checked(
    connection: &Connection,
    workspace_id: WorkspaceId,
    transfer_id: Uuid,
    allowance: UnclassifiedIntentAllowance,
) -> Result<Option<LedgerGeneration>, OperationsError> {
    validate_lookup_identity(workspace_id, transfer_id)?;
    let Some(intent) = load_intent_slot(connection, workspace_id, transfer_id)? else {
        let orphaned: bool = connection.query_row(
            "SELECT EXISTS(
                 SELECT 1 FROM ops_team_permanent_transfer_generation_ancestry
                 WHERE workspace_id=?1 AND transfer_id=?2
             ) OR EXISTS(
                 SELECT 1 FROM ops_team_permanent_transfer_generation_heads
                 WHERE workspace_id=?1 AND transfer_id=?2
             )",
            params![workspace_id.into_uuid().as_bytes(), transfer_id.as_bytes()],
            |row| row.get(0),
        )?;
        return if orphaned {
            Err(OperationsError::Corrupt(
                "permanent Team transfer generation intent missing",
            ))
        } else {
            Ok(None)
        };
    };
    let ancestry = load_ancestry(connection, workspace_id, transfer_id)?.ok_or(
        OperationsError::Corrupt("permanent Team transfer generation row missing"),
    )?;
    validate_intent_ancestry(intent, ancestry)?;
    let head = validate_slot_integrity(
        connection,
        workspace_id,
        ancestry.decision_id,
        ancestry.ordinal,
        allowance,
    )?;
    Ok(Some(generation_from_row(workspace_id, ancestry, head)))
}

pub(super) fn create_root(
    transaction: &Transaction<'_>,
    workspace_id: WorkspaceId,
    transfer_id: Uuid,
) -> Result<LedgerGeneration, OperationsError> {
    validate_lookup_identity(workspace_id, transfer_id)?;
    let intent = load_intent_slot(transaction, workspace_id, transfer_id)?.ok_or(
        OperationsError::Conflict("permanent Team transfer root intent missing"),
    )?;
    if load_ancestry(transaction, workspace_id, transfer_id)?.is_some() {
        return Err(OperationsError::Conflict(
            "permanent Team transfer generation already recorded",
        ));
    }
    if has_consent(transaction, workspace_id, transfer_id)? {
        return Err(OperationsError::Conflict(
            "permanent Team transfer root must precede consent",
        ));
    }

    let history_exists = slot_has_ancestry(
        transaction,
        workspace_id,
        intent.decision_id,
        intent.ordinal,
    )?;
    let head = load_head(
        transaction,
        workspace_id,
        intent.decision_id,
        intent.ordinal,
    )?;
    if history_exists != head.is_some() {
        return Err(OperationsError::Corrupt(
            "permanent Team transfer generation root slot state",
        ));
    }
    if history_exists {
        validate_slot_integrity(
            transaction,
            workspace_id,
            intent.decision_id,
            intent.ordinal,
            UnclassifiedIntentAllowance::None,
        )?;
        return Err(OperationsError::Conflict(
            "permanent Team transfer destination slot already has a generation",
        ));
    }
    let intent_count: i64 = transaction.query_row(
        "SELECT COUNT(*) FROM ops_team_permanent_transfer_intents
         WHERE workspace_id=?1 AND destination_decision_id=?2 AND destination_ordinal=?3",
        params![
            workspace_id.into_uuid().as_bytes(),
            intent.decision_id.as_bytes(),
            i64::from(intent.ordinal),
        ],
        |row| row.get(0),
    )?;
    if intent_count != 1 {
        return Err(OperationsError::Corrupt(
            "permanent Team transfer unclassified root slot",
        ));
    }

    let ancestry = AncestryRow {
        transfer_id,
        decision_id: intent.decision_id,
        ordinal: intent.ordinal,
        generation: 1,
        predecessor_transfer_id: None,
        recorded_at: intent.recorded_at,
    };
    insert_ancestry(transaction, workspace_id, ancestry)?;
    transaction.execute(
        "INSERT INTO ops_team_permanent_transfer_generation_heads(
             workspace_id,destination_decision_id,destination_ordinal,transfer_id,generation
         ) VALUES(?1,?2,?3,?4,1)",
        params![
            workspace_id.into_uuid().as_bytes(),
            intent.decision_id.as_bytes(),
            i64::from(intent.ordinal),
            transfer_id.as_bytes(),
        ],
    )?;
    Ok(generation_from_row(
        workspace_id,
        ancestry,
        GenerationHead {
            transfer_id,
            generation: 1,
        },
    ))
}

/// Advance one current intent-only slot head in the caller's transaction.
///
/// `expected_generation` is the predecessor's current generation. The caller
/// must roll back the transaction if this operation returns an error.
pub(super) fn replace_head(
    transaction: &Transaction<'_>,
    workspace_id: WorkspaceId,
    transfer_id: Uuid,
    expected_predecessor_transfer_id: Uuid,
    expected_generation: u64,
) -> Result<LedgerGeneration, OperationsError> {
    validate_lookup_identity(workspace_id, transfer_id)?;
    if expected_predecessor_transfer_id.is_nil() || expected_generation == 0 {
        return Err(OperationsError::Conflict(
            "permanent Team transfer predecessor identity",
        ));
    }
    let intent = load_intent_slot(transaction, workspace_id, transfer_id)?.ok_or(
        OperationsError::Conflict("permanent Team transfer replacement intent missing"),
    )?;
    if load_ancestry(transaction, workspace_id, transfer_id)?.is_some() {
        return Err(OperationsError::Conflict(
            "permanent Team transfer generation already recorded",
        ));
    }
    if has_consent(transaction, workspace_id, transfer_id)? {
        return Err(OperationsError::Conflict(
            "permanent Team transfer replacement must precede consent",
        ));
    }

    let predecessor_intent =
        load_intent_slot(transaction, workspace_id, expected_predecessor_transfer_id)?.ok_or(
            OperationsError::Conflict("permanent Team transfer predecessor missing"),
        )?;
    if predecessor_intent.decision_id != intent.decision_id
        || predecessor_intent.ordinal != intent.ordinal
    {
        return Err(OperationsError::Conflict(
            "permanent Team transfer predecessor slot changed",
        ));
    }
    let predecessor = read_generation_for_replacement(
        transaction,
        workspace_id,
        expected_predecessor_transfer_id,
        transfer_id,
    )?
    .ok_or(OperationsError::Conflict(
        "permanent Team transfer predecessor missing",
    ))?;
    if !predecessor.active()
        || predecessor.generation() != expected_generation
        || predecessor.decision_id() != intent.decision_id
        || predecessor.ordinal() != intent.ordinal
    {
        return Err(OperationsError::Conflict(
            "permanent Team transfer predecessor head changed",
        ));
    }
    if intent.recorded_at.get() < predecessor.recorded_at().get() {
        return Err(OperationsError::Conflict(
            "permanent Team transfer generation timestamp regressed",
        ));
    }
    let generation = expected_generation
        .checked_add(1)
        .ok_or(OperationsError::Bounds(
            "permanent Team transfer generation",
        ))?;
    let generation_db = database_generation(generation)?;
    let predecessor_db = database_generation(expected_generation)?;
    let ancestry = AncestryRow {
        transfer_id,
        decision_id: intent.decision_id,
        ordinal: intent.ordinal,
        generation,
        predecessor_transfer_id: Some(expected_predecessor_transfer_id),
        recorded_at: intent.recorded_at,
    };

    // Replacement is supported while this feature owns intent and consent
    // history only. A future source dispatcher must add a persisted no-dispatch
    // guard here before it can be enabled.
    insert_ancestry(transaction, workspace_id, ancestry)?;
    let changed = transaction.execute(
        "UPDATE ops_team_permanent_transfer_generation_heads
         SET transfer_id=?1,generation=?2
         WHERE workspace_id=?3 AND destination_decision_id=?4 AND destination_ordinal=?5
           AND transfer_id=?6 AND generation=?7",
        params![
            transfer_id.as_bytes(),
            generation_db,
            workspace_id.into_uuid().as_bytes(),
            intent.decision_id.as_bytes(),
            i64::from(intent.ordinal),
            expected_predecessor_transfer_id.as_bytes(),
            predecessor_db,
        ],
    )?;
    if changed != 1 {
        return Err(OperationsError::Conflict(
            "permanent Team transfer generation head compare-and-swap",
        ));
    }
    Ok(generation_from_row(
        workspace_id,
        ancestry,
        GenerationHead {
            transfer_id,
            generation,
        },
    ))
}

fn validate_lookup_identity(
    workspace_id: WorkspaceId,
    transfer_id: Uuid,
) -> Result<(), OperationsError> {
    if workspace_id.into_uuid().is_nil() || transfer_id.is_nil() {
        return Err(OperationsError::Conflict(
            "permanent Team transfer generation identity",
        ));
    }
    Ok(())
}

fn load_intent_slot(
    connection: &Connection,
    workspace_id: WorkspaceId,
    transfer_id: Uuid,
) -> Result<Option<IntentSlot>, OperationsError> {
    let raw = connection
        .query_row(
            "SELECT destination_decision_id,destination_ordinal,recorded_at_ms
             FROM ops_team_permanent_transfer_intents
             WHERE workspace_id=?1 AND transfer_id=?2",
            params![workspace_id.into_uuid().as_bytes(), transfer_id.as_bytes()],
            |row| {
                Ok((
                    row.get::<_, Vec<u8>>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                ))
            },
        )
        .optional()?;
    raw.map(|(decision_id, ordinal, recorded_at)| {
        Ok(IntentSlot {
            decision_id: decode_id(decision_id, "permanent transfer decision identity")?,
            ordinal: decode_ordinal(ordinal)?,
            recorded_at: decode_timestamp(recorded_at, "permanent transfer intent timestamp")?,
        })
    })
    .transpose()
}

fn load_ancestry(
    connection: &Connection,
    workspace_id: WorkspaceId,
    transfer_id: Uuid,
) -> Result<Option<AncestryRow>, OperationsError> {
    let raw = connection
        .query_row(
            "SELECT destination_decision_id,destination_ordinal,generation,
                    predecessor_transfer_id,recorded_at_ms
             FROM ops_team_permanent_transfer_generation_ancestry
             WHERE workspace_id=?1 AND transfer_id=?2",
            params![workspace_id.into_uuid().as_bytes(), transfer_id.as_bytes()],
            |row| {
                Ok((
                    row.get::<_, Vec<u8>>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, Option<Vec<u8>>>(3)?,
                    row.get::<_, i64>(4)?,
                ))
            },
        )
        .optional()?;
    raw.map(
        |(decision_id, ordinal, generation, predecessor_transfer_id, recorded_at)| {
            Ok(AncestryRow {
                transfer_id,
                decision_id: decode_id(decision_id, "permanent transfer decision identity")?,
                ordinal: decode_ordinal(ordinal)?,
                generation: decode_positive_u64(generation, "permanent Team transfer generation")?,
                predecessor_transfer_id: predecessor_transfer_id
                    .map(|id| decode_id(id, "permanent transfer predecessor identity"))
                    .transpose()?,
                recorded_at: decode_timestamp(
                    recorded_at,
                    "permanent transfer generation timestamp",
                )?,
            })
        },
    )
    .transpose()
}

fn load_head(
    connection: &Connection,
    workspace_id: WorkspaceId,
    decision_id: Uuid,
    ordinal: u8,
) -> Result<Option<GenerationHead>, OperationsError> {
    let raw = connection
        .query_row(
            "SELECT transfer_id,generation
             FROM ops_team_permanent_transfer_generation_heads
             WHERE workspace_id=?1 AND destination_decision_id=?2 AND destination_ordinal=?3",
            params![
                workspace_id.into_uuid().as_bytes(),
                decision_id.as_bytes(),
                i64::from(ordinal),
            ],
            |row| Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, i64>(1)?)),
        )
        .optional()?;
    raw.map(|(transfer_id, generation)| {
        Ok(GenerationHead {
            transfer_id: decode_id(transfer_id, "permanent transfer generation head identity")?,
            generation: decode_positive_u64(generation, "permanent Team transfer generation head")?,
        })
    })
    .transpose()
}

fn validate_intent_ancestry(
    intent: IntentSlot,
    ancestry: AncestryRow,
) -> Result<(), OperationsError> {
    if intent.decision_id != ancestry.decision_id
        || intent.ordinal != ancestry.ordinal
        || intent.recorded_at != ancestry.recorded_at
    {
        return Err(OperationsError::Corrupt(
            "permanent Team transfer intent generation mismatch",
        ));
    }
    Ok(())
}

fn validate_slot_integrity(
    connection: &Connection,
    workspace_id: WorkspaceId,
    decision_id: Uuid,
    ordinal: u8,
    allowance: UnclassifiedIntentAllowance,
) -> Result<GenerationHead, OperationsError> {
    let corrupt: bool = connection.query_row(
        SLOT_HISTORY_CORRUPTION_SQL,
        params![
            workspace_id.into_uuid().as_bytes(),
            decision_id.as_bytes(),
            i64::from(ordinal),
        ],
        |row| row.get(0),
    )?;
    if corrupt {
        return Err(OperationsError::Corrupt(
            "permanent Team transfer generation ancestry or head",
        ));
    }
    let allowed_candidate = match allowance {
        UnclassifiedIntentAllowance::None => None,
        UnclassifiedIntentAllowance::ReplacementCandidate(candidate_id) => {
            Some(candidate_id.as_bytes().to_vec())
        }
    };
    let unclassified_intent: bool = connection.query_row(
        SLOT_UNCLASSIFIED_INTENT_SQL,
        params![
            workspace_id.into_uuid().as_bytes(),
            decision_id.as_bytes(),
            i64::from(ordinal),
            allowed_candidate,
        ],
        |row| row.get(0),
    )?;
    if unclassified_intent {
        return Err(OperationsError::Corrupt(
            "permanent Team transfer generation intent classification",
        ));
    }
    load_head(connection, workspace_id, decision_id, ordinal)?.ok_or(OperationsError::Corrupt(
        "permanent Team transfer generation head missing",
    ))
}

fn slot_has_ancestry(
    connection: &Connection,
    workspace_id: WorkspaceId,
    decision_id: Uuid,
    ordinal: u8,
) -> Result<bool, OperationsError> {
    connection
        .query_row(
            "SELECT EXISTS(
                 SELECT 1 FROM ops_team_permanent_transfer_generation_ancestry
                 WHERE workspace_id=?1 AND destination_decision_id=?2 AND destination_ordinal=?3
             )",
            params![
                workspace_id.into_uuid().as_bytes(),
                decision_id.as_bytes(),
                i64::from(ordinal),
            ],
            |row| row.get(0),
        )
        .map_err(Into::into)
}

fn has_consent(
    connection: &Connection,
    workspace_id: WorkspaceId,
    transfer_id: Uuid,
) -> Result<bool, OperationsError> {
    connection
        .query_row(
            "SELECT EXISTS(
                 SELECT 1 FROM ops_team_permanent_transfer_consents
                 WHERE workspace_id=?1 AND transfer_id=?2
             )",
            params![workspace_id.into_uuid().as_bytes(), transfer_id.as_bytes()],
            |row| row.get(0),
        )
        .map_err(Into::into)
}

fn insert_ancestry(
    transaction: &Transaction<'_>,
    workspace_id: WorkspaceId,
    row: AncestryRow,
) -> Result<(), OperationsError> {
    transaction.execute(
        "INSERT INTO ops_team_permanent_transfer_generation_ancestry(
             workspace_id,transfer_id,destination_decision_id,destination_ordinal,
             generation,predecessor_transfer_id,recorded_at_ms
         ) VALUES(?1,?2,?3,?4,?5,?6,?7)",
        params![
            workspace_id.into_uuid().as_bytes(),
            row.transfer_id.as_bytes(),
            row.decision_id.as_bytes(),
            i64::from(row.ordinal),
            database_generation(row.generation)?,
            row.predecessor_transfer_id.map(|id| id.as_bytes().to_vec()),
            row.recorded_at.get(),
        ],
    )?;
    Ok(())
}

fn generation_from_row(
    workspace_id: WorkspaceId,
    row: AncestryRow,
    head: GenerationHead,
) -> LedgerGeneration {
    LedgerGeneration {
        workspace_id,
        transfer_id: row.transfer_id,
        decision_id: row.decision_id,
        ordinal: row.ordinal,
        generation: row.generation,
        predecessor_transfer_id: row.predecessor_transfer_id,
        recorded_at: row.recorded_at,
        head_transfer_id: head.transfer_id,
        head_generation: head.generation,
    }
}

fn decode_id(value: Vec<u8>, description: &'static str) -> Result<Uuid, OperationsError> {
    let bytes: [u8; 16] = value
        .try_into()
        .map_err(|_| OperationsError::Corrupt(description))?;
    let id = Uuid::from_bytes(bytes);
    if id.is_nil() {
        return Err(OperationsError::Corrupt(description));
    }
    Ok(id)
}

fn decode_ordinal(value: i64) -> Result<u8, OperationsError> {
    let ordinal = u8::try_from(value)
        .map_err(|_| OperationsError::Corrupt("permanent transfer destination ordinal"))?;
    if ordinal > MAX_ORDINAL {
        return Err(OperationsError::Corrupt(
            "permanent transfer destination ordinal",
        ));
    }
    Ok(ordinal)
}

fn decode_positive_u64(value: i64, description: &'static str) -> Result<u64, OperationsError> {
    let value = u64::try_from(value).map_err(|_| OperationsError::Corrupt(description))?;
    if value == 0 {
        return Err(OperationsError::Corrupt(description));
    }
    Ok(value)
}

fn decode_timestamp(
    value: i64,
    description: &'static str,
) -> Result<TimestampMillis, OperationsError> {
    TimestampMillis::new(value).map_err(|_| OperationsError::Corrupt(description))
}

fn database_generation(value: u64) -> Result<i64, OperationsError> {
    i64::try_from(value).map_err(|_| OperationsError::Bounds("permanent Team transfer generation"))
}

#[cfg(test)]
mod tests;
