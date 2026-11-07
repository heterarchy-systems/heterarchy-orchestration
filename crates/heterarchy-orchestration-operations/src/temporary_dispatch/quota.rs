//! Durable per-Workspace/per-requesting-Team admission history and quota.

use forge_domain::{TimestampMillis, WorkspaceId};
use rusqlite::{OptionalExtension, Transaction, params};
use uuid::Uuid;

use super::{
    OperationsError, TF_ADMISSION_WINDOW_MS, TemporaryDispatchActorProvenance,
    TemporaryDispatchAdmissionRequest, TemporaryDispatchAdmissionResult,
    TemporaryDispatchRequestRecord,
};

const RECEIPT_TAIL_SIZE: usize = 3;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ReceiptAnchor {
    request_id: Uuid,
    sequence: i64,
    admitted_at_ms: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct LockAnchor {
    request_id: Uuid,
    locked_until_ms: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct QuotaState {
    last_admitted_at_ms: i64,
    last_sequence: i64,
    recent: [Option<ReceiptAnchor>; RECEIPT_TAIL_SIZE],
    lock: Option<LockAnchor>,
}

#[derive(Debug)]
struct RawQuotaState {
    last_admitted_at_ms: i64,
    last_sequence: i64,
    recent_1_request_id: Vec<u8>,
    recent_1_sequence: i64,
    recent_1_admitted_at_ms: i64,
    recent_2_request_id: Option<Vec<u8>>,
    recent_2_sequence: Option<i64>,
    recent_2_admitted_at_ms: Option<i64>,
    recent_3_request_id: Option<Vec<u8>>,
    recent_3_sequence: Option<i64>,
    recent_3_admitted_at_ms: Option<i64>,
    locked_until_ms: Option<i64>,
    lock_request_id: Option<Vec<u8>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum QuotaDecision {
    Admitted {
        lock_created_until_ms: Option<i64>,
        next_lock: Option<LockAnchor>,
    },
    Rejected {
        locked_until_ms: i64,
    },
}

pub(super) fn record_admission_attempt(
    transaction: &Transaction<'_>,
    request: &TemporaryDispatchAdmissionRequest,
    actor: &TemporaryDispatchActorProvenance,
    target_home_team_id: Uuid,
    now: TimestampMillis,
) -> Result<TemporaryDispatchAdmissionResult, OperationsError> {
    let (state, latest) = audit_quota_state(
        transaction,
        request.workspace_id(),
        request.requesting_team_id(),
    )?;
    let decision = decide_quota(
        transaction,
        request.workspace_id(),
        request.requesting_team_id(),
        request.request_id(),
        state,
        &latest,
        now,
    )?;
    let (lock_created_until_ms, lock) = match decision {
        QuotaDecision::Admitted {
            lock_created_until_ms,
            next_lock,
        } => (lock_created_until_ms, next_lock),
        QuotaDecision::Rejected { locked_until_ms } => {
            return Ok(TemporaryDispatchAdmissionResult::QuotaRejected {
                request_id: request.request_id(),
                workspace_id: request.workspace_id(),
                requesting_team_id: request.requesting_team_id(),
                retry_at: timestamp(locked_until_ms, "temporary dispatch lock")?,
            });
        }
    };

    let last_sequence = state.map_or(0, |current| current.last_sequence);
    let admission_sequence = last_sequence.checked_add(1).ok_or(OperationsError::Bounds(
        "temporary dispatch admission sequence",
    ))?;
    let lock_until = lock_created_until_ms
        .map(|value| timestamp(value, "temporary dispatch lock"))
        .transpose()?;
    let anchor = ReceiptAnchor {
        request_id: request.request_id(),
        sequence: admission_sequence,
        admitted_at_ms: now.get(),
    };
    let recent = [
        Some(anchor),
        state.and_then(|current| current.recent[0]),
        state.and_then(|current| current.recent[1]),
    ];
    let next_state = QuotaState {
        last_admitted_at_ms: now.get(),
        last_sequence: admission_sequence,
        recent,
        lock,
    };

    insert_request(
        transaction,
        request,
        actor,
        target_home_team_id,
        admission_sequence,
        now.get(),
        lock_until.map(TimestampMillis::get),
    )?;
    persist_quota_state(
        transaction,
        request.workspace_id(),
        request.requesting_team_id(),
        next_state,
    )?;

    Ok(TemporaryDispatchAdmissionResult::Admitted(Box::new(
        TemporaryDispatchRequestRecord {
            request_id: request.request_id(),
            workspace_id: request.workspace_id(),
            requesting_team_id: request.requesting_team_id(),
            leadership_revision: request.expected_leadership_revision(),
            actor: actor.clone(),
            target_home_team_id,
            target_participant_id: request.target_participant_id(),
            requested_scope: request.requested_scope().clone(),
            requested_end_at: request.requested_end_at(),
            admitted_at: now,
            lock_until,
            replayed: false,
        },
    )))
}

fn audit_quota_state(
    transaction: &Transaction<'_>,
    workspace_id: WorkspaceId,
    requesting_team_id: Uuid,
) -> Result<(Option<QuotaState>, Vec<ReceiptAnchor>), OperationsError> {
    let state = load_quota_state(transaction, workspace_id, requesting_team_id)?;
    let latest = latest_receipt_anchors(transaction, workspace_id, requesting_team_id)?;
    let latest_lock = latest_lock_anchor(transaction, workspace_id, requesting_team_id)?;
    match state {
        None if latest.is_empty() && latest_lock.is_none() => Ok((None, latest)),
        None => Err(OperationsError::Corrupt(
            "temporary dispatch quota state missing",
        )),
        Some(state) => {
            let expected_recent = [
                latest.first().copied(),
                latest.get(1).copied(),
                latest.get(2).copied(),
            ];
            if state.recent != expected_recent
                || state.last_sequence != state.recent[0].map_or(0, |anchor| anchor.sequence)
                || state.last_admitted_at_ms
                    != state.recent[0].map_or(-1, |anchor| anchor.admitted_at_ms)
                || state.lock != latest_lock
            {
                return Err(OperationsError::Corrupt(
                    "temporary dispatch quota receipt anchors",
                ));
            }
            Ok((Some(state), latest))
        }
    }
}

fn load_quota_state(
    transaction: &Transaction<'_>,
    workspace_id: WorkspaceId,
    requesting_team_id: Uuid,
) -> Result<Option<QuotaState>, OperationsError> {
    let raw = transaction
        .query_row(
            "SELECT last_admitted_at_ms,last_sequence,recent_1_request_id,recent_1_sequence,recent_1_admitted_at_ms,recent_2_request_id,recent_2_sequence,recent_2_admitted_at_ms,recent_3_request_id,recent_3_sequence,recent_3_admitted_at_ms,locked_until_ms,lock_request_id FROM ops_temporary_dispatch_quota_state WHERE workspace_id=?1 AND requesting_team_id=?2",
            params![
                workspace_id.into_uuid().as_bytes(),
                requesting_team_id.as_bytes(),
            ],
            |row| {
                Ok(RawQuotaState {
                    last_admitted_at_ms: row.get(0)?,
                    last_sequence: row.get(1)?,
                    recent_1_request_id: row.get(2)?,
                    recent_1_sequence: row.get(3)?,
                    recent_1_admitted_at_ms: row.get(4)?,
                    recent_2_request_id: row.get(5)?,
                    recent_2_sequence: row.get(6)?,
                    recent_2_admitted_at_ms: row.get(7)?,
                    recent_3_request_id: row.get(8)?,
                    recent_3_sequence: row.get(9)?,
                    recent_3_admitted_at_ms: row.get(10)?,
                    locked_until_ms: row.get(11)?,
                    lock_request_id: row.get(12)?,
                })
            },
        )
        .optional()?;
    raw.map(decode_quota_state).transpose()
}

fn decode_quota_state(raw: RawQuotaState) -> Result<QuotaState, OperationsError> {
    if raw.last_admitted_at_ms < 0 || raw.last_sequence <= 0 {
        return Err(OperationsError::Corrupt(
            "temporary dispatch quota high-water",
        ));
    }
    let recent_1 = decode_anchor(
        Some(raw.recent_1_request_id),
        Some(raw.recent_1_sequence),
        Some(raw.recent_1_admitted_at_ms),
    )?
    .ok_or(OperationsError::Corrupt(
        "temporary dispatch quota first anchor",
    ))?;
    let recent_2 = decode_anchor(
        raw.recent_2_request_id,
        raw.recent_2_sequence,
        raw.recent_2_admitted_at_ms,
    )?;
    let recent_3 = decode_anchor(
        raw.recent_3_request_id,
        raw.recent_3_sequence,
        raw.recent_3_admitted_at_ms,
    )?;
    if recent_1.sequence != raw.last_sequence
        || recent_1.admitted_at_ms != raw.last_admitted_at_ms
        || recent_2.is_some_and(|anchor| anchor.sequence.checked_add(1) != Some(recent_1.sequence))
        || recent_3.is_some_and(|anchor| {
            recent_2.is_none_or(|newer| anchor.sequence.checked_add(1) != Some(newer.sequence))
        })
    {
        return Err(OperationsError::Corrupt(
            "temporary dispatch quota anchor sequence",
        ));
    }
    let lock = match (raw.locked_until_ms, raw.lock_request_id) {
        (None, None) => None,
        (Some(locked_until_ms), Some(request_id)) if locked_until_ms >= 0 => {
            let request_id = Uuid::from_bytes(crate::wire::id(request_id)?);
            if request_id.is_nil() {
                return Err(OperationsError::Corrupt(
                    "temporary dispatch quota lock identity",
                ));
            }
            Some(LockAnchor {
                request_id,
                locked_until_ms,
            })
        }
        _ => {
            return Err(OperationsError::Corrupt(
                "temporary dispatch quota lock state",
            ));
        }
    };
    Ok(QuotaState {
        last_admitted_at_ms: raw.last_admitted_at_ms,
        last_sequence: raw.last_sequence,
        recent: [Some(recent_1), recent_2, recent_3],
        lock,
    })
}

fn decode_anchor(
    request_id: Option<Vec<u8>>,
    sequence: Option<i64>,
    admitted_at_ms: Option<i64>,
) -> Result<Option<ReceiptAnchor>, OperationsError> {
    match (request_id, sequence, admitted_at_ms) {
        (None, None, None) => Ok(None),
        (Some(request_id), Some(sequence), Some(admitted_at_ms))
            if sequence > 0 && admitted_at_ms >= 0 =>
        {
            let request_id = Uuid::from_bytes(crate::wire::id(request_id)?);
            if request_id.is_nil() {
                return Err(OperationsError::Corrupt(
                    "temporary dispatch quota anchor identity",
                ));
            }
            Ok(Some(ReceiptAnchor {
                request_id,
                sequence,
                admitted_at_ms,
            }))
        }
        _ => Err(OperationsError::Corrupt(
            "temporary dispatch quota anchor shape",
        )),
    }
}

fn latest_receipt_anchors(
    transaction: &Transaction<'_>,
    workspace_id: WorkspaceId,
    requesting_team_id: Uuid,
) -> Result<Vec<ReceiptAnchor>, OperationsError> {
    let mut statement = transaction.prepare(
        "SELECT request_id,admission_sequence,admitted_at_ms FROM ops_temporary_dispatch_requests WHERE workspace_id=?1 AND requesting_team_id=?2 ORDER BY admission_sequence DESC LIMIT 3",
    )?;
    let rows = statement.query_map(
        params![
            workspace_id.into_uuid().as_bytes(),
            requesting_team_id.as_bytes(),
        ],
        |row| {
            Ok((
                row.get::<_, Vec<u8>>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, i64>(2)?,
            ))
        },
    )?;
    let mut anchors = Vec::with_capacity(RECEIPT_TAIL_SIZE);
    for row in rows {
        let (request_id, sequence, admitted_at_ms) = row?;
        if sequence <= 0 || admitted_at_ms < 0 {
            return Err(OperationsError::Corrupt(
                "temporary dispatch admitted receipt index",
            ));
        }
        let request_id = Uuid::from_bytes(crate::wire::id(request_id)?);
        if request_id.is_nil() {
            return Err(OperationsError::Corrupt(
                "temporary dispatch admitted receipt identity",
            ));
        }
        anchors.push(ReceiptAnchor {
            request_id,
            sequence,
            admitted_at_ms,
        });
    }
    Ok(anchors)
}

fn latest_lock_anchor(
    transaction: &Transaction<'_>,
    workspace_id: WorkspaceId,
    requesting_team_id: Uuid,
) -> Result<Option<LockAnchor>, OperationsError> {
    let raw = transaction
        .query_row(
            "SELECT request_id,lock_created_until_ms FROM ops_temporary_dispatch_requests WHERE workspace_id=?1 AND requesting_team_id=?2 AND lock_created_until_ms IS NOT NULL ORDER BY lock_created_until_ms DESC,admission_sequence DESC LIMIT 1",
            params![
                workspace_id.into_uuid().as_bytes(),
                requesting_team_id.as_bytes(),
            ],
            |row| Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, i64>(1)?)),
        )
        .optional()?;
    raw.map(|(request_id, locked_until_ms)| {
        let request_id = Uuid::from_bytes(crate::wire::id(request_id)?);
        if request_id.is_nil() || locked_until_ms < 0 {
            return Err(OperationsError::Corrupt("temporary dispatch lock receipt"));
        }
        Ok(LockAnchor {
            request_id,
            locked_until_ms,
        })
    })
    .transpose()
}

fn decide_quota(
    transaction: &Transaction<'_>,
    workspace_id: WorkspaceId,
    requesting_team_id: Uuid,
    request_id: Uuid,
    state: Option<QuotaState>,
    latest: &[ReceiptAnchor],
    now: TimestampMillis,
) -> Result<QuotaDecision, OperationsError> {
    let next_lock_boundary = now
        .get()
        .checked_add(TF_ADMISSION_WINDOW_MS)
        .ok_or(OperationsError::Bounds("temporary dispatch clock range"))?;
    if latest
        .first()
        .is_some_and(|anchor| now.get() < anchor.admitted_at_ms)
    {
        return Err(OperationsError::Conflict(
            "temporary dispatch clock regression",
        ));
    }
    let lock = state.and_then(|current| current.lock);
    if let Some(lock) = lock
        && now.get() < lock.locked_until_ms
    {
        return Ok(QuotaDecision::Rejected {
            locked_until_ms: lock.locked_until_ms,
        });
    }

    let lower_bound_ms = now.get() - TF_ADMISSION_WINDOW_MS;
    let recent_count: i64 = transaction.query_row(
        "SELECT COUNT(*) FROM ops_temporary_dispatch_requests WHERE workspace_id=?1 AND requesting_team_id=?2 AND admitted_at_ms>?3 AND admitted_at_ms<=?4",
        params![
            workspace_id.into_uuid().as_bytes(),
            requesting_team_id.as_bytes(),
            lower_bound_ms,
            now.get(),
        ],
        |row| row.get(0),
    )?;
    if recent_count < 0 {
        return Err(OperationsError::Corrupt(
            "temporary dispatch admission count",
        ));
    }
    match recent_count {
        0 | 1 => Ok(QuotaDecision::Admitted {
            lock_created_until_ms: None,
            next_lock: lock,
        }),
        2 => Ok(QuotaDecision::Admitted {
            lock_created_until_ms: Some(next_lock_boundary),
            next_lock: Some(LockAnchor {
                request_id,
                locked_until_ms: next_lock_boundary,
            }),
        }),
        _ => Err(OperationsError::Corrupt(
            "temporary dispatch quota lock missing",
        )),
    }
}

fn persist_quota_state(
    transaction: &Transaction<'_>,
    workspace_id: WorkspaceId,
    requesting_team_id: Uuid,
    state: QuotaState,
) -> Result<(), OperationsError> {
    let [Some(recent_1), recent_2, recent_3] = state.recent else {
        return Err(OperationsError::Corrupt(
            "temporary dispatch quota persisted anchors",
        ));
    };
    transaction.execute(
        "INSERT INTO ops_temporary_dispatch_quota_state(workspace_id,requesting_team_id,last_admitted_at_ms,last_sequence,recent_1_request_id,recent_1_sequence,recent_1_admitted_at_ms,recent_2_request_id,recent_2_sequence,recent_2_admitted_at_ms,recent_3_request_id,recent_3_sequence,recent_3_admitted_at_ms,locked_until_ms,lock_request_id) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15) ON CONFLICT(workspace_id,requesting_team_id) DO UPDATE SET last_admitted_at_ms=excluded.last_admitted_at_ms,last_sequence=excluded.last_sequence,recent_1_request_id=excluded.recent_1_request_id,recent_1_sequence=excluded.recent_1_sequence,recent_1_admitted_at_ms=excluded.recent_1_admitted_at_ms,recent_2_request_id=excluded.recent_2_request_id,recent_2_sequence=excluded.recent_2_sequence,recent_2_admitted_at_ms=excluded.recent_2_admitted_at_ms,recent_3_request_id=excluded.recent_3_request_id,recent_3_sequence=excluded.recent_3_sequence,recent_3_admitted_at_ms=excluded.recent_3_admitted_at_ms,locked_until_ms=excluded.locked_until_ms,lock_request_id=excluded.lock_request_id",
        params![
            workspace_id.into_uuid().as_bytes(),
            requesting_team_id.as_bytes(),
            state.last_admitted_at_ms,
            state.last_sequence,
            recent_1.request_id.as_bytes(),
            recent_1.sequence,
            recent_1.admitted_at_ms,
            recent_2.map(|anchor| anchor.request_id.into_bytes().to_vec()),
            recent_2.map(|anchor| anchor.sequence),
            recent_2.map(|anchor| anchor.admitted_at_ms),
            recent_3.map(|anchor| anchor.request_id.into_bytes().to_vec()),
            recent_3.map(|anchor| anchor.sequence),
            recent_3.map(|anchor| anchor.admitted_at_ms),
            state.lock.map(|lock| lock.locked_until_ms),
            state.lock.map(|lock| lock.request_id.into_bytes().to_vec()),
        ],
    )?;
    Ok(())
}

fn insert_request(
    transaction: &Transaction<'_>,
    request: &TemporaryDispatchAdmissionRequest,
    actor: &TemporaryDispatchActorProvenance,
    target_home_team_id: Uuid,
    admission_sequence: i64,
    admitted_at_ms: i64,
    lock_created_until_ms: Option<i64>,
) -> Result<(), OperationsError> {
    let (activity_id, agent_id, session_id) = match request.actor() {
        super::OperationalActor::Participant {
            activity_id,
            agent_id,
            session_id,
        } => (activity_id, agent_id, session_id),
        super::OperationalActor::LocalOperator => {
            return Err(OperationsError::Conflict(
                "temporary dispatch requires a participant actor",
            ));
        }
    };
    transaction.execute(
        "INSERT INTO ops_temporary_dispatch_requests(request_id,admission_sequence,workspace_id,requesting_team_id,leadership_revision,actor_activity_id,actor_agent_id,actor_session_id,actor_participation_id,actor_profile_id,actor_participant_id,actor_display_name,target_participant_id,target_home_team_id,requested_scope,requested_end_at_ms,lock_created_until_ms,admitted_at_ms) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18)",
        params![
            request.request_id().as_bytes(),
            admission_sequence,
            request.workspace_id().into_uuid().as_bytes(),
            request.requesting_team_id().as_bytes(),
            i64::try_from(request.expected_leadership_revision())
                .map_err(|_| OperationsError::Bounds("Team leadership revision"))?,
            activity_id.into_uuid().as_bytes(),
            agent_id.into_uuid().as_bytes(),
            session_id.into_uuid().as_bytes(),
            actor.participation_id.into_uuid().as_bytes(),
            actor.profile_id.as_bytes(),
            actor.participant_id.as_bytes(),
            actor.display_name,
            request.target_participant_id().as_bytes(),
            target_home_team_id.as_bytes(),
            request.requested_scope().as_str(),
            request.requested_end_at().get(),
            lock_created_until_ms,
            admitted_at_ms,
        ],
    )?;
    Ok(())
}

#[cfg(test)]
pub(super) fn seed_receipt_sequence_for_test(
    transaction: &Transaction<'_>,
    request: &TemporaryDispatchAdmissionRequest,
    actor: &TemporaryDispatchActorProvenance,
    target_home_team_id: Uuid,
    sequence: i64,
    admitted_at: TimestampMillis,
) -> Result<(), OperationsError> {
    if sequence <= 0 {
        return Err(OperationsError::Bounds(
            "temporary dispatch admission sequence",
        ));
    }
    insert_request(
        transaction,
        request,
        actor,
        target_home_team_id,
        sequence,
        admitted_at.get(),
        None,
    )?;
    persist_quota_state(
        transaction,
        request.workspace_id(),
        request.requesting_team_id(),
        QuotaState {
            last_admitted_at_ms: admitted_at.get(),
            last_sequence: sequence,
            recent: [
                Some(ReceiptAnchor {
                    request_id: request.request_id(),
                    sequence,
                    admitted_at_ms: admitted_at.get(),
                }),
                None,
                None,
            ],
            lock: None,
        },
    )?;
    Ok(())
}

fn timestamp(value: i64, label: &'static str) -> Result<TimestampMillis, OperationsError> {
    TimestampMillis::new(value).map_err(|_| OperationsError::Corrupt(label))
}
