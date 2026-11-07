use forge_domain::{
    ActivityId, EvaluationDigest, EvaluationManifestMember, EvaluationOriginalBindingStatus,
    EvaluationSourceIdentity,
};
use rusqlite::{Connection, params};

use crate::{OperationsError, WorkspaceParticipationId};

#[derive(Debug)]
struct PriorSourceBindingRow {
    activity_id: Vec<u8>,
    obligation_id: Vec<u8>,
    work_unit_id: Vec<u8>,
    participation_id: Vec<u8>,
    weight: i64,
    due_at_ms: i64,
    assignment_id: Vec<u8>,
    assignment_revision: i64,
    target_agent_id: Vec<u8>,
    target_session_id: Vec<u8>,
    criteria_digest: Vec<u8>,
    binding_state: i64,
    binding_digest: Option<Vec<u8>>,
    binding_revision: Option<i64>,
    binding_sequence: Option<i64>,
}

/// Frozen local/source terms whose identity must remain stable over later Plans.
#[derive(Debug, Clone, Copy)]
pub(super) struct SourceMemberHistoryExpectation {
    pub(super) source: EvaluationSourceIdentity,
    pub(super) activity_id: ActivityId,
    pub(super) declaration: EvaluationManifestMember,
    pub(super) participation_id: WorkspaceParticipationId,
    pub(super) weight: u64,
    pub(super) due_at_ms: i64,
    pub(super) criteria_digest: EvaluationDigest,
    /// `None` is used before the source seal exists; prior observations must
    /// still agree with one another.
    pub(super) expected_original_binding: Option<EvaluationOriginalBindingStatus>,
}

/// Validate any prior source member sharing an obligation, unit, or assignment.
pub(super) fn check_prior_source_member_history(
    connection: &Connection,
    expected: &SourceMemberHistoryExpectation,
) -> Result<(), OperationsError> {
    if expected.declaration.criteria_digest() != expected.criteria_digest {
        return Err(OperationsError::Conflict(
            "mission evidence source criteria digest mismatch",
        ));
    }
    let existing = load_prior_source_binding_rows(connection, expected)?;
    validate_prior_source_binding_rows(existing, expected)
}

fn load_prior_source_binding_rows(
    connection: &Connection,
    expected: &SourceMemberHistoryExpectation,
) -> Result<Vec<PriorSourceBindingRow>, OperationsError> {
    let source = expected.source;
    let declaration = expected.declaration;
    let mut statement = connection.prepare(
        "SELECT p.activity_id AS activity_id,m.obligation_id AS obligation_id,m.work_unit_id AS work_unit_id,m.participation_id AS participation_id,m.weight AS weight,m.due_at_ms AS due_at_ms,m.assignment_id AS assignment_id,m.assignment_revision AS assignment_revision,m.target_agent_id AS target_agent_id,m.target_session_id AS target_session_id,m.criteria_digest AS criteria_digest,m.original_binding_state AS binding_state,m.original_binding_digest AS binding_digest,m.original_binding_revision AS binding_revision,m.original_binding_sequence AS binding_sequence FROM ops_mission_evidence_bindings b JOIN ops_mission_evidence_binding_partitions p ON p.binding_request_id=b.request_id JOIN ops_mission_evidence_binding_members m ON m.binding_request_id=p.binding_request_id AND m.partition_ordinal=p.partition_ordinal WHERE b.source_backend_id=?1 AND b.source_workspace_id=?2 AND b.source_migration_id=?3 AND b.source_destination_id=?4 AND b.source_freeze_revision=?5 AND b.source_delegation_digest=?6 AND (m.obligation_id=?7 OR m.work_unit_id=?8 OR m.assignment_id=?9) ORDER BY p.activity_id,b.request_id,m.assignment_id LIMIT 257",
    )?;
    let rows = statement.query_map(
        params![
            source.backend_id().into_uuid().as_bytes(),
            source.workspace_id().into_uuid().as_bytes(),
            source.delegation_migration_id().as_bytes(),
            source.delegation_destination_id().as_bytes(),
            source.delegation_freeze_revision().get(),
            source.delegation_manifest_digest().as_bytes(),
            declaration.obligation_id().into_uuid().as_bytes(),
            declaration.work_unit_id().into_uuid().as_bytes(),
            declaration.assignment_id().into_uuid().as_bytes(),
        ],
        |row| {
            Ok(PriorSourceBindingRow {
                activity_id: row.get("activity_id")?,
                obligation_id: row.get("obligation_id")?,
                work_unit_id: row.get("work_unit_id")?,
                participation_id: row.get("participation_id")?,
                weight: row.get("weight")?,
                due_at_ms: row.get("due_at_ms")?,
                assignment_id: row.get("assignment_id")?,
                assignment_revision: row.get("assignment_revision")?,
                target_agent_id: row.get("target_agent_id")?,
                target_session_id: row.get("target_session_id")?,
                criteria_digest: row.get("criteria_digest")?,
                binding_state: row.get("binding_state")?,
                binding_digest: row.get("binding_digest")?,
                binding_revision: row.get("binding_revision")?,
                binding_sequence: row.get("binding_sequence")?,
            })
        },
    )?;
    let existing = rows.collect::<Result<Vec<_>, _>>()?;
    if existing.len() > 256 {
        return Err(OperationsError::Bounds(
            "mission evidence original binding history",
        ));
    }
    Ok(existing)
}

fn validate_prior_source_binding_rows(
    existing: Vec<PriorSourceBindingRow>,
    expected: &SourceMemberHistoryExpectation,
) -> Result<(), OperationsError> {
    let declaration = expected.declaration;
    let mut observed_original_binding = None;
    for prior in existing {
        if decode_uuid(prior.activity_id, "mission evidence prior Activity")?
            != expected.activity_id.into_uuid()
            || decode_uuid(prior.obligation_id, "mission evidence prior obligation")?
                != declaration.obligation_id().into_uuid()
            || decode_uuid(prior.work_unit_id, "mission evidence prior work unit")?
                != declaration.work_unit_id().into_uuid()
            || WorkspaceParticipationId::from_uuid(decode_uuid(
                prior.participation_id,
                "mission evidence prior participation",
            )?)
            .map_err(|_| OperationsError::Corrupt("mission evidence prior participation"))?
                != expected.participation_id
            || positive(prior.weight, "mission evidence prior weight")? != expected.weight
            || nonnegative(prior.due_at_ms, "mission evidence prior due time")?
                != expected.due_at_ms
            || decode_uuid(prior.assignment_id, "mission evidence prior assignment")?
                != declaration.assignment_id().into_uuid()
            || positive(
                prior.assignment_revision,
                "mission evidence prior assignment revision",
            )? != u64::try_from(declaration.assignment_revision().get())
                .map_err(|_| OperationsError::Corrupt("mission evidence prior revision"))?
            || decode_uuid(prior.target_agent_id, "mission evidence prior target Agent")?
                != declaration.target_agent_id().into_uuid()
            || decode_uuid(
                prior.target_session_id,
                "mission evidence prior target Session",
            )? != declaration.target_session_id().into_uuid()
            || decode_digest(
                prior.criteria_digest,
                "mission evidence prior criteria digest",
            )? != *declaration.criteria_digest().as_bytes()
        {
            return Err(OperationsError::Conflict(
                "mission evidence original assignment identity changed",
            ));
        }
        let prior_original_binding = super::codec::decode_original_binding(
            prior.binding_state,
            prior.binding_digest,
            prior.binding_revision,
            prior.binding_sequence,
        )?;
        if observed_original_binding.is_some_and(|previous| previous != prior_original_binding) {
            return Err(OperationsError::Conflict(
                "mission evidence original binding history changed",
            ));
        }
        observed_original_binding = Some(prior_original_binding);
        if let Some(expected_binding) = expected.expected_original_binding {
            match (expected_binding, prior_original_binding) {
                (
                    EvaluationOriginalBindingStatus::UnknownHistorical,
                    EvaluationOriginalBindingStatus::UnknownHistorical,
                ) => {}
                (
                    EvaluationOriginalBindingStatus::Bound(expected),
                    EvaluationOriginalBindingStatus::Bound(prior),
                ) if expected == prior => {}
                (
                    EvaluationOriginalBindingStatus::Bound(_),
                    EvaluationOriginalBindingStatus::Bound(_),
                ) => {
                    return Err(OperationsError::Conflict(
                        "mission evidence original assignment binding changed",
                    ));
                }
                _ => {
                    return Err(OperationsError::Conflict(
                        "mission evidence original binding history changed",
                    ));
                }
            }
        }
    }
    Ok(())
}

fn decode_uuid(bytes: Vec<u8>, label: &'static str) -> Result<uuid::Uuid, OperationsError> {
    let bytes: [u8; 16] = bytes
        .try_into()
        .map_err(|_| OperationsError::Corrupt(label))?;
    let value = uuid::Uuid::from_bytes(bytes);
    if value.is_nil() {
        return Err(OperationsError::Corrupt(label));
    }
    Ok(value)
}

fn positive(value: i64, label: &'static str) -> Result<u64, OperationsError> {
    u64::try_from(value)
        .ok()
        .filter(|value| *value > 0)
        .ok_or(OperationsError::Corrupt(label))
}

fn nonnegative(value: i64, label: &'static str) -> Result<i64, OperationsError> {
    if value < 0 {
        return Err(OperationsError::Corrupt(label));
    }
    Ok(value)
}

fn decode_digest(bytes: Vec<u8>, label: &'static str) -> Result<[u8; 32], OperationsError> {
    let digest: [u8; 32] = bytes
        .try_into()
        .map_err(|_| OperationsError::Corrupt(label))?;
    if digest == [0; 32] {
        return Err(OperationsError::Corrupt(label));
    }
    Ok(digest)
}
