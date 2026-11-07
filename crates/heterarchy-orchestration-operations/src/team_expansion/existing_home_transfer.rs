//! Immutable source membership observed by a V3 Team-expansion approval.
//!
//! This source proves only that the selected Profile had one exact JOINED Home
//! membership when the Workspace Orchestrator approved the destination hold.
//! It is not consent, transfer cutover, work custody, or assignment authority.

use rusqlite::{Connection, OptionalExtension, Transaction, params};
use uuid::Uuid;

use super::{
    TeamExpansionApprovalSlotPlanV3, TeamExpansionDecisionReceipt, TeamExpansionWorkspaceTargetV3,
    TimestampMillis, WorkspaceId, WorkspaceParticipationId, wire,
};
use crate::OperationsError;

const MANIFESTS: &str = "CREATE TABLE ops_team_expansion_v3_manifests(decision_id BLOB PRIMARY KEY NOT NULL CHECK(length(decision_id)=16 AND decision_id!=zeroblob(16)),request_id BLOB NOT NULL CHECK(length(request_id)=16 AND request_id!=zeroblob(16)),workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16),team_id BLOB NOT NULL CHECK(length(team_id)=16 AND team_id!=zeroblob(16)),slot_count INTEGER NOT NULL CHECK(slot_count BETWEEN 1 AND 8),transfer_count INTEGER NOT NULL CHECK(transfer_count BETWEEN 1 AND slot_count),recorded_at_ms INTEGER NOT NULL CHECK(recorded_at_ms>=0),UNIQUE(workspace_id,team_id,decision_id),UNIQUE(workspace_id,team_id,decision_id,request_id),FOREIGN KEY(workspace_id,team_id,decision_id,request_id) REFERENCES ops_team_expansion_decisions(workspace_id,team_id,decision_id,request_id))";
const MANIFESTS_NO_UPDATE: &str = "CREATE TRIGGER ops_team_expansion_v3_manifests_no_update BEFORE UPDATE ON ops_team_expansion_v3_manifests BEGIN SELECT RAISE(ABORT,'Team expansion V3 manifests are immutable'); END";
const MANIFESTS_NO_DELETE: &str = "CREATE TRIGGER ops_team_expansion_v3_manifests_no_delete BEFORE DELETE ON ops_team_expansion_v3_manifests BEGIN SELECT RAISE(ABORT,'Team expansion V3 manifests are immutable'); END";

const TRANSFERS: &str = "CREATE TABLE ops_team_expansion_existing_home_transfers(decision_id BLOB NOT NULL CHECK(length(decision_id)=16 AND decision_id!=zeroblob(16)),ordinal INTEGER NOT NULL CHECK(ordinal BETWEEN 0 AND 7),workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16),destination_team_id BLOB NOT NULL CHECK(length(destination_team_id)=16 AND destination_team_id!=zeroblob(16)),target_profile_id BLOB NOT NULL CHECK(length(target_profile_id)=16 AND target_profile_id!=zeroblob(16)),target_participation_id BLOB NOT NULL CHECK(length(target_participation_id)=16 AND target_participation_id!=zeroblob(16)),source_team_id BLOB NOT NULL CHECK(length(source_team_id)=16 AND source_team_id!=zeroblob(16)),source_membership_revision INTEGER NOT NULL CHECK(source_membership_revision>=1),PRIMARY KEY(decision_id,ordinal),UNIQUE(workspace_id,decision_id,ordinal),FOREIGN KEY(workspace_id,destination_team_id,decision_id) REFERENCES ops_team_expansion_v3_manifests(workspace_id,team_id,decision_id),FOREIGN KEY(workspace_id,destination_team_id,decision_id,ordinal) REFERENCES ops_team_expansion_workspace_hold_plans(workspace_id,team_id,decision_id,ordinal),FOREIGN KEY(workspace_id,source_team_id,target_participation_id) REFERENCES ops_team_members(workspace_id,team_id,participation_id),CHECK(source_team_id!=destination_team_id))";
const TRANSFERS_NO_UPDATE: &str = "CREATE TRIGGER ops_team_expansion_existing_home_transfers_no_update BEFORE UPDATE ON ops_team_expansion_existing_home_transfers BEGIN SELECT RAISE(ABORT,'Team expansion existing Home transfer sources are immutable'); END";
const TRANSFERS_NO_DELETE: &str = "CREATE TRIGGER ops_team_expansion_existing_home_transfers_no_delete BEFORE DELETE ON ops_team_expansion_existing_home_transfers BEGIN SELECT RAISE(ABORT,'Team expansion existing Home transfer sources are immutable'); END";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct ExistingHomeTransferSource {
    pub(super) ordinal: u8,
    pub(super) profile_id: Uuid,
    pub(super) participation_id: WorkspaceParticipationId,
    pub(super) source_team_id: Uuid,
    pub(super) source_membership_revision: u64,
}

pub(super) const fn schema_objects() -> &'static [(&'static str, &'static str, &'static str)] {
    &[
        ("table", "ops_team_expansion_v3_manifests", MANIFESTS),
        (
            "trigger",
            "ops_team_expansion_v3_manifests_no_update",
            MANIFESTS_NO_UPDATE,
        ),
        (
            "trigger",
            "ops_team_expansion_v3_manifests_no_delete",
            MANIFESTS_NO_DELETE,
        ),
        (
            "table",
            "ops_team_expansion_existing_home_transfers",
            TRANSFERS,
        ),
        (
            "trigger",
            "ops_team_expansion_existing_home_transfers_no_update",
            TRANSFERS_NO_UPDATE,
        ),
        (
            "trigger",
            "ops_team_expansion_existing_home_transfers_no_delete",
            TRANSFERS_NO_DELETE,
        ),
    ]
}

pub(super) fn create_schema(transaction: &Transaction<'_>) -> Result<(), OperationsError> {
    for &(_, _, statement) in schema_objects() {
        transaction.execute_batch(statement)?;
    }
    Ok(())
}

/// Fresh ordinary assignment/join must not consume a slot approved for an
/// existing Home transfer. Historical reads/replays do not use this check.
pub(super) fn ensure_generic_slot_admission(
    connection: &Connection,
    workspace_id: WorkspaceId,
    team_id: Uuid,
    slot: super::TeamExpansionSlotRef,
) -> Result<(), OperationsError> {
    let manifest = has_manifest(connection, workspace_id, slot.approval_id)?;
    if !manifest {
        let orphan_source: bool = connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM ops_team_expansion_existing_home_transfers WHERE workspace_id=?1 AND decision_id=?2)",
            params![workspace_id.into_uuid().as_bytes(), slot.approval_id.as_bytes()],
            |row| row.get(0),
        )?;
        return if orphan_source {
            Err(OperationsError::Corrupt(
                "Team expansion V3 manifest missing",
            ))
        } else {
            Ok(())
        };
    }
    let decision =
        super::decision::load_decision_by_id(connection, workspace_id, slot.approval_id, false)?
            .ok_or(OperationsError::Corrupt(
                "Team expansion V3 decision missing",
            ))?;
    if decision.team_id != team_id {
        return Err(OperationsError::Conflict("Team expansion V3 slot Team"));
    }
    // Loading the bounded plan checks source cardinality. A missing transfer
    // row must not silently turn into an ordinary Profile slot.
    let plan = load_original_plan(connection, &decision)?;
    if !source_matches_receipt(connection, &decision, &sources_from_v3(&plan))? {
        return Err(OperationsError::Corrupt(
            "Team expansion V3 source mismatch",
        ));
    }
    let selected = plan
        .get(usize::from(slot.ordinal))
        .ok_or(OperationsError::Conflict("Team expansion V3 slot ordinal"))?;
    if matches!(
        selected.target,
        TeamExpansionWorkspaceTargetV3::ExistingHomeTransfer { .. }
    ) {
        return Err(OperationsError::Conflict(
            "existing Home transfer requires atomic cutover",
        ));
    }
    Ok(())
}

pub(super) fn sources_from_v3(
    plans: &[TeamExpansionApprovalSlotPlanV3],
) -> Vec<ExistingHomeTransferSource> {
    plans
        .iter()
        .filter_map(|plan| match plan.target {
            TeamExpansionWorkspaceTargetV3::ExistingHomeTransfer {
                profile_id,
                participation_id,
                source_team_id,
                source_membership_revision,
            } => Some(ExistingHomeTransferSource {
                ordinal: plan.ordinal,
                profile_id,
                participation_id,
                source_team_id,
                source_membership_revision,
            }),
            TeamExpansionWorkspaceTargetV3::ExistingWorkspaceProfile { .. }
            | TeamExpansionWorkspaceTargetV3::NewProfile => None,
        })
        .collect()
}

pub(super) fn validate_current_sources(
    connection: &Connection,
    workspace_id: WorkspaceId,
    destination_team_id: Uuid,
    sources: &[ExistingHomeTransferSource],
) -> Result<(), OperationsError> {
    if sources.is_empty() {
        return Err(OperationsError::Conflict(
            "Team expansion V3 requires an existing Home transfer",
        ));
    }
    for source in sources {
        if source.profile_id.is_nil()
            || source.source_team_id.is_nil()
            || source.source_team_id == destination_team_id
            || source.source_membership_revision == 0
            || i64::try_from(source.source_membership_revision).is_err()
        {
            return Err(OperationsError::Bounds(
                "Team expansion existing Home transfer source",
            ));
        }
        let mut statement = connection.prepare(
            "SELECT member.team_id,member.participation_id,member.revision
             FROM ops_team_members AS member
             JOIN ops_teams AS home
               ON home.workspace_id=member.workspace_id AND home.team_id=member.team_id
             JOIN ops_workspace_participations AS participation
               ON participation.workspace_id=member.workspace_id
              AND participation.participation_id=member.participation_id
             WHERE member.workspace_id=?1 AND participation.agent_profile_id=?2
               AND member.state='JOINED' AND home.state='ACTIVE'
             ORDER BY member.team_id,member.participation_id LIMIT 2",
        )?;
        let rows = statement
            .query_map(
                params![
                    workspace_id.into_uuid().as_bytes(),
                    source.profile_id.as_bytes(),
                ],
                |row| {
                    Ok((
                        row.get::<_, Vec<u8>>(0)?,
                        row.get::<_, Vec<u8>>(1)?,
                        row.get::<_, i64>(2)?,
                    ))
                },
            )?
            .collect::<Result<Vec<_>, _>>()?;
        let [(team, participation, revision)] = rows.as_slice() else {
            return Err(OperationsError::Conflict(
                "Team expansion transfer target must have one Home Team",
            ));
        };
        if Uuid::from_bytes(wire::id(team.clone())?) != source.source_team_id
            || Uuid::from_bytes(wire::id(participation.clone())?)
                != source.participation_id.into_uuid()
            || u64::try_from(*revision).ok() != Some(source.source_membership_revision)
        {
            return Err(OperationsError::Conflict(
                "Team expansion transfer source membership changed",
            ));
        }
    }
    Ok(())
}

pub(super) fn ensure_source_time_not_regressed(
    connection: &Connection,
    workspace_id: WorkspaceId,
    sources: &[ExistingHomeTransferSource],
    recorded_at: TimestampMillis,
) -> Result<(), OperationsError> {
    for source in sources {
        let (team_updated_at, joined_at): (i64, i64) = connection.query_row(
            "SELECT home.updated_at_ms,member.joined_at_ms
             FROM ops_team_members AS member
             JOIN ops_teams AS home
               ON home.workspace_id=member.workspace_id AND home.team_id=member.team_id
             WHERE member.workspace_id=?1 AND member.team_id=?2
               AND member.participation_id=?3 AND member.state='JOINED'",
            params![
                workspace_id.into_uuid().as_bytes(),
                source.source_team_id.as_bytes(),
                source.participation_id.into_uuid().as_bytes(),
            ],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        if recorded_at.get() < team_updated_at || recorded_at.get() < joined_at {
            return Err(OperationsError::Conflict(
                "Team expansion trusted clock regressed",
            ));
        }
    }
    Ok(())
}

pub(super) struct V3SourceInsert<'a> {
    pub(super) workspace_id: WorkspaceId,
    pub(super) destination_team_id: Uuid,
    pub(super) decision_id: Uuid,
    pub(super) request_id: Uuid,
    pub(super) slot_count: u8,
    pub(super) sources: &'a [ExistingHomeTransferSource],
    pub(super) recorded_at: TimestampMillis,
}

pub(super) fn insert_sources(
    transaction: &Transaction<'_>,
    source: &V3SourceInsert<'_>,
) -> Result<(), OperationsError> {
    transaction.execute(
        "INSERT INTO ops_team_expansion_v3_manifests(decision_id,request_id,workspace_id,team_id,slot_count,transfer_count,recorded_at_ms) VALUES(?1,?2,?3,?4,?5,?6,?7)",
        params![
            source.decision_id.as_bytes(),
            source.request_id.as_bytes(),
            source.workspace_id.into_uuid().as_bytes(),
            source.destination_team_id.as_bytes(),
            i64::from(source.slot_count),
            i64::try_from(source.sources.len())
                .map_err(|_| OperationsError::Bounds("Team expansion V3 transfer count"))?,
            source.recorded_at.get(),
        ],
    )?;
    for transfer in source.sources {
        transaction.execute(
            "INSERT INTO ops_team_expansion_existing_home_transfers(decision_id,ordinal,workspace_id,destination_team_id,target_profile_id,target_participation_id,source_team_id,source_membership_revision) VALUES(?1,?2,?3,?4,?5,?6,?7,?8)",
            params![
                source.decision_id.as_bytes(),
                i64::from(transfer.ordinal),
                source.workspace_id.into_uuid().as_bytes(),
                source.destination_team_id.as_bytes(),
                transfer.profile_id.as_bytes(),
                transfer.participation_id.into_uuid().as_bytes(),
                transfer.source_team_id.as_bytes(),
                i64::try_from(transfer.source_membership_revision).map_err(|_| {
                    OperationsError::Bounds("Team expansion source membership revision")
                })?,
            ],
        )?;
    }
    Ok(())
}

pub(super) fn has_manifest(
    connection: &Connection,
    workspace_id: WorkspaceId,
    decision_id: Uuid,
) -> Result<bool, OperationsError> {
    Ok(connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM ops_team_expansion_v3_manifests WHERE workspace_id=?1 AND decision_id=?2)",
        params![workspace_id.into_uuid().as_bytes(), decision_id.as_bytes()],
        |row| row.get(0),
    )?)
}

pub(super) fn source_matches_intent(
    connection: &Connection,
    command: &super::TeamExpansionDecisionCommand,
    sources: &[ExistingHomeTransferSource],
) -> Result<bool, OperationsError> {
    source_matches(
        connection,
        command.workspace_id,
        command.decision_id,
        command.request_id,
        command.team_id,
        approved_count(command),
        sources,
    )
}

pub(super) fn source_matches_receipt(
    connection: &Connection,
    decision: &TeamExpansionDecisionReceipt,
    sources: &[ExistingHomeTransferSource],
) -> Result<bool, OperationsError> {
    source_matches(
        connection,
        decision.workspace_id,
        decision.decision_id,
        decision.request_id,
        decision.team_id,
        Some(decision.approved_count),
        sources,
    )
}

fn source_matches(
    connection: &Connection,
    workspace_id: WorkspaceId,
    decision_id: Uuid,
    request_id: Uuid,
    team_id: Uuid,
    approved_count: Option<u8>,
    sources: &[ExistingHomeTransferSource],
) -> Result<bool, OperationsError> {
    let Some((request, team, slots, transfers)): Option<(Vec<u8>, Vec<u8>, i64, i64)> = connection
        .query_row(
            "SELECT request_id,team_id,slot_count,transfer_count FROM ops_team_expansion_v3_manifests WHERE workspace_id=?1 AND decision_id=?2",
            params![
                workspace_id.into_uuid().as_bytes(),
                decision_id.as_bytes(),
            ],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .optional()?
    else {
        return Ok(false);
    };
    if Uuid::from_bytes(wire::id(request)?) != request_id
        || Uuid::from_bytes(wire::id(team)?) != team_id
        || u8::try_from(slots).ok() != approved_count
        || usize::try_from(transfers).ok() != Some(sources.len())
    {
        return Ok(false);
    }
    let mut statement = connection.prepare(
        "SELECT ordinal,target_profile_id,target_participation_id,source_team_id,source_membership_revision FROM ops_team_expansion_existing_home_transfers WHERE workspace_id=?1 AND destination_team_id=?2 AND decision_id=?3 ORDER BY ordinal",
    )?;
    let stored = statement
        .query_map(
            params![
                workspace_id.into_uuid().as_bytes(),
                team_id.as_bytes(),
                decision_id.as_bytes(),
            ],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, Vec<u8>>(1)?,
                    row.get::<_, Vec<u8>>(2)?,
                    row.get::<_, Vec<u8>>(3)?,
                    row.get::<_, i64>(4)?,
                ))
            },
        )?
        .collect::<Result<Vec<_>, _>>()?;
    if stored.len() != sources.len() {
        return Ok(false);
    }
    for (stored, expected) in stored.iter().zip(sources) {
        if u8::try_from(stored.0).ok() != Some(expected.ordinal)
            || Uuid::from_bytes(wire::id(stored.1.clone())?) != expected.profile_id
            || Uuid::from_bytes(wire::id(stored.2.clone())?)
                != expected.participation_id.into_uuid()
            || Uuid::from_bytes(wire::id(stored.3.clone())?) != expected.source_team_id
            || u64::try_from(stored.4).ok() != Some(expected.source_membership_revision)
        {
            return Ok(false);
        }
    }
    Ok(true)
}

fn approved_count(command: &super::TeamExpansionDecisionCommand) -> Option<u8> {
    match command.decision {
        super::TeamExpansionDecision::Approve { approved_count } => Some(approved_count),
        super::TeamExpansionDecision::Reject => None,
    }
}

struct StoredV3PlanRaw {
    ordinal: i64,
    target_kind: i64,
    target_profile: Option<Vec<u8>>,
    target_participation: Option<Vec<u8>>,
    source_team: Option<Vec<u8>>,
    source_membership_revision: Option<i64>,
}

pub(super) fn load_original_plan(
    connection: &Connection,
    decision: &TeamExpansionDecisionReceipt,
) -> Result<Vec<TeamExpansionApprovalSlotPlanV3>, OperationsError> {
    let (request, team, slot_count, transfer_count): (Vec<u8>, Vec<u8>, i64, i64) = connection
        .query_row(
            "SELECT request_id,team_id,slot_count,transfer_count FROM ops_team_expansion_v3_manifests WHERE workspace_id=?1 AND decision_id=?2",
            params![
                decision.workspace_id.into_uuid().as_bytes(),
                decision.decision_id.as_bytes(),
            ],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )?;
    let slot_count = u8::try_from(slot_count)
        .map_err(|_| OperationsError::Corrupt("Team expansion V3 slot count"))?;
    let transfer_count = usize::try_from(transfer_count)
        .map_err(|_| OperationsError::Corrupt("Team expansion V3 transfer count"))?;
    if Uuid::from_bytes(wire::id(request)?) != decision.request_id
        || Uuid::from_bytes(wire::id(team)?) != decision.team_id
        || slot_count != decision.approved_count
    {
        return Err(OperationsError::Corrupt(
            "Team expansion V3 manifest identity",
        ));
    }

    let mut statement = connection.prepare(
        "SELECT plan.ordinal,plan.target_kind,plan.target_profile_id,plan.target_participation_id,transfer.source_team_id,transfer.source_membership_revision
         FROM ops_team_expansion_workspace_hold_plans AS plan
         LEFT JOIN ops_team_expansion_existing_home_transfers AS transfer
           ON transfer.workspace_id=plan.workspace_id
          AND transfer.destination_team_id=plan.team_id
          AND transfer.decision_id=plan.decision_id
          AND transfer.ordinal=plan.ordinal
         WHERE plan.workspace_id=?1 AND plan.team_id=?2 AND plan.decision_id=?3
         ORDER BY plan.ordinal",
    )?;
    let rows = statement
        .query_map(
            params![
                decision.workspace_id.into_uuid().as_bytes(),
                decision.team_id.as_bytes(),
                decision.decision_id.as_bytes(),
            ],
            |row| {
                Ok(StoredV3PlanRaw {
                    ordinal: row.get(0)?,
                    target_kind: row.get(1)?,
                    target_profile: row.get(2)?,
                    target_participation: row.get(3)?,
                    source_team: row.get(4)?,
                    source_membership_revision: row.get(5)?,
                })
            },
        )?
        .collect::<Result<Vec<_>, _>>()?;
    if rows.len() != usize::from(slot_count) {
        return Err(OperationsError::Corrupt(
            "Team expansion V3 plan cardinality",
        ));
    }
    let mut plans = Vec::with_capacity(rows.len());
    let mut decoded_transfers = 0_usize;
    for (expected_ordinal, row) in (0..slot_count).zip(rows) {
        let ordinal = u8::try_from(row.ordinal)
            .map_err(|_| OperationsError::Corrupt("Team expansion V3 plan ordinal"))?;
        if ordinal != expected_ordinal {
            return Err(OperationsError::Corrupt("Team expansion V3 plan ordinal"));
        }
        let target = decode_stored_target(&row)?;
        if matches!(
            target,
            TeamExpansionWorkspaceTargetV3::ExistingHomeTransfer { .. }
        ) {
            decoded_transfers = decoded_transfers
                .checked_add(1)
                .ok_or(OperationsError::Corrupt("Team expansion V3 transfer count"))?;
        }
        plans.push(TeamExpansionApprovalSlotPlanV3::new(ordinal, target));
    }
    if decoded_transfers != transfer_count {
        return Err(OperationsError::Corrupt(
            "Team expansion V3 transfer cardinality",
        ));
    }
    Ok(plans)
}

fn decode_stored_target(
    row: &StoredV3PlanRaw,
) -> Result<TeamExpansionWorkspaceTargetV3, OperationsError> {
    let profile = row
        .target_profile
        .clone()
        .map(|value| wire::id(value).map(Uuid::from_bytes))
        .transpose()?;
    let participation = row
        .target_participation
        .clone()
        .map(|value| wire::id(value).map(Uuid::from_bytes))
        .transpose()?;
    let source_team = row
        .source_team
        .clone()
        .map(|value| wire::id(value).map(Uuid::from_bytes))
        .transpose()?;
    match (
        row.target_kind,
        profile,
        participation,
        source_team,
        row.source_membership_revision,
    ) {
        (0, Some(profile_id), Some(participation_id), None, None) => {
            Ok(TeamExpansionWorkspaceTargetV3::ExistingWorkspaceProfile {
                profile_id,
                participation_id: WorkspaceParticipationId::from_uuid(participation_id).map_err(
                    |_| OperationsError::Corrupt("Team expansion V3 participation identity"),
                )?,
            })
        }
        (0, Some(profile_id), Some(participation_id), Some(source_team_id), Some(revision)) => {
            Ok(TeamExpansionWorkspaceTargetV3::ExistingHomeTransfer {
                profile_id,
                participation_id: WorkspaceParticipationId::from_uuid(participation_id).map_err(
                    |_| OperationsError::Corrupt("Team expansion V3 participation identity"),
                )?,
                source_team_id,
                source_membership_revision: u64::try_from(revision).map_err(|_| {
                    OperationsError::Corrupt("Team expansion V3 source membership revision")
                })?,
            })
        }
        (2, None, None, None, None) => Ok(TeamExpansionWorkspaceTargetV3::NewProfile),
        _ => Err(OperationsError::Corrupt(
            "Team expansion V3 stored target shape",
        )),
    }
}
