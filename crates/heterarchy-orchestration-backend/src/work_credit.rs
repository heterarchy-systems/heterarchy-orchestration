//! Axum adapter for the `WorkCredit` / score observation surface.
//!
//! Read-only projection of the durable credit ledger, score snapshots and
//! provenance rows. Forge evidence is observed here — it is never imported
//! as Forge authority, and nothing on this surface mutates the ledger.

use axum::extract::{Path, Query, State};
use axum::routing::{get, post};
use axum::{Extension, Json, Router};
use forge_domain::AuthenticatedPrincipal;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::BackendState;
use crate::app_support::{LaneError, now_millis};
use crate::wire::work_credit::{
    WorkCreditFulfillmentWire, WorkCreditProvenanceWire, WorkCreditScoreQueryRequest,
    WorkCreditScoreQueryResponse, WorkCreditScoreWire, WorkCreditSubjectFulfillmentWire,
    WorkCreditSubjectIdentityWire, WorkCreditSubjectKindWire, WorkCreditSubjectScoreWire,
};

/// `WorkCredit` observation routes; merge before the router takes its state.
pub(crate) fn routes() -> Router<BackendState> {
    Router::new()
        .route(
            "/internal/v1/workspaces/{workspace_id}/work-credits",
            get(entries),
        )
        .route(
            "/internal/v1/workspaces/{workspace_id}/work-credits/fulfillment",
            get(fulfillment),
        )
        .route(
            "/internal/v1/workspaces/{workspace_id}/work-credits/provenance",
            get(provenance),
        )
        .route(
            "/internal/v1/workspaces/{workspace_id}/work-credits/scores",
            get(scores),
        )
        .route(
            "/internal/v1/workspaces/{workspace_id}/work-credits/scores/query",
            post(scores_query),
        )
        .route(
            "/internal/v1/workspaces/{workspace_id}/work-credits/team-scores",
            get(team_scores),
        )
        .route(
            "/internal/v1/elections/{election_id}/work-credit-snapshot",
            get(election_snapshot),
        )
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ScoresQuery {
    team_id: Option<String>,
    /// Observation clock override; defaults to the adapter clock.
    now_ms: Option<i64>,
}

async fn entries(
    State(state): State<BackendState>,
    Path(workspace_id): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    headers: axum::http::HeaderMap,
) -> Result<Json<Vec<EntryWire>>, LaneError> {
    let session = crate::app_support::session_selector(&headers)?;
    let workspace = canonical_uuid(Some(workspace_id.as_str()))?;
    let rows = state
        .run_workspace_query(workspace, session, principal, move |runtime| {
            runtime.work_credit_entries(workspace)
        })
        .await?;
    Ok(Json(
        rows.iter()
            .map(|entry| EntryWire {
                credit_id: entry.credit_id,
                workspace_id: entry.workspace_id,
                assignment_id: entry.assignment_id,
                participant_id: entry.participant_id,
                agent_id: entry.agent_id,
                activity_id: entry.activity_id,
                goal_id: entry.goal_id,
                node_id: entry.node_id,
                budget: entry.budget,
                award_source_sequence: entry.award_source_sequence,
                evidence_execution_id: entry.evidence_execution_id,
                awarded_at_ms: entry.awarded_at_ms,
            })
            .collect(),
    ))
}

async fn fulfillment(
    State(state): State<BackendState>,
    Path(workspace_id): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    headers: axum::http::HeaderMap,
) -> Result<Json<FulfillmentWire>, LaneError> {
    let session = crate::app_support::session_selector(&headers)?;
    let workspace = canonical_uuid(Some(workspace_id.as_str()))?;
    let (completed, evaluated) = state
        .run_workspace_query(workspace, session, principal, move |runtime| {
            runtime.work_credit_fulfillment(workspace)
        })
        .await?;
    Ok(Json(FulfillmentWire {
        completed,
        evaluated,
    }))
}

async fn provenance(
    State(state): State<BackendState>,
    Path(workspace_id): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    headers: axum::http::HeaderMap,
) -> Result<Json<Vec<ProvenanceWire>>, LaneError> {
    let session = crate::app_support::session_selector(&headers)?;
    let workspace = canonical_uuid(Some(workspace_id.as_str()))?;
    let rows = state
        .run_workspace_query(workspace, session, principal, move |runtime| {
            runtime.work_credit_submit_provenance(workspace)
        })
        .await?;
    Ok(Json(
        rows.iter()
            .map(|row| ProvenanceWire {
                assignment_id: row.assignment_id,
                activity_id: row.activity_id,
                goal_id: row.goal_id,
                node_id: row.node_id,
                target_agent_id: row.target_agent_id,
                provenance: row.provenance.as_ref().map(|matched| MatchWire {
                    change_set_id: matched.change_set_id,
                    diff_id: matched.diff_id,
                    producer_agent_id: matched.producer_agent_id,
                    producer_session_id: matched.producer_session_id,
                    dimension: matched.dimension,
                }),
            })
            .collect(),
    ))
}

async fn scores(
    State(state): State<BackendState>,
    Path(workspace_id): Path<String>,
    Query(query): Query<ScoresQuery>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    headers: axum::http::HeaderMap,
) -> Result<Json<SnapshotWire>, LaneError> {
    let session = crate::app_support::session_selector(&headers)?;
    if query.team_id.is_some() {
        return Err(LaneError::InvalidIdentity);
    }
    let workspace = canonical_uuid(Some(workspace_id.as_str()))?;
    let now = query.now_ms.unwrap_or(now_millis()?);
    let snapshot = state
        .run_workspace_query(workspace, session, principal, move |runtime| {
            runtime.work_credit_scores(workspace, now)
        })
        .await?;
    Ok(Json(SnapshotWire::from_summary(&snapshot)))
}

async fn scores_query(
    State(state): State<BackendState>,
    Path(workspace_id): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(body): Json<WorkCreditScoreQueryRequest>,
) -> Result<Json<WorkCreditScoreQueryResponse>, LaneError> {
    crate::app_support::validate_workspace_reference(&body.scope, body.session_id)?;
    if canonical_uuid(Some(workspace_id.as_str()))? != body.scope.workspace_id {
        return Err(LaneError::InvalidIdentity);
    }
    let scope = body.scope.clone();
    let workspace = scope.workspace_id;
    let snapshot = state
        .run_workspace_reference_query(
            body.scope,
            body.session_id,
            principal,
            move |runtime, observed_at_ms| {
                runtime.work_credit_scores_bounded(workspace, observed_at_ms)
            },
        )
        .await?;
    Ok(Json(WorkCreditScoreQueryResponse {
        scope,
        snapshot: score_wire_from_summary(&snapshot),
    }))
}

async fn team_scores(
    State(state): State<BackendState>,
    Path(workspace_id): Path<String>,
    Query(query): Query<ScoresQuery>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    headers: axum::http::HeaderMap,
) -> Result<Json<SnapshotWire>, LaneError> {
    let session = crate::app_support::session_selector(&headers)?;
    let workspace = canonical_uuid(Some(workspace_id.as_str()))?;
    let team = canonical_uuid(query.team_id.as_deref())?;
    let now = query.now_ms.unwrap_or(now_millis()?);
    let snapshot = state
        .run_workspace_query(workspace, session, principal, move |runtime| {
            runtime.team_work_credit_scores(workspace, team, now)
        })
        .await?;
    Ok(Json(SnapshotWire::from_summary(&snapshot)))
}

async fn election_snapshot(
    State(state): State<BackendState>,
    Path(election_id): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    headers: axum::http::HeaderMap,
) -> Result<Json<Option<ElectionSnapshotWire>>, LaneError> {
    let session = crate::app_support::session_selector(&headers)?;
    let workspace = crate::app_support::workspace_selector(&headers)?;
    let election = canonical_uuid(Some(election_id.as_str()))?;
    let summary = state
        .run_resource_query(workspace, session, principal, move |runtime| {
            Ok(runtime
                .election_work_credit_snapshot(election)?
                .map(|summary| (summary.workspace_id, summary)))
        })
        .await?;
    Ok(Json(
        summary.as_ref().map(ElectionSnapshotWire::from_summary),
    ))
}

fn canonical_uuid(value: Option<&str>) -> Result<Uuid, LaneError> {
    let Some(value) = value else {
        return Err(LaneError::InvalidIdentity);
    };
    let parsed = Uuid::parse_str(value).map_err(|_| LaneError::InvalidIdentity)?;
    if parsed.is_nil() || parsed.as_hyphenated().to_string() != value {
        return Err(LaneError::InvalidIdentity);
    }
    Ok(parsed)
}

// --- wire projections ---

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct EntryWire {
    credit_id: Uuid,
    workspace_id: Uuid,
    assignment_id: Uuid,
    participant_id: Option<Uuid>,
    agent_id: Uuid,
    activity_id: Uuid,
    goal_id: Uuid,
    node_id: Uuid,
    budget: i64,
    award_source_sequence: u64,
    evidence_execution_id: Uuid,
    awarded_at_ms: i64,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct FulfillmentWire {
    completed: u64,
    evaluated: u64,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ProvenanceWire {
    assignment_id: Uuid,
    activity_id: Uuid,
    goal_id: Uuid,
    node_id: Uuid,
    target_agent_id: Uuid,
    provenance: Option<MatchWire>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct MatchWire {
    change_set_id: Uuid,
    diff_id: Uuid,
    producer_agent_id: Uuid,
    producer_session_id: Option<Uuid>,
    dimension: &'static str,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct SnapshotWire {
    workspace_id: Uuid,
    team_id: Option<Uuid>,
    window_ms: i64,
    per_subject: Vec<SubjectWire>,
    total_credits: i64,
    subject_arithmetic_mean: i64,
    subject_mean_remainder_numerator: i64,
    mean_subject_count: i64,
    subject_total_credits: i64,
    unattributed_count: i64,
    fulfillment_completed: i64,
    fulfillment_evaluated: i64,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct SubjectWire {
    kind: &'static str,
    subject_id: Uuid,
    profile_id: Option<Uuid>,
    source_participant_count: u64,
    total_credits: i64,
    entry_count: i64,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ElectionSnapshotWire {
    election_id: Uuid,
    workspace_id: Uuid,
    team_id: Option<Uuid>,
    candidate_participant_id: Uuid,
    subject_kind: &'static str,
    subject_id: Uuid,
    source_participant_count: u64,
    window_ms: i64,
    evaluated_at_ms: i64,
    candidate_total_credits: i64,
    workspace_total_credits: i64,
    workspace_arithmetic_mean: i64,
    mean_remainder_numerator: i64,
    mean_subject_count: i64,
    qualified: bool,
    snapshot_digest: String,
}

impl ElectionSnapshotWire {
    fn from_summary(
        summary: &heterarchy_orchestration_runtime::work_credit::ElectionSnapshotSummary,
    ) -> Self {
        Self {
            election_id: summary.election_id,
            workspace_id: summary.workspace_id,
            team_id: summary.team_id,
            candidate_participant_id: summary.candidate_participant_id,
            subject_kind: summary.subject_kind,
            subject_id: summary.subject_id,
            source_participant_count: summary.source_participant_count,
            window_ms: summary.window_ms,
            evaluated_at_ms: summary.evaluated_at_ms,
            candidate_total_credits: summary.candidate_total_credits,
            workspace_total_credits: summary.workspace_total_credits,
            workspace_arithmetic_mean: summary.workspace_arithmetic_mean,
            mean_remainder_numerator: summary.mean_remainder_numerator,
            mean_subject_count: summary.mean_subject_count,
            qualified: summary.qualified,
            snapshot_digest: encode_snapshot_digest(&summary.snapshot_digest),
        }
    }
}

fn encode_snapshot_digest(digest: &[u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(64);
    for byte in digest {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}

impl SnapshotWire {
    fn from_summary(
        summary: &heterarchy_orchestration_runtime::work_credit::WorkCreditScoreSnapshotSummary,
    ) -> Self {
        Self {
            workspace_id: summary.workspace_id,
            team_id: summary.team_id,
            window_ms: summary.window_ms,
            per_subject: summary
                .per_subject
                .iter()
                .map(|subject| SubjectWire {
                    kind: heterarchy_orchestration_runtime::work_credit::subject_kind_label(
                        subject.subject.kind,
                    ),
                    subject_id: subject.subject.subject_id,
                    profile_id: subject.subject.profile_id,
                    source_participant_count: subject.source_participant_count,
                    total_credits: subject.total_credits,
                    entry_count: subject.entry_count,
                })
                .collect(),
            total_credits: summary.total_credits,
            subject_arithmetic_mean: summary.subject_arithmetic_mean,
            subject_mean_remainder_numerator: summary.subject_mean_remainder_numerator,
            mean_subject_count: summary.mean_subject_count,
            subject_total_credits: summary.subject_total_credits,
            unattributed_count: summary.unattributed_count,
            fulfillment_completed: summary.fulfillment_completed,
            fulfillment_evaluated: summary.fulfillment_evaluated,
        }
    }
}

fn score_wire_from_summary(
    summary: &heterarchy_orchestration_runtime::work_credit::WorkCreditScoreSnapshotSummary,
) -> WorkCreditScoreWire {
    WorkCreditScoreWire {
        workspace_id: summary.workspace_id,
        team_id: summary.team_id,
        window_ms: summary.window_ms,
        total_credits: summary.total_credits,
        workspace_arithmetic_mean: summary.subject_arithmetic_mean,
        mean_remainder_numerator: summary.subject_mean_remainder_numerator,
        mean_subject_count: summary.mean_subject_count,
        subject_total_credits: summary.subject_total_credits,
        unattributed_count: summary.unattributed_count,
        fulfillment: WorkCreditFulfillmentWire {
            completed: summary.fulfillment_completed,
            evaluated: summary.fulfillment_evaluated,
            per_subject: summary
                .fulfillment_per_subject
                .iter()
                .map(|fulfillment| WorkCreditSubjectFulfillmentWire {
                    subject: subject_identity_wire(&fulfillment.subject),
                    source_participant_count: fulfillment.source_participant_count,
                    completed: fulfillment.completed,
                    evaluated: fulfillment.evaluated,
                })
                .collect(),
        },
        provenance: WorkCreditProvenanceWire {
            submitted: summary.provenance_submitted,
            matched: summary.provenance_matched,
        },
        per_subject: summary
            .per_subject
            .iter()
            .map(|score| WorkCreditSubjectScoreWire {
                subject: subject_identity_wire(&score.subject),
                source_participant_count: score.source_participant_count,
                total_credits: score.total_credits,
                entry_count: score.entry_count,
            })
            .collect(),
    }
}

fn subject_identity_wire(
    subject: &heterarchy_orchestration_runtime::work_credit::WorkCreditSubjectIdentitySummary,
) -> WorkCreditSubjectIdentityWire {
    WorkCreditSubjectIdentityWire {
        kind: match subject.kind {
            heterarchy_orchestration_operations::WorkCreditSubjectKind::WorkspaceParticipation => {
                WorkCreditSubjectKindWire::WorkspaceParticipation
            }
            heterarchy_orchestration_operations::WorkCreditSubjectKind::LegacyParticipant => {
                WorkCreditSubjectKindWire::LegacyParticipant
            }
        },
        subject_id: subject.subject_id,
        participation_id: subject.participation_id,
        profile_id: subject.profile_id,
        legacy_participant_id: subject.legacy_participant_id,
        participation_revision: subject.participation_revision,
    }
}

#[cfg(test)]
mod election_snapshot_tests {
    use super::ElectionSnapshotWire;
    use heterarchy_orchestration_runtime::work_credit::ElectionSnapshotSummary;
    use uuid::Uuid;

    #[test]
    fn election_wire_preserves_frozen_qualification_arithmetic_and_digest()
    -> Result<(), serde_json::Error> {
        for qualified in [false, true] {
            let summary = ElectionSnapshotSummary {
                election_id: Uuid::from_u128(1),
                workspace_id: Uuid::from_u128(2),
                team_id: None,
                candidate_participant_id: Uuid::from_u128(3),
                subject_kind: "workspace_participation",
                subject_id: Uuid::from_u128(4),
                source_participant_count: 2,
                window_ms: 60_000,
                evaluated_at_ms: 42,
                candidate_total_credits: 8,
                workspace_total_credits: 20,
                workspace_arithmetic_mean: 6,
                mean_remainder_numerator: 2,
                mean_subject_count: 3,
                qualified,
                snapshot_digest: [0xab; 32],
            };
            let wire = serde_json::to_value(ElectionSnapshotWire::from_summary(&summary))?;
            assert_eq!(wire["qualified"], qualified);
            assert_eq!(wire["workspaceArithmeticMean"], 6);
            assert_eq!(wire["meanRemainderNumerator"], 2);
            assert_eq!(wire["meanSubjectCount"], 3);
            assert_eq!(wire["snapshotDigest"], "ab".repeat(32));
            assert!(wire["teamId"].is_null());
            assert_eq!(wire["electionId"], Uuid::from_u128(1).to_string());
        }
        Ok(())
    }
}
