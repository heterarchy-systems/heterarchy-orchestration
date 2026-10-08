//! Bounded client for the source-fenced `WorkCredit` score query.

use std::collections::{HashMap, HashSet};

use heterarchy_orchestration_operations::{
    MAX_WORKSPACE_PARTICIPATIONS_PER_READ, WORK_CREDIT_WINDOW_MS,
};
use hyper::Method;
use hyper::header::HeaderValue;
use uuid::Uuid;

use super::{ClientError, OrchestrationHttpClient};
use crate::wire::{
    WorkCreditScoreQueryRequest, WorkCreditScoreQueryResponse, WorkCreditScoreWire,
    WorkCreditSubjectIdentityWire, WorkCreditSubjectKindWire,
};

const MAX_WORKSPACE_LOCATOR_BYTES: usize = 4096;
const MAX_WORK_CREDIT_SOURCE_PARTICIPANTS: usize = MAX_WORKSPACE_PARTICIPATIONS_PER_READ;

impl OrchestrationHttpClient {
    /// Query a complete source-fenced `Workspace` `WorkCredit` score snapshot.
    ///
    /// The response uses the client's shared 256 KiB body limit and is checked
    /// against the exact scope and the Operations subject-binding contract.
    ///
    /// # Errors
    /// Returns invalid input, a bounded transport error, a typed service
    /// rejection, or a protocol error for an incomplete or inconsistent view.
    pub async fn work_credit_scores(
        &self,
        authorization: &HeaderValue,
        request: &WorkCreditScoreQueryRequest,
    ) -> Result<WorkCreditScoreQueryResponse, ClientError> {
        validate_work_credit_request(request)?;
        let response: WorkCreditScoreQueryResponse = self
            .call(
                authorization,
                Method::POST,
                format!(
                    "/internal/v1/workspaces/{}/work-credits/scores/query",
                    request.scope.workspace_id
                ),
                Some(request),
                None,
            )
            .await?;
        if !valid_work_credit_response(&response, request) {
            return Err(ClientError::Protocol);
        }
        Ok(response)
    }
}

fn validate_work_credit_request(request: &WorkCreditScoreQueryRequest) -> Result<(), ClientError> {
    let scope = &request.scope;
    if scope.workspace_id.is_nil()
        || scope.location_id.is_nil()
        || scope.location_revision <= 0
        || scope.workspace_revision <= 0
        || scope.locator.is_empty()
        || scope.locator.len() > MAX_WORKSPACE_LOCATOR_BYTES
        || scope.locator.chars().any(char::is_control)
        || request.session_id.is_some_and(|id| id.is_nil())
    {
        return Err(ClientError::InvalidRequest);
    }
    Ok(())
}

fn valid_work_credit_response(
    response: &WorkCreditScoreQueryResponse,
    request: &WorkCreditScoreQueryRequest,
) -> bool {
    response.scope == request.scope
        && valid_work_credit_snapshot(&response.snapshot, request.scope.workspace_id)
}

fn valid_work_credit_snapshot(snapshot: &WorkCreditScoreWire, workspace_id: Uuid) -> bool {
    if snapshot.workspace_id != workspace_id
        || snapshot.team_id.is_some()
        || snapshot.window_ms != WORK_CREDIT_WINDOW_MS
        || snapshot.total_credits < 0
        || snapshot.workspace_arithmetic_mean < 0
        || snapshot.mean_remainder_numerator < 0
        || snapshot.mean_subject_count < 0
        || snapshot.subject_total_credits != snapshot.total_credits
        || snapshot.unattributed_count < 0
        || snapshot.fulfillment.completed < 0
        || snapshot.fulfillment.evaluated < snapshot.fulfillment.completed
        || snapshot.provenance.submitted < 0
        || snapshot.provenance.matched < 0
        || snapshot.provenance.matched > snapshot.provenance.submitted
        || snapshot.per_subject.len() > MAX_WORK_CREDIT_SOURCE_PARTICIPANTS
        || snapshot.fulfillment.per_subject.len() > MAX_WORK_CREDIT_SOURCE_PARTICIPANTS
    {
        return false;
    }

    let Ok(score_count) = i64::try_from(snapshot.per_subject.len()) else {
        return false;
    };
    if snapshot.mean_subject_count != score_count {
        return false;
    }
    if score_count == 0 {
        if snapshot.total_credits != 0
            || snapshot.workspace_arithmetic_mean != 0
            || snapshot.mean_remainder_numerator != 0
        {
            return false;
        }
    } else if snapshot.workspace_arithmetic_mean != snapshot.total_credits / score_count
        || snapshot.mean_remainder_numerator != snapshot.total_credits % score_count
        || snapshot.mean_remainder_numerator >= score_count
    {
        return false;
    }

    let Ok(max_source_count) = u64::try_from(MAX_WORK_CREDIT_SOURCE_PARTICIPANTS) else {
        return false;
    };
    let mut score_source_count = 0_u64;
    let mut score_subjects = HashMap::with_capacity(snapshot.per_subject.len());
    for score in &snapshot.per_subject {
        let key = (score.subject.kind, score.subject.subject_id);
        let Some(next_source_count) =
            score_source_count.checked_add(score.source_participant_count)
        else {
            return false;
        };
        if !valid_subject_identity(&score.subject, score.source_participant_count)
            || score.total_credits < 0
            || score.entry_count < 0
            || score.entry_count == 0
            || score_subjects.insert(key, score.subject).is_some()
        {
            return false;
        }
        score_source_count = next_source_count;
    }
    if score_source_count > max_source_count {
        return false;
    }

    let mut fulfillment_subjects = HashSet::with_capacity(snapshot.fulfillment.per_subject.len());
    let mut fulfillment_source_count = 0_u64;
    for fulfillment in &snapshot.fulfillment.per_subject {
        let key = (fulfillment.subject.kind, fulfillment.subject.subject_id);
        let Some(next_source_count) =
            fulfillment_source_count.checked_add(fulfillment.source_participant_count)
        else {
            return false;
        };
        if !valid_subject_identity(&fulfillment.subject, fulfillment.source_participant_count)
            || fulfillment.completed < 0
            || fulfillment.evaluated < fulfillment.completed
            || !fulfillment_subjects.insert(key)
            || score_subjects
                .get(&key)
                .is_some_and(|score_subject| *score_subject != fulfillment.subject)
        {
            return false;
        }
        fulfillment_source_count = next_source_count;
    }
    if fulfillment_source_count > max_source_count {
        return false;
    }
    true
}

fn valid_subject_identity(
    subject: &WorkCreditSubjectIdentityWire,
    source_participant_count: u64,
) -> bool {
    if subject.subject_id.is_nil() || source_participant_count == 0 {
        return false;
    }
    match subject.kind {
        WorkCreditSubjectKindWire::WorkspaceParticipation => {
            subject.participation_id == Some(subject.subject_id)
                && subject.profile_id.is_some_and(|id| !id.is_nil())
                && subject.legacy_participant_id.is_none()
                && subject
                    .participation_revision
                    .is_some_and(|revision| revision > 0)
        }
        WorkCreditSubjectKindWire::LegacyParticipant => {
            source_participant_count == 1
                && subject.participation_id.is_none()
                && subject.profile_id.is_none()
                && subject.legacy_participant_id == Some(subject.subject_id)
                && subject.participation_revision.is_none()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::{
        WorkCreditFulfillmentWire, WorkCreditProvenanceWire, WorkCreditSubjectFulfillmentWire,
        WorkCreditSubjectScoreWire, WorkspaceQueryScope,
    };

    fn valid_request() -> WorkCreditScoreQueryRequest {
        WorkCreditScoreQueryRequest {
            scope: WorkspaceQueryScope {
                workspace_id: Uuid::from_u128(1),
                location_id: Uuid::from_u128(2),
                location_revision: 3,
                workspace_revision: 4,
                locator: "workspace".to_owned(),
            },
            session_id: Some(Uuid::from_u128(5)),
        }
    }

    fn participation_subject() -> WorkCreditSubjectIdentityWire {
        WorkCreditSubjectIdentityWire {
            kind: WorkCreditSubjectKindWire::WorkspaceParticipation,
            subject_id: Uuid::from_u128(6),
            participation_id: Some(Uuid::from_u128(6)),
            profile_id: Some(Uuid::from_u128(7)),
            legacy_participant_id: None,
            participation_revision: Some(8),
        }
    }

    fn full_response(request: &WorkCreditScoreQueryRequest) -> WorkCreditScoreQueryResponse {
        let subject = participation_subject();
        WorkCreditScoreQueryResponse {
            scope: request.scope.clone(),
            snapshot: WorkCreditScoreWire {
                workspace_id: request.scope.workspace_id,
                team_id: None,
                window_ms: 2_592_000_000,
                total_credits: 5,
                workspace_arithmetic_mean: 5,
                mean_remainder_numerator: 0,
                mean_subject_count: 1,
                subject_total_credits: 5,
                unattributed_count: 0,
                fulfillment: WorkCreditFulfillmentWire {
                    completed: 1,
                    evaluated: 2,
                    per_subject: vec![WorkCreditSubjectFulfillmentWire {
                        subject,
                        source_participant_count: 2,
                        completed: 1,
                        evaluated: 2,
                    }],
                },
                provenance: WorkCreditProvenanceWire {
                    submitted: 2,
                    matched: 1,
                },
                per_subject: vec![WorkCreditSubjectScoreWire {
                    subject,
                    source_participant_count: 2,
                    total_credits: 5,
                    entry_count: 2,
                }],
            },
        }
    }

    #[test]
    fn full_score_round_trip_preserves_owner_fields_and_rejects_malformed_binding()
    -> Result<(), Box<dyn std::error::Error>> {
        let request = valid_request();
        let response = full_response(&request);
        let encoded = serde_json::to_value(&response)?;
        assert_eq!(
            encoded["snapshot"]["perSubject"][0]["subject"]["participationId"],
            serde_json::json!(Uuid::from_u128(6))
        );
        assert_eq!(
            encoded["snapshot"]["perSubject"][0]["subject"]["legacyParticipantId"],
            serde_json::Value::Null
        );
        assert_eq!(
            encoded["snapshot"]["fulfillment"]["perSubject"][0]["completed"],
            1
        );
        assert_eq!(encoded["snapshot"]["provenance"]["matched"], 1);

        let decoded: WorkCreditScoreQueryResponse = serde_json::from_value(encoded)?;
        assert_eq!(decoded, response);
        assert!(valid_work_credit_response(&decoded, &request));

        let mut malformed = decoded;
        malformed.snapshot.per_subject[0].subject.participation_id = Some(Uuid::from_u128(99));
        assert!(!valid_work_credit_response(&malformed, &request));
        Ok(())
    }

    #[test]
    fn full_score_requires_nullable_identity_fields_to_be_present()
    -> Result<(), Box<dyn std::error::Error>> {
        let request = valid_request();
        let mut encoded = serde_json::to_value(full_response(&request))?;
        encoded["snapshot"]["perSubject"][0]["subject"]
            .as_object_mut()
            .ok_or("identity object missing")?
            .remove("legacyParticipantId");
        assert!(serde_json::from_value::<WorkCreditScoreQueryResponse>(encoded).is_err());
        Ok(())
    }
}
