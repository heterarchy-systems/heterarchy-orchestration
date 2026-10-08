//! Source-scoped Leadership advance, confirmation, recovery, and readback contracts.

use serde::{Deserialize, Deserializer, Serialize};
use uuid::Uuid;

use super::leadership::LeadershipWire;
use super::leadership::WorkspaceQueryScope;

/// Exact Workspace, leadership scope, and caller-owned logical Session.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LeadershipLifecycleTarget {
    pub scope: WorkspaceQueryScope,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub activity_id: Option<Uuid>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub team_id: Option<Uuid>,
    pub session_id: Uuid,
}

/// Closed leadership states serialized with the existing lowercase HTTP names.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LeadershipStateWire {
    Leaderless,
    Bootstrapping,
    Stabilizing,
    Active,
    Electing,
    AuthorityPending,
    FailoverRequired,
    Vacant,
    RecoveryRequired,
}

/// Closed durable confirmation-intent lifecycle states.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LeadershipIntentStateWire {
    Pending,
    RemoteRecorded,
    Committed,
    Superseded,
    Rejected,
}

/// Closed result of reconciling one leadership recovery intent.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LeadershipRecoveryResolutionWire {
    ConfirmCompleted,
    Resolved,
    Unresolved,
}

/// Exact source leadership row returned by a mutation or confirmation readback.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LeadershipReceiptWire {
    pub workspace_id: Uuid,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub activity_id: Option<Uuid>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub team_id: Option<Uuid>,
    pub state: LeadershipStateWire,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub leader_participant_id: Option<Uuid>,
    pub leadership_revision: i64,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub stabilization_until_ms: Option<i64>,
    pub updated_at_ms: i64,
}

/// Advance one exact leadership scope when its policy permits progress.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LeadershipAdvanceRequest {
    pub target: LeadershipLifecycleTarget,
}

/// Full leadership view after an advance attempt.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LeadershipAdvanceResponse {
    pub target: LeadershipLifecycleTarget,
    pub advanced: bool,
    #[serde(deserialize_with = "deserialize_required_leadership")]
    pub leadership: Option<LeadershipWire>,
}

/// Confirm the elected leader through the existing durable Broker fence saga.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LeadershipConfirmRequest {
    pub target: LeadershipLifecycleTarget,
    pub election_id: Uuid,
    pub request_id: Uuid,
    pub expected_leadership_revision: i64,
}

/// Complete durable confirmation outcome and exact source leadership row.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LeadershipConfirmResponse {
    pub target: LeadershipLifecycleTarget,
    pub request_id: Uuid,
    pub election_id: Uuid,
    pub intent_state: LeadershipIntentStateWire,
    pub record: LeadershipReceiptWire,
}

/// Reconcile one exact durable leadership recovery request.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LeadershipReconcileRequest {
    pub target: LeadershipLifecycleTarget,
    pub request_id: Uuid,
    pub expected_leadership_revision: i64,
}

/// Full recovery resolution and exact source leadership row.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LeadershipReconcileResponse {
    pub target: LeadershipLifecycleTarget,
    pub request_id: Uuid,
    pub resolution: LeadershipRecoveryResolutionWire,
    pub record: LeadershipReceiptWire,
}

/// Read one exact confirmation intent and its current source readback.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LeadershipConfirmationQueryRequest {
    pub target: LeadershipLifecycleTarget,
    pub request_id: Uuid,
}

/// One exact confirmation intent snapshot and source readback.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LeadershipConfirmationSnapshotWire {
    pub election_id: Uuid,
    pub elected_participant_id: Uuid,
    pub expected_local_revision: i64,
    pub requested_fence_revision: i64,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub remote_recorded_revision: Option<i64>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub confirmed_at_ms: Option<i64>,
    pub intent_state: LeadershipIntentStateWire,
    pub record: LeadershipReceiptWire,
}

/// Exact query target and explicit nullable confirmation intent.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LeadershipConfirmationQueryResponse {
    pub target: LeadershipLifecycleTarget,
    pub request_id: Uuid,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub confirmation: Option<LeadershipConfirmationSnapshotWire>,
}

fn deserialize_required_option<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer)
}

fn deserialize_required_leadership<'de, D>(
    deserializer: D,
) -> Result<Option<LeadershipWire>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = serde_json::Value::deserialize(deserializer)?;
    if !value.is_null() && !has_required_leadership_options(&value) {
        return Err(serde::de::Error::custom(
            "Leadership advance omitted required nullable metadata",
        ));
    }
    serde_json::from_value(value).map_err(serde::de::Error::custom)
}

fn has_required_leadership_options(value: &serde_json::Value) -> bool {
    let Some(leadership) = value.as_object() else {
        return false;
    };
    [
        "activityId",
        "teamId",
        "leaderParticipationId",
        "leaderProfileId",
        "leaderParticipantId",
        "leaderDisplayName",
        "stabilizationUntilMs",
        "currentElection",
    ]
    .iter()
    .all(|field| leadership.contains_key(*field))
}

#[cfg(test)]
mod tests {
    use super::LeadershipLifecycleTarget;
    use super::{
        LeadershipAdvanceResponse, LeadershipConfirmationQueryRequest,
        LeadershipConfirmationQueryResponse, LeadershipIntentStateWire, LeadershipReceiptWire,
        LeadershipRecoveryResolutionWire, LeadershipStateWire,
    };
    use crate::wire::leadership::WorkspaceQueryScope;
    use uuid::Uuid;

    fn target() -> LeadershipLifecycleTarget {
        LeadershipLifecycleTarget {
            scope: WorkspaceQueryScope {
                workspace_id: Uuid::from_u128(1),
                location_id: Uuid::from_u128(2),
                location_revision: 3,
                workspace_revision: 4,
                locator: "workspace".to_owned(),
            },
            activity_id: Some(Uuid::from_u128(5)),
            team_id: None,
            session_id: Uuid::from_u128(6),
        }
    }

    fn receipt() -> LeadershipReceiptWire {
        LeadershipReceiptWire {
            workspace_id: Uuid::from_u128(1),
            activity_id: Some(Uuid::from_u128(5)),
            team_id: None,
            state: LeadershipStateWire::Active,
            leader_participant_id: Some(Uuid::from_u128(7)),
            leadership_revision: 2,
            stabilization_until_ms: None,
            updated_at_ms: 10,
        }
    }

    #[test]
    fn lifecycle_wire_enums_keep_closed_lowercase_http_labels()
    -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(
            serde_json::to_value(LeadershipStateWire::Leaderless)?,
            "leaderless"
        );
        assert_eq!(
            serde_json::to_value(LeadershipIntentStateWire::RemoteRecorded)?,
            "remote_recorded"
        );
        assert_eq!(
            serde_json::to_value(LeadershipRecoveryResolutionWire::ConfirmCompleted)?,
            "confirm_completed"
        );
        assert!(
            serde_json::from_value::<LeadershipStateWire>(serde_json::json!("LEADERLESS")).is_err()
        );
        Ok(())
    }

    #[test]
    fn lifecycle_required_nullable_fields_reject_omission_but_accept_nulls()
    -> Result<(), Box<dyn std::error::Error>> {
        let query = LeadershipConfirmationQueryRequest {
            target: target(),
            request_id: Uuid::from_u128(8),
        };
        let encoded = serde_json::to_value(query)?;
        assert!(
            serde_json::from_value::<LeadershipConfirmationQueryRequest>(encoded.clone()).is_ok()
        );
        for field in ["activityId", "teamId"] {
            let mut missing = encoded.clone();
            missing["target"]
                .as_object_mut()
                .ok_or("target object")?
                .remove(field);
            assert!(serde_json::from_value::<LeadershipConfirmationQueryRequest>(missing).is_err());
        }

        let mut encoded_receipt = serde_json::to_value(receipt())?;
        assert!(serde_json::from_value::<LeadershipReceiptWire>(encoded_receipt.clone()).is_ok());
        encoded_receipt
            .as_object_mut()
            .ok_or("receipt object")?
            .remove("teamId");
        assert!(serde_json::from_value::<LeadershipReceiptWire>(encoded_receipt).is_err());

        let advance = LeadershipAdvanceResponse {
            target: target(),
            advanced: false,
            leadership: None,
        };
        let mut encoded_advance = serde_json::to_value(advance)?;
        assert!(
            serde_json::from_value::<LeadershipAdvanceResponse>(encoded_advance.clone()).is_ok()
        );
        encoded_advance
            .as_object_mut()
            .ok_or("advance object")?
            .remove("leadership");
        assert!(serde_json::from_value::<LeadershipAdvanceResponse>(encoded_advance).is_err());
        Ok(())
    }

    #[test]
    fn advance_leadership_requires_nullable_view_metadata() -> Result<(), Box<dyn std::error::Error>>
    {
        let response = serde_json::json!({
            "target": serde_json::to_value(target())?,
            "advanced": false,
            "leadership": {
                "workspaceId": Uuid::from_u128(1),
                "activityId": Uuid::from_u128(5),
                "teamId": null,
                "state": "active",
                "leadershipRevision": 2,
                "leaderParticipationId": null,
                "leaderProfileId": null,
                "leaderParticipantId": Uuid::from_u128(7),
                "leaderDisplayName": null,
                "stabilizationUntilMs": null,
                "updatedAtMs": 10,
                "currentElection": null
            }
        });
        assert!(serde_json::from_value::<LeadershipAdvanceResponse>(response.clone()).is_ok());
        for field in [
            "activityId",
            "teamId",
            "leaderParticipationId",
            "leaderProfileId",
            "leaderParticipantId",
            "leaderDisplayName",
            "stabilizationUntilMs",
            "currentElection",
        ] {
            let mut missing = response.clone();
            missing["leadership"]
                .as_object_mut()
                .ok_or("leadership object")?
                .remove(field);
            assert!(serde_json::from_value::<LeadershipAdvanceResponse>(missing).is_err());
        }
        Ok(())
    }

    #[test]
    fn confirmation_snapshot_requires_nullable_readback_fields()
    -> Result<(), Box<dyn std::error::Error>> {
        let confirmation = serde_json::json!({
            "electionId": Uuid::from_u128(8),
            "electedParticipantId": Uuid::from_u128(9),
            "expectedLocalRevision": 2,
            "requestedFenceRevision": 2,
            "remoteRecordedRevision": null,
            "confirmedAtMs": null,
            "intentState": "pending",
            "record": serde_json::to_value(receipt())?
        });
        let response = serde_json::json!({
            "target": serde_json::to_value(target())?,
            "requestId": Uuid::from_u128(10),
            "confirmation": confirmation
        });
        assert!(
            serde_json::from_value::<LeadershipConfirmationQueryResponse>(response.clone()).is_ok()
        );
        for field in ["remoteRecordedRevision", "confirmedAtMs"] {
            let mut missing = response.clone();
            missing["confirmation"]
                .as_object_mut()
                .ok_or("confirmation object")?
                .remove(field);
            assert!(
                serde_json::from_value::<LeadershipConfirmationQueryResponse>(missing).is_err()
            );
        }
        let mut missing_confirmation = response;
        missing_confirmation
            .as_object_mut()
            .ok_or("confirmation response object")?
            .remove("confirmation");
        assert!(
            serde_json::from_value::<LeadershipConfirmationQueryResponse>(missing_confirmation)
                .is_err()
        );
        Ok(())
    }
}
