//! Strict HTTP contracts for Session-bound Reviewer interest statements.
//!
//! These records do not qualify a Reviewer, appoint one, grant case access, or
//! make withdrawal affect an existing case.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::WorkspaceQueryScope;

/// Bounded Activity/Agent/Session selector owned by the authenticated caller.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ReviewerParticipationTargetWire {
    pub scope: WorkspaceQueryScope,
    pub activity_id: Uuid,
    pub agent_id: Uuid,
    pub session_id: Uuid,
}

/// The statement carried by the Profile-bound Agent Session. Neither variant
/// carries a permission or proves an independent human Agent identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ReviewerParticipationStatementWire {
    Interested,
    WithdrawInterest {
        withdraws_request_id: Uuid,
        withdraws_revision: u64,
    },
}

/// Append one exact immutable statement at the expected participation revision.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ReviewerParticipationMutationRequest {
    pub target: ReviewerParticipationTargetWire,
    pub request_id: Uuid,
    pub expected_revision: u64,
    pub statement: ReviewerParticipationStatementWire,
}

/// Exact receipt identity and the original Agent Session attribution.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ReviewerParticipationReceiptWire {
    pub workspace_id: Uuid,
    pub participation_id: Uuid,
    pub profile_id: Uuid,
    pub participant_id: Uuid,
    pub request_id: Uuid,
    pub revision: u64,
    pub expected_revision: u64,
    pub statement: ReviewerParticipationStatementWire,
    pub activity_id: Uuid,
    pub agent_id: Uuid,
    pub session_id: Uuid,
    pub display_name: String,
    pub recorded_at_ms: i64,
    pub replayed: bool,
}

/// Mutation echo and immutable stored receipt.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ReviewerParticipationMutationResponse {
    pub target: ReviewerParticipationTargetWire,
    pub receipt: ReviewerParticipationReceiptWire,
}

/// Exact-receipt selector. A current same-participation Session may read it.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ReviewerParticipationRequestReadRequest {
    pub target: ReviewerParticipationTargetWire,
    pub request_id: Uuid,
}

/// Exact readback with an explicitly present nullable receipt.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ReviewerParticipationRequestReadResponse {
    pub target: ReviewerParticipationTargetWire,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub receipt: Option<ReviewerParticipationReceiptWire>,
}

/// Bounded history query for this authenticated Profile participation.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ReviewerParticipationHistoryRequest {
    pub target: ReviewerParticipationTargetWire,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub after_revision: Option<u64>,
    pub limit: u32,
}

/// Immutable statement history in ascending revision order.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ReviewerParticipationHistoryResponse {
    pub target: ReviewerParticipationTargetWire,
    pub records: Vec<ReviewerParticipationReceiptWire>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub next_after_revision: Option<u64>,
}

fn deserialize_required_option<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, json};

    fn target_value() -> Value {
        json!({
            "scope": {
                "workspaceId": Uuid::from_u128(1),
                "locationId": Uuid::from_u128(2),
                "locationRevision": 3,
                "workspaceRevision": 4,
                "locator": "fixture"
            },
            "activityId": Uuid::from_u128(5),
            "agentId": Uuid::from_u128(6),
            "sessionId": Uuid::from_u128(7)
        })
    }

    #[test]
    fn nullable_receipt_and_cursor_fields_must_be_present_but_may_be_null()
    -> Result<(), Box<dyn std::error::Error>> {
        let target = target_value();
        let request_read = json!({
            "target": target,
            "receipt": null
        });
        let decoded: ReviewerParticipationRequestReadResponse =
            serde_json::from_value(request_read.clone())?;
        assert!(decoded.receipt.is_none());
        let mut missing_receipt = request_read;
        missing_receipt
            .as_object_mut()
            .ok_or_else(|| std::io::Error::other("expected JSON object"))?
            .remove("receipt");
        assert!(
            serde_json::from_value::<ReviewerParticipationRequestReadResponse>(missing_receipt)
                .is_err()
        );

        let history = json!({
            "target": target_value(),
            "afterRevision": null,
            "limit": 4
        });
        let decoded: ReviewerParticipationHistoryRequest = serde_json::from_value(history.clone())?;
        assert!(decoded.after_revision.is_none());
        let mut missing_cursor = history;
        missing_cursor
            .as_object_mut()
            .ok_or_else(|| std::io::Error::other("expected JSON object"))?
            .remove("afterRevision");
        assert!(
            serde_json::from_value::<ReviewerParticipationHistoryRequest>(missing_cursor).is_err()
        );

        let response = json!({
            "target": target_value(),
            "records": [],
            "nextAfterRevision": null
        });
        let decoded: ReviewerParticipationHistoryResponse =
            serde_json::from_value(response.clone())?;
        assert!(decoded.next_after_revision.is_none());
        let mut missing_next = response;
        missing_next
            .as_object_mut()
            .ok_or_else(|| std::io::Error::other("expected JSON object"))?
            .remove("nextAfterRevision");
        assert!(
            serde_json::from_value::<ReviewerParticipationHistoryResponse>(missing_next).is_err()
        );
        Ok(())
    }
}
