//! Bounded request/readback contracts for temporary-dispatch admission only.

use serde::{Deserialize, Deserializer, Serialize};
use uuid::Uuid;

use super::leadership::WorkspaceQueryScope;

/// Maximum UTF-8 byte length of request-supplied scope intent.
pub const MAX_TEMPORARY_DISPATCH_SCOPE_BYTES: usize = 2_048;
/// Maximum JSON bytes for one admission or readback request body.
pub const MAX_TEMPORARY_DISPATCH_BODY_BYTES: usize = 32 * 1024;
const MAX_WORKSPACE_LOCATOR_BYTES: usize = 4_096;
const TEMPORARY_DISPATCH_LOCK_MS: i64 = 60 * 60 * 1_000;

/// Exact original request intent. Agent, role, consent, source evidence and
/// admission time are deliberately absent; the server derives the actor Agent.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TemporaryDispatchAdmissionRequest {
    pub scope: WorkspaceQueryScope,
    pub requesting_team_id: Uuid,
    pub request_id: Uuid,
    pub activity_id: Uuid,
    pub session_id: Uuid,
    pub expected_leadership_revision: u64,
    pub target_participant_id: Uuid,
    pub requested_scope: String,
    pub requested_end_at_ms: i64,
}

/// The exact original request is retained in an uncertain-mutation target.
pub type TemporaryDispatchReadbackIntent = TemporaryDispatchAdmissionRequest;

/// The authenticated actor binding used to recover an original receipt after
/// Session rebind. Agent identity is derived from this Activity and Session.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TemporaryDispatchCurrentActorWire {
    pub activity_id: Uuid,
    pub session_id: Uuid,
}

/// Exact receipt query with original intent and separately authorized current
/// source/actor selectors. `intent.scope` is retained request data; only
/// `current_scope` is source-proven during readback.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TemporaryDispatchReadbackRequest {
    pub intent: TemporaryDispatchReadbackIntent,
    /// Current source scope for the same Workspace; unlike the retained
    /// original intent scope, this is revalidated against Forge at read time.
    pub current_scope: WorkspaceQueryScope,
    pub current_actor: TemporaryDispatchCurrentActorWire,
}

/// One durable actor attribution snapshot retained by the Operations owner.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TemporaryDispatchActorReceiptWire {
    pub activity_id: Uuid,
    pub agent_id: Uuid,
    pub session_id: Uuid,
    pub participation_id: Uuid,
    pub profile_id: Uuid,
    pub participant_id: Uuid,
    pub display_name: String,
}

/// Immutable admission receipt stored by Operations.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TemporaryDispatchReceiptWire {
    pub request_id: Uuid,
    pub workspace_id: Uuid,
    pub requesting_team_id: Uuid,
    pub leadership_revision: u64,
    pub actor: TemporaryDispatchActorReceiptWire,
    pub target_home_team_id: Uuid,
    pub target_participant_id: Uuid,
    pub requested_scope: String,
    pub requested_end_at_ms: i64,
    pub admitted_at_ms: i64,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub lock_until_ms: Option<i64>,
    pub replayed: bool,
}

/// Definitive no-write quota rejection or a durable immutable admission.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(tag = "outcome", rename_all = "snake_case", deny_unknown_fields)]
pub enum TemporaryDispatchAdmissionOutcomeWire {
    /// A durable receipt entered Team quota history.
    Admitted {
        receipt: Box<TemporaryDispatchReceiptWire>,
    },
    /// A typed result: this attempt made no quota or receipt write.
    QuotaRejected {
        request_id: Uuid,
        workspace_id: Uuid,
        requesting_team_id: Uuid,
        retry_at_ms: i64,
    },
}

/// Admission response echoes the exact submitted intent for client correlation.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TemporaryDispatchAdmissionResponse {
    pub request: TemporaryDispatchAdmissionRequest,
    pub outcome: TemporaryDispatchAdmissionOutcomeWire,
}

/// Receipt readback. The echoed original scope is retained caller intent, not
/// source proof for the receipt; `request.current_scope` is the scope rechecked
/// at read time. `receipt: null` is only a snapshot absence and does not prove
/// an in-flight command cannot still commit.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TemporaryDispatchReadbackResponse {
    pub request: TemporaryDispatchReadbackRequest,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub receipt: Option<TemporaryDispatchReceiptWire>,
}

pub(crate) fn valid_admission_request(request: &TemporaryDispatchAdmissionRequest) -> bool {
    valid_scope(&request.scope)
        && !request.requesting_team_id.is_nil()
        && !request.request_id.is_nil()
        && !request.activity_id.is_nil()
        && !request.session_id.is_nil()
        && request.expected_leadership_revision > 0
        && i64::try_from(request.expected_leadership_revision).is_ok()
        && !request.target_participant_id.is_nil()
        && valid_requested_scope(&request.requested_scope)
        && request.requested_end_at_ms >= 0
}

pub(crate) fn valid_readback_request(request: &TemporaryDispatchReadbackRequest) -> bool {
    valid_admission_request(&request.intent)
        && valid_scope(&request.current_scope)
        && request.current_scope.workspace_id == request.intent.scope.workspace_id
        && !request.current_actor.activity_id.is_nil()
        && !request.current_actor.session_id.is_nil()
}

pub(crate) fn valid_receipt(receipt: &TemporaryDispatchReceiptWire) -> bool {
    !receipt.request_id.is_nil()
        && !receipt.workspace_id.is_nil()
        && !receipt.requesting_team_id.is_nil()
        && receipt.leadership_revision > 0
        && i64::try_from(receipt.leadership_revision).is_ok()
        && !receipt.actor.activity_id.is_nil()
        && !receipt.actor.agent_id.is_nil()
        && !receipt.actor.session_id.is_nil()
        && !receipt.actor.participation_id.is_nil()
        && !receipt.actor.profile_id.is_nil()
        && !receipt.actor.participant_id.is_nil()
        && !receipt.actor.display_name.is_empty()
        && receipt.actor.display_name.len() <= 128
        && !receipt.target_home_team_id.is_nil()
        && receipt.target_home_team_id != receipt.requesting_team_id
        && !receipt.target_participant_id.is_nil()
        && valid_requested_scope(&receipt.requested_scope)
        && receipt.admitted_at_ms >= 0
        && receipt.requested_end_at_ms > receipt.admitted_at_ms
        && receipt.lock_until_ms.is_none_or(|until| {
            receipt
                .admitted_at_ms
                .checked_add(TEMPORARY_DISPATCH_LOCK_MS)
                .is_some_and(|expected| until == expected)
        })
}

pub(crate) fn receipt_matches_intent(
    receipt: &TemporaryDispatchReceiptWire,
    intent: &TemporaryDispatchReadbackIntent,
) -> bool {
    valid_receipt(receipt)
        && receipt.request_id == intent.request_id
        && receipt.workspace_id == intent.scope.workspace_id
        && receipt.requesting_team_id == intent.requesting_team_id
        && receipt.leadership_revision == intent.expected_leadership_revision
        && receipt.actor.activity_id == intent.activity_id
        && receipt.actor.session_id == intent.session_id
        && receipt.target_participant_id == intent.target_participant_id
        && receipt.requested_scope == intent.requested_scope
        && receipt.requested_end_at_ms == intent.requested_end_at_ms
}

fn valid_scope(scope: &WorkspaceQueryScope) -> bool {
    !scope.workspace_id.is_nil()
        && !scope.location_id.is_nil()
        && scope.location_revision > 0
        && scope.workspace_revision > 0
        && !scope.locator.is_empty()
        && scope.locator.len() <= MAX_WORKSPACE_LOCATOR_BYTES
        && !scope.locator.chars().any(char::is_control)
}

fn valid_requested_scope(value: &str) -> bool {
    !value.trim().is_empty() && value.len() <= MAX_TEMPORARY_DISPATCH_SCOPE_BYTES
}

fn deserialize_required_option<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request() -> TemporaryDispatchAdmissionRequest {
        TemporaryDispatchAdmissionRequest {
            scope: WorkspaceQueryScope {
                workspace_id: Uuid::from_u128(1),
                location_id: Uuid::from_u128(2),
                location_revision: 3,
                workspace_revision: 4,
                locator: "workspace".to_owned(),
            },
            requesting_team_id: Uuid::from_u128(5),
            request_id: Uuid::from_u128(6),
            activity_id: Uuid::from_u128(7),
            session_id: Uuid::from_u128(8),
            expected_leadership_revision: 9,
            target_participant_id: Uuid::from_u128(10),
            requested_scope: "review one bounded work item".to_owned(),
            requested_end_at_ms: 20,
        }
    }

    #[test]
    fn admission_input_rejects_caller_supplied_authority_and_outcome_fields() {
        let encoded = serde_json::to_value(request());
        assert!(encoded.is_ok());
        let Some(mut value) = encoded.ok() else {
            return;
        };
        let object = value.as_object_mut();
        assert!(object.is_some());
        let Some(object) = object else {
            return;
        };
        object.insert("agentId".to_owned(), serde_json::json!(Uuid::from_u128(11)));
        object.insert("admittedAtMs".to_owned(), serde_json::json!(12));
        object.insert("consent".to_owned(), serde_json::json!(true));
        object.insert("role".to_owned(), serde_json::json!("leader"));
        object.insert("sourceEvidence".to_owned(), serde_json::json!("untrusted"));
        assert!(serde_json::from_value::<TemporaryDispatchAdmissionRequest>(value).is_err());
    }

    #[test]
    fn readback_keeps_original_intent_separate_from_current_actor() {
        let original = request();
        let readback = TemporaryDispatchReadbackRequest {
            intent: original.clone(),
            current_scope: WorkspaceQueryScope {
                workspace_id: original.scope.workspace_id,
                location_id: Uuid::from_u128(20),
                location_revision: 15,
                workspace_revision: 16,
                locator: "current-workspace-binding".to_owned(),
            },
            current_actor: TemporaryDispatchCurrentActorWire {
                activity_id: Uuid::from_u128(13),
                session_id: Uuid::from_u128(14),
            },
        };
        let encoded_result = serde_json::to_vec(&readback);
        assert!(encoded_result.is_ok());
        let Some(encoded) = encoded_result.ok() else {
            return;
        };
        let decoded = serde_json::from_slice::<TemporaryDispatchReadbackRequest>(&encoded);
        assert!(decoded.is_ok());
        let Some(roundtrip) = decoded.ok() else {
            return;
        };
        assert_eq!(roundtrip, readback);
        assert_eq!(roundtrip.intent.session_id, original.session_id);
        assert_ne!(roundtrip.current_actor.session_id, original.session_id);
        assert!(valid_readback_request(&roundtrip));
    }

    #[test]
    fn readback_requires_explicit_nullable_receipt_field() {
        let encoded_intent = serde_json::to_value(request());
        assert!(encoded_intent.is_ok());
        let Some(intent) = encoded_intent.ok() else {
            return;
        };
        let value = serde_json::json!({
            "request": {
                "intent": intent,
                "currentActor": {
                    "activityId": Uuid::from_u128(13),
                    "sessionId": Uuid::from_u128(14)
                }
            }
        });
        assert!(serde_json::from_value::<TemporaryDispatchReadbackResponse>(value).is_err());
    }

    #[test]
    fn maximum_escaped_valid_admission_and_readback_fit_the_route_body_limit() {
        let mut intent = request();
        intent.scope.locator = "\\\"".repeat(MAX_WORKSPACE_LOCATOR_BYTES / 2);
        intent.requested_scope = "\u{1}".repeat(MAX_TEMPORARY_DISPATCH_SCOPE_BYTES);
        assert!(valid_admission_request(&intent));

        let admission = serde_json::to_vec(&intent);
        assert!(admission.is_ok());
        let Some(admission) = admission.ok() else {
            return;
        };
        assert!(admission.len() <= MAX_TEMPORARY_DISPATCH_BODY_BYTES);

        let readback = TemporaryDispatchReadbackRequest {
            intent,
            current_scope: WorkspaceQueryScope {
                workspace_id: Uuid::from_u128(1),
                location_id: Uuid::from_u128(21),
                location_revision: 22,
                workspace_revision: 23,
                locator: "\\\"".repeat(MAX_WORKSPACE_LOCATOR_BYTES / 2),
            },
            current_actor: TemporaryDispatchCurrentActorWire {
                activity_id: Uuid::from_u128(13),
                session_id: Uuid::from_u128(14),
            },
        };
        assert!(valid_readback_request(&readback));
        let encoded_readback = serde_json::to_vec(&readback);
        assert!(encoded_readback.is_ok());
        let Some(encoded_readback) = encoded_readback.ok() else {
            return;
        };
        assert!(encoded_readback.len() <= MAX_TEMPORARY_DISPATCH_BODY_BYTES);
    }
}
