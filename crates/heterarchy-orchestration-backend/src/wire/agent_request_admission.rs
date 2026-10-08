//! Full scoped `AgentRequest` admission and exact recovery contracts.

use serde::{Deserialize, Deserializer, Serialize};
use uuid::Uuid;

use super::leadership::WorkspaceQueryScope;
use super::provisioning_operations::ProvisionOperationWire;

/// Requested side-effect authority for one `AgentRequest`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AdmissionEffectScopeWire {
    ReadOnly,
    SourceWrite,
}

/// Independence condition attached to one `AgentRequest`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AdmissionIndependenceWire {
    None,
    IndependentFromCurrentWriter,
}

/// Complete immutable request intent plus current source selectors.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AgentRequestAdmissionRequest {
    pub scope: WorkspaceQueryScope,
    pub activity_id: Uuid,
    pub coordinator_session_id: Uuid,
    pub coordinator_agent_id: Uuid,
    pub coordinator_revision: i64,
    pub request_id: Uuid,
    pub objective: String,
    pub capability: String,
    pub count: u8,
    pub effect_scope: AdmissionEffectScopeWire,
    pub independence: AdmissionIndependenceWire,
    pub reason: String,
}

/// Durable `AgentRequest` lifecycle returned by admission and recovery.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AdmissionRequestStateWire {
    ResolutionPending,
    ResolutionReserved,
    Fulfilled,
}

/// Native-equivalent durable request receipt.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AgentRequestAdmissionReceiptWire {
    pub request_id: Uuid,
    pub workspace_id: Uuid,
    pub activity_id: Uuid,
    pub state: AdmissionRequestStateWire,
    pub count: u8,
    pub created: bool,
    pub revision: u64,
}

/// Completed admission result with the exact caller intent echoed.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AgentRequestAdmissionResponse {
    pub request: AgentRequestAdmissionRequest,
    pub receipt: AgentRequestAdmissionReceiptWire,
}

/// Read-only recovery result. `snapshot: null` means the exact request is absent.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AgentRequestAdmissionRecoverResponse {
    pub request: AgentRequestAdmissionRequest,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub snapshot: Option<AgentRequestAdmissionSnapshotWire>,
}

/// Partial or complete same-owner readback of durable admission evidence.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AgentRequestAdmissionSnapshotWire {
    pub receipt: AgentRequestAdmissionReceiptWire,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub resolution: Option<AdmissionResolutionWire>,
    pub operations: Vec<ProvisionOperationWire>,
}

/// Complete resolver reservation attached to one durable request revision.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AdmissionResolutionWire {
    pub request_id: Uuid,
    pub workspace_id: Uuid,
    pub request_state: AdmissionRequestStateWire,
    pub request_revision: u64,
    pub slots: Vec<AdmissionResolutionSlotWire>,
}

/// Resolver choice for one requested slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AdmissionResolutionKindWire {
    ReuseCurrentScope,
    ReuseReadyRuntime,
    WakeExistingRuntime,
    WakeNewRuntime,
    ProvisionNew,
}

/// Exact selected target evidence for one resolver slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AdmissionResolutionSlotWire {
    pub slot_ordinal: u8,
    pub kind: AdmissionResolutionKindWire,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub claim_id: Option<Uuid>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub profile_id: Option<Uuid>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub runtime_session_id: Option<Uuid>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub runtime_session_revision: Option<i64>,
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

    fn scope() -> WorkspaceQueryScope {
        WorkspaceQueryScope {
            workspace_id: Uuid::from_u128(1),
            location_id: Uuid::from_u128(2),
            location_revision: 3,
            workspace_revision: 4,
            locator: "workspace".to_owned(),
        }
    }

    fn request() -> AgentRequestAdmissionRequest {
        AgentRequestAdmissionRequest {
            scope: scope(),
            activity_id: Uuid::from_u128(5),
            coordinator_session_id: Uuid::from_u128(6),
            coordinator_agent_id: Uuid::from_u128(7),
            coordinator_revision: 8,
            request_id: Uuid::from_u128(9),
            objective: "verify an independent capability".to_owned(),
            capability: "verification".to_owned(),
            count: 1,
            effect_scope: AdmissionEffectScopeWire::ReadOnly,
            independence: AdmissionIndependenceWire::IndependentFromCurrentWriter,
            reason: "additional evidence is useful".to_owned(),
        }
    }

    #[test]
    fn admission_wire_uses_camel_case_and_rejects_unknown_fields()
    -> Result<(), Box<dyn std::error::Error>> {
        let encoded = serde_json::to_value(request())?;
        assert!(encoded.get("coordinatorSessionId").is_some());
        assert!(encoded.get("coordinator_session_id").is_none());
        assert_eq!(encoded["effectScope"], "read_only");
        assert_eq!(encoded["independence"], "independent_from_current_writer");

        let mut hostile = encoded;
        hostile
            .as_object_mut()
            .ok_or("request object")?
            .insert("unexpected".to_owned(), serde_json::Value::Null);
        assert!(serde_json::from_value::<AgentRequestAdmissionRequest>(hostile).is_err());
        Ok(())
    }

    #[test]
    fn recovery_nullable_evidence_must_be_present_even_when_null()
    -> Result<(), Box<dyn std::error::Error>> {
        let request = request();
        let recovery = AgentRequestAdmissionRecoverResponse {
            request,
            snapshot: Some(AgentRequestAdmissionSnapshotWire {
                receipt: AgentRequestAdmissionReceiptWire {
                    request_id: Uuid::from_u128(9),
                    workspace_id: Uuid::from_u128(1),
                    activity_id: Uuid::from_u128(5),
                    state: AdmissionRequestStateWire::ResolutionReserved,
                    count: 1,
                    created: false,
                    revision: 2,
                },
                resolution: Some(AdmissionResolutionWire {
                    request_id: Uuid::from_u128(9),
                    workspace_id: Uuid::from_u128(1),
                    request_state: AdmissionRequestStateWire::ResolutionReserved,
                    request_revision: 2,
                    slots: vec![AdmissionResolutionSlotWire {
                        slot_ordinal: 0,
                        kind: AdmissionResolutionKindWire::ProvisionNew,
                        claim_id: None,
                        profile_id: None,
                        runtime_session_id: None,
                        runtime_session_revision: None,
                    }],
                }),
                operations: Vec::new(),
            }),
        };
        let baseline = serde_json::to_value(recovery)?;
        assert!(
            serde_json::from_value::<AgentRequestAdmissionRecoverResponse>(baseline.clone())
                .is_ok()
        );

        let mut missing_snapshot = baseline.clone();
        missing_snapshot
            .as_object_mut()
            .ok_or("recovery object")?
            .remove("snapshot");
        assert!(
            serde_json::from_value::<AgentRequestAdmissionRecoverResponse>(missing_snapshot)
                .is_err()
        );

        let mut missing_resolution = baseline.clone();
        missing_resolution["snapshot"]
            .as_object_mut()
            .ok_or("snapshot object")?
            .remove("resolution");
        assert!(
            serde_json::from_value::<AgentRequestAdmissionRecoverResponse>(missing_resolution)
                .is_err()
        );

        for field in [
            "claimId",
            "profileId",
            "runtimeSessionId",
            "runtimeSessionRevision",
        ] {
            let mut missing_target = baseline.clone();
            missing_target["snapshot"]["resolution"]["slots"][0]
                .as_object_mut()
                .ok_or("slot object")?
                .remove(field);
            assert!(
                serde_json::from_value::<AgentRequestAdmissionRecoverResponse>(missing_target)
                    .is_err()
            );
        }
        Ok(())
    }
}
