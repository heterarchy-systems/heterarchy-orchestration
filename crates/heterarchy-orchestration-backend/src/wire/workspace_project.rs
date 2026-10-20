//! Session-free operator contract for Workspace `ChatGPT` Project bindings.

use forge_domain::TimestampMillis;
use heterarchy_orchestration_operations::{
    ChatGptProjectId, WorkspaceProjectBinding, WorkspaceProjectBindingReceipt,
    WorkspaceProjectBindingRevision, WorkspaceProjectConfigureCommand,
    WorkspaceProjectConfigureOutcome, WorkspaceProjectReference, WorkspaceProjectScope,
};
use serde::{Deserialize, Deserializer, Serialize};
use std::str::FromStr;
use uuid::Uuid;

use super::WorkspaceQueryScope;

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkspaceProjectQueryRequest {
    pub scope: WorkspaceQueryScope,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkspaceProjectQueryResponse {
    pub workspace_id: Uuid,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub binding: Option<WorkspaceProjectBindingWire>,
    pub work_authorized: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkspaceProjectConfigureRequest {
    pub request_id: Uuid,
    pub scope: WorkspaceQueryScope,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub expected_revision: Option<String>,
    pub project_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkspaceProjectConfigureResponse {
    pub receipt: WorkspaceProjectBindingReceiptWire,
    pub current_binding: WorkspaceProjectBindingWire,
    pub replayed: bool,
    pub work_authorized: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkspaceProjectReadbackRequest {
    pub request_id: Uuid,
    pub scope: WorkspaceQueryScope,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkspaceProjectReadbackResponse {
    #[serde(deserialize_with = "deserialize_required_option")]
    pub receipt: Option<WorkspaceProjectBindingReceiptWire>,
    pub retry_authorized: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkspaceProjectBindingWire {
    pub workspace_id: Uuid,
    pub project_id: String,
    pub revision: String,
    pub configured_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkspaceProjectBindingReceiptWire {
    pub request_id: Uuid,
    pub scope: WorkspaceQueryScope,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub expected_revision: Option<String>,
    pub project_id: String,
    pub binding: WorkspaceProjectBindingWire,
}

/// Optional frozen provider route used by provision, chat, and message targets.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ProviderProjectReferenceWire {
    pub project_id: String,
    pub binding_revision: String,
}

impl TryFrom<&WorkspaceQueryScope> for WorkspaceProjectScope {
    type Error = heterarchy_orchestration_operations::OperationsError;

    fn try_from(scope: &WorkspaceQueryScope) -> Result<Self, Self::Error> {
        let locator = forge_control::WorkspaceLocator::decode(&scope.locator).map_err(|_| {
            heterarchy_orchestration_operations::OperationsError::Conflict(
                "ChatGPT Project Workspace locator",
            )
        })?;
        if locator.encode() != scope.locator {
            return Err(
                heterarchy_orchestration_operations::OperationsError::Conflict(
                    "ChatGPT Project Workspace locator",
                ),
            );
        }
        if locator.location_id().into_uuid() != scope.location_id
            || locator.expected_revision().get() != scope.location_revision
        {
            return Err(
                heterarchy_orchestration_operations::OperationsError::Conflict(
                    "ChatGPT Project Workspace locator",
                ),
            );
        }
        Self::new(
            scope.workspace_id,
            scope.location_id,
            scope.location_revision,
            scope.workspace_revision,
            scope.locator.clone(),
        )
    }
}

impl WorkspaceProjectConfigureRequest {
    /// Convert the wire intent into a checked source mutation command.
    ///
    /// # Errors
    /// Rejects noncanonical Workspace locators, Project IDs, or revisions and
    /// nil request identities.
    pub fn into_command(
        self,
        configured_at: TimestampMillis,
    ) -> Result<
        WorkspaceProjectConfigureCommand,
        heterarchy_orchestration_operations::OperationsError,
    > {
        let scope = WorkspaceProjectScope::try_from(&self.scope)?;
        let expected_revision = self
            .expected_revision
            .as_deref()
            .map(WorkspaceProjectBindingRevision::from_str)
            .transpose()?;
        let project_id = ChatGptProjectId::try_from(self.project_id.as_str())?;
        WorkspaceProjectConfigureCommand::new(
            self.request_id,
            scope,
            expected_revision,
            project_id,
            configured_at,
        )
    }
}

impl From<&WorkspaceProjectBinding> for WorkspaceProjectBindingWire {
    fn from(binding: &WorkspaceProjectBinding) -> Self {
        Self {
            workspace_id: binding.workspace_id(),
            project_id: binding.project_id().to_string(),
            revision: binding.revision().to_string(),
            configured_at_ms: binding.configured_at().get(),
        }
    }
}

impl From<&WorkspaceProjectBindingReceipt> for WorkspaceProjectBindingReceiptWire {
    fn from(receipt: &WorkspaceProjectBindingReceipt) -> Self {
        Self {
            request_id: receipt.request_id(),
            scope: scope_wire(receipt.scope()),
            expected_revision: receipt.expected_revision().map(|value| value.to_string()),
            project_id: receipt.project_id().to_string(),
            binding: WorkspaceProjectBindingWire::from(receipt.binding()),
        }
    }
}

impl From<(&WorkspaceProjectConfigureOutcome, bool)> for WorkspaceProjectConfigureResponse {
    fn from((outcome, work_authorized): (&WorkspaceProjectConfigureOutcome, bool)) -> Self {
        Self {
            receipt: WorkspaceProjectBindingReceiptWire::from(outcome.receipt()),
            current_binding: WorkspaceProjectBindingWire::from(outcome.current_binding()),
            replayed: outcome.replayed(),
            work_authorized,
        }
    }
}

impl From<WorkspaceProjectReference> for ProviderProjectReferenceWire {
    fn from(reference: WorkspaceProjectReference) -> Self {
        Self {
            project_id: reference.project_id().to_string(),
            binding_revision: reference.binding_revision().to_string(),
        }
    }
}

impl TryFrom<ProviderProjectReferenceWire> for WorkspaceProjectReference {
    type Error = heterarchy_orchestration_operations::OperationsError;

    fn try_from(value: ProviderProjectReferenceWire) -> Result<Self, Self::Error> {
        let project_id = ChatGptProjectId::try_from(value.project_id.as_str())?;
        let binding_revision = WorkspaceProjectBindingRevision::from_str(&value.binding_revision)?;
        Ok(Self::new(project_id, binding_revision))
    }
}

#[must_use]
pub fn valid_provider_project_id(value: &str) -> bool {
    ChatGptProjectId::try_from(value).is_ok()
}

#[must_use]
pub fn valid_provider_project_reference(value: &ProviderProjectReferenceWire) -> bool {
    WorkspaceProjectReference::try_from(value.clone()).is_ok()
}

#[must_use]
pub fn scope_wire(scope: &WorkspaceProjectScope) -> WorkspaceQueryScope {
    WorkspaceQueryScope {
        workspace_id: scope.workspace_id(),
        location_id: scope.location_id(),
        location_revision: scope.location_revision(),
        workspace_revision: scope.workspace_revision(),
        locator: scope.locator().to_owned(),
    }
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
    use crate::wire::provisioning_operations::ProvisionOperationWire;
    use serde_json::{Value, json};
    use std::error::Error;

    #[test]
    fn legacy_operation_omits_project_but_explicit_null_is_rejected() -> Result<(), Box<dyn Error>>
    {
        let operation = ProvisionOperationWire {
            operation_id: Uuid::from_u128(1),
            request_id: Uuid::from_u128(2),
            slot_ordinal: 0,
            effect_kind: "provision_new".to_owned(),
            provider_family: "chatgpt".to_owned(),
            profile_id: None,
            runtime_session_id: None,
            state: "prepared".to_owned(),
            provider_conversation_id: None,
            reported_failure_code: None,
            prepared_at_ms: 1,
            updated_at_ms: 1,
            revision: 1,
            uncertainty_retirement: None,
            project: None,
        };
        let mut value = serde_json::to_value(&operation)?;
        assert_eq!(
            serde_json::from_value::<ProvisionOperationWire>(value.clone())?,
            operation
        );
        assert!(value.get("project").is_none());

        let object = value.as_object_mut().ok_or("operation object")?;
        object.insert("project".to_owned(), Value::Null);
        assert!(serde_json::from_value::<ProvisionOperationWire>(value.clone()).is_err());

        let object = value.as_object_mut().ok_or("operation object")?;
        object.insert(
            "project".to_owned(),
            json!({
                "projectId": "g-p-6ad231f7ad7c8191bfc1d5ff782c171f",
                "bindingRevision": "1"
            }),
        );
        let bound: ProvisionOperationWire = serde_json::from_value(value)?;
        assert!(valid_provider_project_reference(
            bound.project.as_ref().ok_or("frozen Project route")?
        ));
        Ok(())
    }

    #[test]
    fn workspace_project_query_and_configure_require_work_authorized() {
        let workspace_id = Uuid::from_u128(0xA111);
        let request_id = Uuid::from_u128(0xA112);
        let project_id = "g-p-6ad231f7ad7c8191bfc1d5ff782c171f";
        let scope = json!({
            "workspaceId": workspace_id,
            "locationId": Uuid::from_u128(0xA113),
            "locationRevision": 3,
            "workspaceRevision": 4,
            "locator": "hf1:example",
        });
        let binding = json!({
            "workspaceId": workspace_id,
            "projectId": project_id,
            "revision": "1",
            "configuredAtMs": 10,
        });

        assert!(
            serde_json::from_value::<WorkspaceProjectQueryResponse>(json!({
                "workspaceId": workspace_id,
                "binding": null,
                "workAuthorized": false,
            }))
            .is_ok()
        );
        assert!(
            serde_json::from_value::<WorkspaceProjectQueryResponse>(json!({
                "workspaceId": workspace_id,
                "binding": null,
            }))
            .is_err()
        );

        let receipt = json!({
            "requestId": request_id,
            "scope": scope,
            "expectedRevision": null,
            "projectId": project_id,
            "binding": binding,
        });
        assert!(
            serde_json::from_value::<WorkspaceProjectConfigureResponse>(json!({
                "receipt": receipt,
                "currentBinding": binding,
                "replayed": false,
                "workAuthorized": true,
            }))
            .is_ok()
        );
        assert!(
            serde_json::from_value::<WorkspaceProjectConfigureResponse>(json!({
                "receipt": receipt,
                "currentBinding": binding,
                "replayed": false,
            }))
            .is_err()
        );
    }
}
