//! Exact Forge planning binding observations; these never resolve an actor.

use serde::{Deserialize, Deserializer, Serialize};
use uuid::Uuid;

use super::super::WorkspaceQueryScope;

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CoordinatorGetRequest {
    pub scope: WorkspaceQueryScope,
    #[serde(deserialize_with = "required_nullable")]
    pub session_id: Option<Uuid>,
    pub activity_id: Uuid,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CoordinatorGetResponse {
    pub scope: WorkspaceQueryScope,
    pub activity_id: Uuid,
    #[serde(deserialize_with = "required_nullable")]
    pub coordinator: Option<CoordinatorBindingWire>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CoordinatorBindingWire {
    pub workspace_id: Uuid,
    pub activity_id: Uuid,
    pub agent_id: Uuid,
    pub revision: i64,
    pub updated_at_ms: i64,
}

impl From<&forge_domain::ActivityCoordinatorBinding> for CoordinatorBindingWire {
    fn from(binding: &forge_domain::ActivityCoordinatorBinding) -> Self {
        Self {
            workspace_id: binding.workspace_id().into_uuid(),
            activity_id: binding.activity_id().into_uuid(),
            agent_id: binding.agent_id().into_uuid(),
            revision: binding.revision().get(),
            updated_at_ms: binding.updated_at().get(),
        }
    }
}

fn required_nullable<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer)
}
