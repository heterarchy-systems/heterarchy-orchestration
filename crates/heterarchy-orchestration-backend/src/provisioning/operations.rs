//! Source-fenced readback for historical Agent provision operations.

use axum::extract::{Extension, Path, State};
use axum::routing::post;
use axum::{Json, Router};
use forge_domain::AuthenticatedPrincipal;
use heterarchy_orchestration_runtime::provisioning::operations::ProvisionOperationFullSummary;
use uuid::Uuid;

use crate::BackendState;
use crate::app_support::{LaneError, validate_workspace_reference};
use crate::wire::WorkspaceQueryScope;
use crate::wire::provisioning_operations::{
    ProvisionOperationQueryRequest, ProvisionOperationQueryResponse,
    ProvisionOperationUncertaintyRetirementWire, ProvisionOperationWire,
};

pub(super) fn routes() -> Router<BackendState> {
    Router::new().route(
        "/internal/v1/workspaces/{workspace_id}/provision-operations/query",
        post(query),
    )
}

async fn query(
    State(state): State<BackendState>,
    Path(workspace): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(body): Json<ProvisionOperationQueryRequest>,
) -> Result<Json<ProvisionOperationQueryResponse>, LaneError> {
    validate_scope(&workspace, &body.scope, body.session_id)?;
    if body.operation_id.is_nil() {
        return Err(LaneError::InvalidIdentity);
    }
    let workspace_id = body.scope.workspace_id;
    let operation_id = body.operation_id;
    let operation = state
        .run_workspace_reference_query(
            body.scope.clone(),
            body.session_id,
            principal,
            move |runtime, _| runtime.provision_operation(workspace_id, operation_id),
        )
        .await?;
    Ok(Json(ProvisionOperationQueryResponse {
        scope: body.scope,
        operation: operation.as_ref().map(operation_wire),
    }))
}

fn validate_scope(
    path: &str,
    scope: &WorkspaceQueryScope,
    session: Option<Uuid>,
) -> Result<(), LaneError> {
    validate_workspace_reference(scope, session)?;
    if canonical_uuid(path)? != scope.workspace_id {
        return Err(LaneError::InvalidIdentity);
    }
    Ok(())
}

fn canonical_uuid(value: &str) -> Result<Uuid, LaneError> {
    let parsed = Uuid::parse_str(value).map_err(|_| LaneError::InvalidIdentity)?;
    if parsed.is_nil() || parsed.to_string() != value {
        return Err(LaneError::InvalidIdentity);
    }
    Ok(parsed)
}

pub(super) fn operation_wire(value: &ProvisionOperationFullSummary) -> ProvisionOperationWire {
    ProvisionOperationWire {
        operation_id: value.operation_id,
        request_id: value.request_id,
        slot_ordinal: value.slot_ordinal,
        effect_kind: value.effect_kind.to_owned(),
        provider_family: value.provider_family.to_owned(),
        profile_id: value.profile_id,
        runtime_session_id: value.runtime_session_id,
        state: value.state.to_owned(),
        provider_conversation_id: value.provider_conversation_id.clone(),
        reported_failure_code: value.reported_failure_code.map(str::to_owned),
        prepared_at_ms: value.prepared_at_ms,
        updated_at_ms: value.updated_at_ms,
        revision: value.revision,
        uncertainty_retirement: value
            .uncertainty_retirement
            .as_ref()
            .map(ProvisionOperationUncertaintyRetirementWire::from),
    }
}
