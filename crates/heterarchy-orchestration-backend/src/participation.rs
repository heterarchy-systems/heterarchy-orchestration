//! Fresh source-fenced participation commands and exact recovery observations.
use crate::{
    ApiError, BackendState, RuntimeError,
    app_support::{LaneError, validate_workspace_reference},
    wire::participation::{
        InboxAckRequest, InboxAckResponse, InboxEntryWire, InboxListRequest, InboxListResponse,
        InboxParticipantWire, InboxQueryRequest, InboxQueryResponse, ParticipantTarget,
        PresenceQueryRequest, PresenceQueryResponse, PresenceTouchRequest, PresenceTouchResponse,
        PresenceWire,
    },
};
use axum::{
    Json, Router,
    extract::{Extension, Path, State},
    routing::post,
};
use forge_domain::AuthenticatedPrincipal;
use heterarchy_orchestration_operations::{LogicalInboxEntry, LogicalPresenceSnapshot};
use heterarchy_orchestration_runtime::{OrchestrationRuntime, participation::ParticipationTarget};
use uuid::Uuid;

pub(super) fn routes() -> Router<BackendState> {
    Router::new()
        .route(
            "/internal/v1/workspaces/{workspace_id}/presence/touch",
            post(touch),
        )
        .route(
            "/internal/v1/workspaces/{workspace_id}/presence/query",
            post(presence),
        )
        .route(
            "/internal/v1/workspaces/{workspace_id}/inbox/list",
            post(list),
        )
        .route(
            "/internal/v1/workspaces/{workspace_id}/inbox/ack",
            post(ack),
        )
        .route(
            "/internal/v1/workspaces/{workspace_id}/inbox/query",
            post(entry),
        )
}
fn validate(path: &str, target: &ParticipantTarget) -> Result<(), LaneError> {
    validate_workspace_reference(&target.scope, Some(target.session_id))?;
    let id = Uuid::parse_str(path).map_err(|_| LaneError::InvalidIdentity)?;
    if id.to_string() != path
        || id != target.scope.workspace_id
        || target.activity_id.is_nil()
        || target.agent_id.is_nil()
    {
        return Err(LaneError::InvalidIdentity);
    }
    Ok(())
}
fn runtime_target(target: &ParticipantTarget) -> ParticipationTarget {
    ParticipationTarget {
        workspace_id: target.scope.workspace_id,
        activity_id: target.activity_id,
        agent_id: target.agent_id,
        session_id: target.session_id,
    }
}
async fn touch(
    State(state): State<BackendState>,
    Path(workspace): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(body): Json<PresenceTouchRequest>,
) -> Result<Json<PresenceTouchResponse>, LaneError> {
    validate(&workspace, &body.target)?;
    if body.expected_presence_revision < 1
        || body.expected_presence_revision.checked_add(1).is_none()
    {
        return Err(LaneError::InvalidIdentity);
    }
    let target = runtime_target(&body.target);
    let record = state
        .participation_mutation(body.target.clone(), principal, move |runtime, now| {
            runtime.touch_presence(target, principal, body.expected_presence_revision, now)
        })
        .await?;
    Ok(Json(PresenceTouchResponse {
        target: body.target,
        presence: presence_wire(&record),
    }))
}
async fn presence(
    State(state): State<BackendState>,
    Path(workspace): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(body): Json<PresenceQueryRequest>,
) -> Result<Json<PresenceQueryResponse>, LaneError> {
    validate(&workspace, &body.target)?;
    let target = runtime_target(&body.target);
    let record = state
        .participation_query(
            body.target.clone(),
            principal,
            SourceAccess::Workspace,
            move |runtime| runtime.presence(target, principal),
        )
        .await?;
    Ok(Json(PresenceQueryResponse {
        target: body.target,
        presence: record.as_ref().map(presence_wire),
    }))
}
async fn list(
    State(state): State<BackendState>,
    Path(workspace): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(body): Json<InboxListRequest>,
) -> Result<Json<InboxListResponse>, LaneError> {
    validate(&workspace, &body.target)?;
    if !(1..=64).contains(&body.limit) {
        return Err(LaneError::InvalidIdentity);
    }
    let target = runtime_target(&body.target);
    let records = state
        .participation_query(
            body.target.clone(),
            principal,
            SourceAccess::OpenActivity,
            move |runtime| runtime.inbox(target, principal, body.limit),
        )
        .await?;
    Ok(Json(InboxListResponse {
        target: body.target,
        entries: records.iter().map(inbox_wire).collect(),
    }))
}
async fn ack(
    State(state): State<BackendState>,
    Path(workspace): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(body): Json<InboxAckRequest>,
) -> Result<Json<InboxAckResponse>, LaneError> {
    validate(&workspace, &body.target)?;
    if body.event_id.is_nil() {
        return Err(LaneError::InvalidIdentity);
    }
    if body.expected_receipt_revision < 1 || body.expected_receipt_revision.checked_add(1).is_none()
    {
        return Err(LaneError::InvalidIdentity);
    }
    let target = runtime_target(&body.target);
    let record = state
        .participation_mutation(body.target.clone(), principal, move |runtime, now| {
            runtime.acknowledge_inbox(
                target,
                principal,
                body.event_id,
                body.expected_receipt_revision,
                now,
            )
        })
        .await?;
    Ok(Json(InboxAckResponse {
        target: body.target,
        entry: inbox_wire(&record),
    }))
}
async fn entry(
    State(state): State<BackendState>,
    Path(workspace): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(body): Json<InboxQueryRequest>,
) -> Result<Json<InboxQueryResponse>, LaneError> {
    validate(&workspace, &body.target)?;
    if body.event_id.is_nil() {
        return Err(LaneError::InvalidIdentity);
    }
    let target = runtime_target(&body.target);
    let record = state
        .participation_query(
            body.target.clone(),
            principal,
            SourceAccess::Workspace,
            move |runtime| runtime.inbox_entry(target, principal, body.event_id),
        )
        .await?;
    Ok(Json(InboxQueryResponse {
        target: body.target,
        entry: record.as_ref().map(inbox_wire),
    }))
}
#[derive(Clone, Copy)]
enum SourceAccess {
    Workspace,
    OpenActivity,
}
impl BackendState {
    async fn participation_query<T, F>(
        &self,
        target: ParticipantTarget,
        principal: AuthenticatedPrincipal,
        access: SourceAccess,
        operation: F,
    ) -> Result<T, ApiError>
    where
        T: Send + 'static,
        F: FnOnce(&OrchestrationRuntime) -> Result<T, RuntimeError> + Send + 'static,
    {
        let selectors = runtime_target(&target);
        self.catalog_job(move |runtime| runtime.require_participation(selectors, principal))
            .await?;
        match access {
            SourceAccess::Workspace => self.workspace_scope_proof(target.scope).await?,
            SourceAccess::OpenActivity => {
                self.activity_scope_proof(target.scope, target.activity_id)
                    .await?;
            }
        }
        // The Runtime operation rechecks Session and binding authority after the source await.
        self.catalog_job(operation).await
    }
    async fn participation_mutation<T, F>(
        &self,
        target: ParticipantTarget,
        principal: AuthenticatedPrincipal,
        operation: F,
    ) -> Result<T, ApiError>
    where
        T: Send + 'static,
        F: FnOnce(&OrchestrationRuntime, i64) -> Result<T, RuntimeError> + Send + 'static,
    {
        let state = self.clone();
        self.run_owned(async move {
            let selectors = runtime_target(&target);
            state
                .catalog_job(move |runtime| runtime.require_participation(selectors, principal))
                .await?;
            state
                .activity_scope_proof(target.scope.clone(), target.activity_id)
                .await?;
            state
                .runtime
                .ensure_broker_owner()
                .await
                .map_err(RuntimeError::BrokerOwner)
                .map_err(ApiError::Runtime)?;
            state
                .activity_scope_proof(target.scope, target.activity_id)
                .await?;
            state
                .catalog_job(move |runtime| {
                    let now = crate::current_timestamp_millis()
                        .map_err(|_| RuntimeError::InvalidInput)?;
                    operation(runtime, now)
                })
                .await
        })
        .await
    }
}
fn presence_wire(value: &LogicalPresenceSnapshot) -> PresenceWire {
    PresenceWire {
        workspace_id: value.workspace_id().into_uuid(),
        participant_id: value.participant_id(),
        agent_id: value.agent_id().into_uuid(),
        session_id: value.session_id().into_uuid(),
        display_name: value.display_name().as_str().to_owned(),
        membership_revision: value.membership_revision(),
        presence_revision: value.presence_revision().get(),
        last_authenticated_at_ms: value.last_authenticated_at().get(),
    }
}
fn inbox_wire(value: &LogicalInboxEntry) -> InboxEntryWire {
    InboxEntryWire {
        workspace_id: value.workspace_id().into_uuid(),
        activity_id: value.activity_id().into_uuid(),
        event_id: value.event_id(),
        recipient: InboxParticipantWire {
            participant_id: value.recipient_participant_id(),
            agent_id: value.recipient_agent_id().into_uuid(),
            session_id: value.recipient_session_id().into_uuid(),
            display_name: value.recipient_display_name().as_str().to_owned(),
        },
        actor: InboxParticipantWire {
            participant_id: value.actor_participant_id(),
            agent_id: value.actor_agent_id().into_uuid(),
            session_id: value.actor_session_id().into_uuid(),
            display_name: value.actor_display_name().as_str().to_owned(),
        },
        membership_revision: value.membership_revision(),
        observed_at_ms: value.observed_at().get(),
        receipt_revision: value.receipt_revision().get(),
        acknowledged_at_ms: value
            .acknowledged_at()
            .map(forge_domain::TimestampMillis::get),
    }
}
