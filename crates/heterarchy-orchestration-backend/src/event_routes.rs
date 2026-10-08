//! Authenticated SSE composition; event filtering remains transport-owned.

use axum::{
    Extension,
    extract::{Path, State},
    http::HeaderMap,
    response::{IntoResponse, Response},
};
use forge_domain::AuthenticatedPrincipal;
use uuid::Uuid;

use crate::{
    ApiError, BackendState, RuntimeError,
    auth::{BearerAuthorizer, CheckedCredential},
    realtime::{self, ResyncReason},
};

pub(super) async fn global(
    State(state): State<BackendState>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Extension(authorizer): Extension<BearerAuthorizer>,
    Extension(credential): Extension<CheckedCredential>,
    headers: HeaderMap,
) -> Response {
    serve_events(
        state, principal, authorizer, credential, headers, None, None,
    )
    .await
}

pub(super) async fn workspace(
    State(state): State<BackendState>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Extension(authorizer): Extension<BearerAuthorizer>,
    Path(workspace): Path<String>,
    Extension(credential): Extension<CheckedCredential>,
    headers: HeaderMap,
) -> Response {
    let scope = match crate::canonical_uuid(&workspace) {
        Ok(scope) => scope,
        Err(error) => return error.into_response(),
    };
    let session = match crate::app_support::session_selector(&headers) {
        Ok(session) => session,
        Err(error) => return error.into_response(),
    };
    serve_events(
        state,
        principal,
        authorizer,
        credential,
        headers,
        Some(scope),
        session,
    )
    .await
}

async fn authorize(
    state: &BackendState,
    principal: AuthenticatedPrincipal,
    authorizer: &BearerAuthorizer,
    credential: &CheckedCredential,
    workspace: Option<Uuid>,
    session: Option<Uuid>,
) -> Result<(), ApiError> {
    if !authorizer
        .revalidate(credential)
        .await
        .map_err(|_| ApiError::Unavailable)?
    {
        return Err(ApiError::Runtime(RuntimeError::Forbidden));
    }
    match workspace {
        None => state
            .runtime
            .require_operator(principal)
            .map_err(ApiError::Runtime),
        Some(workspace) => {
            state
                .run_catalog(move |runtime| {
                    runtime.require_workspace_read(workspace, session, principal)
                })
                .await
        }
    }
}

async fn serve_events(
    state: BackendState,
    principal: AuthenticatedPrincipal,
    authorizer: BearerAuthorizer,
    credential: CheckedCredential,
    headers: HeaderMap,
    workspace: Option<Uuid>,
    session: Option<Uuid>,
) -> Response {
    if let Err(error) = authorize(
        &state,
        principal,
        &authorizer,
        &credential,
        workspace,
        session,
    )
    .await
    {
        return error.into_response();
    }
    let hub = state.realtime();
    realtime::events_handler_authorized(State(hub), headers, workspace, move || {
        let state = state.clone();
        let authorizer = authorizer.clone();
        let credential = credential.clone();
        async move {
            authorize(
                &state,
                principal,
                &authorizer,
                &credential,
                workspace,
                session,
            )
            .await
            .map_err(|error| {
                if matches!(error, ApiError::Runtime(RuntimeError::Forbidden)) {
                    ResyncReason::AccessRevoked
                } else {
                    ResyncReason::AuthorityUnavailable
                }
            })
        }
    })
    .into_response()
}
