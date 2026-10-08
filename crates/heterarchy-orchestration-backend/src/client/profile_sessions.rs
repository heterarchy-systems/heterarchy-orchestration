//! Owner-bound immutable Profile/Session correspondence and readback.

use hyper::{Method, header::HeaderValue};
use uuid::Uuid;

use super::{
    ClientError, OrchestrationHttpClient, ReadbackTarget, canonical_identity, require_identity,
};
use crate::wire::{ProfileSessionBindRequest, ProfileSessionBindResponse};

impl OrchestrationHttpClient {
    /// Link an owned open Session to a Profile through the canonical catalog.
    /// A new nonce for an already-linked pair returns the original receipt.
    ///
    /// # Errors
    /// Preserves authorization/lifecycle/replay rejection and exact pair readback
    /// when a mutation outcome cannot be confirmed.
    pub async fn bind_profile_session(
        &self,
        authorization: &HeaderValue,
        profile_id: Uuid,
        request: &ProfileSessionBindRequest,
    ) -> Result<ProfileSessionBindResponse, ClientError> {
        require_identity(profile_id)?;
        let request_id = canonical_identity(&request.request_id)?;
        let session_id = canonical_identity(&request.session_id)?;
        let readback = ReadbackTarget::ProfileSession {
            request_id,
            profile_id,
            session_id,
        };
        let response: ProfileSessionBindResponse = self
            .call(
                authorization,
                Method::POST,
                format!("/internal/v1/agent-profiles/{profile_id}/sessions"),
                Some(request),
                Some(readback.clone()),
            )
            .await?;
        if !valid_binding(&response, profile_id, session_id)
            || (response.created && response.request_id != request_id)
        {
            return Err(ClientError::UnknownOutcome { readback });
        }
        Ok(response)
    }

    /// Read an exact owner-bound pair, including after the Session closes.
    ///
    /// # Errors
    /// Returns typed access/missing/transport failures and rejects wrong identity
    /// or a peer that describes a read as a newly-created correspondence.
    pub async fn profile_session_binding(
        &self,
        authorization: &HeaderValue,
        profile_id: Uuid,
        session_id: Uuid,
    ) -> Result<ProfileSessionBindResponse, ClientError> {
        require_identity(profile_id)?;
        require_identity(session_id)?;
        let response: ProfileSessionBindResponse = self
            .call::<_, ()>(
                authorization,
                Method::GET,
                format!("/internal/v1/agent-profiles/{profile_id}/sessions/{session_id}"),
                None,
                None,
            )
            .await?;
        if !valid_binding(&response, profile_id, session_id) || response.created {
            return Err(ClientError::Protocol);
        }
        Ok(response)
    }
}

fn valid_binding(
    response: &ProfileSessionBindResponse,
    profile_id: Uuid,
    session_id: Uuid,
) -> bool {
    response.profile_id == profile_id
        && response.session_id == session_id
        && !response.request_id.is_nil()
        && response.profile_revision > 0
        && response.session_revision > 0
}
