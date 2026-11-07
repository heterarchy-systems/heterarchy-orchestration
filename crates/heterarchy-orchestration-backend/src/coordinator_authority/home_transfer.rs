//! Explicit host-selected Home source identity; never an effect-facts fallback.
use forge_backend::ForgeHttpClient;
use forge_control::{HomeTransferSourceService, ResolvedWorkspaceScope};
use forge_domain::{
    HomeTransferSourceAction, HomeTransferSourceGrant, HomeTransferSourceRequest, TimestampMillis,
};

use super::{CoordinatorAuthority, CoordinatorAuthorityError, SourceTransport};

#[derive(Debug, Clone)]
pub(crate) struct ConfiguredHomeTransferSource {
    client: ForgeHttpClient,
    service: HomeTransferSourceService,
    grant_validity_ms: i64,
}

impl ConfiguredHomeTransferSource {
    pub(crate) const fn client(&self) -> &ForgeHttpClient {
        &self.client
    }
    pub(crate) const fn service(&self) -> HomeTransferSourceService {
        self.service
    }

    /// A new first-attempt grant. This helper does not renew an admitted attempt.
    pub(crate) fn grant(
        &self,
        request: &HomeTransferSourceRequest,
        action: HomeTransferSourceAction,
        issued_at: TimestampMillis,
    ) -> Result<HomeTransferSourceGrant, CoordinatorAuthorityError> {
        let expires_at = issued_at
            .get()
            .checked_add(self.grant_validity_ms)
            .and_then(|value| TimestampMillis::new(value).ok())
            .ok_or(CoordinatorAuthorityError::Stale)?;
        HomeTransferSourceGrant::new(
            self.service.issuer,
            action,
            request.request_digest(),
            issued_at,
            expires_at,
        )
        .map_err(|_| CoordinatorAuthorityError::Stale)
    }
}

impl CoordinatorAuthority {
    /// Configure one explicit Home source and grant duration. No duration or
    /// service identity is inferred from another source feature.
    ///
    /// # Errors
    /// Rejects nil/mismatched service identities and nonpositive durations.
    pub fn with_home_transfer_source(
        mut self,
        client: ForgeHttpClient,
        service: HomeTransferSourceService,
        grant_validity_ms: i64,
    ) -> Result<Self, CoordinatorAuthorityError> {
        if service.issuer.into_uuid().is_nil()
            || service.backend_id.into_uuid().is_nil()
            || service.destination_id.is_nil()
            || grant_validity_ms <= 0
            || client.expected_backend_id() != service.backend_id
            || client.destination_id() != service.destination_id
        {
            return Err(CoordinatorAuthorityError::Denied);
        }
        self.home_transfer_source = Some(ConfiguredHomeTransferSource {
            client,
            service,
            grant_validity_ms,
        });
        Ok(self)
    }

    pub(crate) fn home_transfer_source(
        &self,
        request: &crate::wire::WorkspaceQueryScope,
    ) -> Result<(ResolvedWorkspaceScope, ConfiguredHomeTransferSource), CoordinatorAuthorityError>
    {
        if !matches!(self.transport, SourceTransport::Http(_)) {
            return Err(CoordinatorAuthorityError::Unavailable);
        }
        let scope = self.trusted_scope(request)?;
        let source = self
            .home_transfer_source
            .as_ref()
            .ok_or(CoordinatorAuthorityError::Unavailable)?;
        Ok((scope, source.clone()))
    }
}
