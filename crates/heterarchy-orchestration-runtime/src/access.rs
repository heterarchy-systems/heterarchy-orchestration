//! Host-selected operator grants, separate from credential authentication.

use forge_domain::AuthenticatedPrincipal;

use crate::OrchestrationRuntime;
use crate::RuntimeError;
use forge_domain::{AgentSessionId, WorkspaceId};
use uuid::Uuid;

const MAX_OPERATOR_PRINCIPALS: usize = 16;

/// Immutable grants supplied by trusted host configuration. Token labels,
/// request fields and a valid bearer credential never create these grants.
#[derive(Debug, Clone, Default)]
pub struct OperatorPolicy {
    principals: Vec<AuthenticatedPrincipal>,
}

impl OperatorPolicy {
    /// Grant operator permission to exact existing authenticated identities.
    /// Native `TrustedStdio` remains a distinct trusted host boundary.
    ///
    /// # Errors
    /// Rejects excessive, duplicate or nil remote identities.
    pub fn new(principals: Vec<AuthenticatedPrincipal>) -> Result<Self, RuntimeError> {
        if principals.len() > MAX_OPERATOR_PRINCIPALS {
            return Err(RuntimeError::InvalidInput);
        }
        for (index, principal) in principals.iter().enumerate() {
            let valid = match principal {
                AuthenticatedPrincipal::TrustedStdio => false,
                AuthenticatedPrincipal::DirectToken(id) => !id.into_uuid().is_nil(),
                AuthenticatedPrincipal::OAuthGrant(id) => !id.into_uuid().is_nil(),
            };
            if !valid || principals[..index].contains(principal) {
                return Err(RuntimeError::InvalidInput);
            }
        }
        Ok(Self { principals })
    }

    pub(super) fn authorize(&self, principal: AuthenticatedPrincipal) -> Result<(), RuntimeError> {
        if principal.is_trusted_stdio() || self.principals.contains(&principal) {
            Ok(())
        } else {
            Err(RuntimeError::Forbidden)
        }
    }
}

impl OrchestrationRuntime {
    /// Authorize a Workspace observation from current logical membership.
    /// An exact host-granted operator may observe all scopes; other callers
    /// must name their own open session and its canonical Workspace membership.
    ///
    /// # Errors
    /// Returns forbidden for absent/foreign/closed membership and preserves
    /// authority failures rather than substituting a cached permission.
    pub fn require_workspace_read(
        &self,
        workspace_id: Uuid,
        session_id: Option<Uuid>,
        principal: AuthenticatedPrincipal,
    ) -> Result<(), RuntimeError> {
        if workspace_id.is_nil() {
            return Err(RuntimeError::InvalidInput);
        }
        if self.require_operator(principal).is_ok() {
            return Ok(());
        }
        let session_id = session_id.ok_or(RuntimeError::Forbidden)?;
        self.require_open_session(session_id, principal)?;
        let catalog = self.catalog()?;
        catalog
            .logical_identity_for_session(
                WorkspaceId::from_uuid(workspace_id),
                AgentSessionId::from_uuid(session_id),
            )?
            .ok_or(RuntimeError::Forbidden)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use forge_domain::{DirectTokenId, OAuthGrantId};
    use uuid::Uuid;

    use super::*;

    #[test]
    fn operator_grants_are_explicit_and_do_not_cross_principal_kinds() -> Result<(), RuntimeError> {
        let id = Uuid::from_u128(17);
        let operator = AuthenticatedPrincipal::DirectToken(DirectTokenId::from_uuid(id));
        let oauth = AuthenticatedPrincipal::OAuthGrant(OAuthGrantId::from_uuid(id));
        assert!(matches!(
            OperatorPolicy::default().authorize(operator),
            Err(RuntimeError::Forbidden)
        ));
        let policy = OperatorPolicy::new(vec![operator])?;
        policy.authorize(operator)?;
        assert!(matches!(
            policy.authorize(oauth),
            Err(RuntimeError::Forbidden)
        ));
        policy.authorize(AuthenticatedPrincipal::TrustedStdio)?;
        assert!(OperatorPolicy::new(vec![operator, operator]).is_err());
        Ok(())
    }
}
