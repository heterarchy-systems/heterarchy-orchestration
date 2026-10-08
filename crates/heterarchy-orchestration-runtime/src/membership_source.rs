//! Exact catalog references for the Forge binding-lifecycle transport.
//!
//! These persisted identifiers and hashes select an existing source contract;
//! they are neither a Forge actor nor permission to execute repository work.

use crate::{OrchestrationRuntime, RuntimeError};
use forge_domain::{AuthenticatedPrincipal, WorkspaceId};
use heterarchy_orchestration_operations::{AdmissionIssuerConfig, ImportProgress};
use uuid::Uuid;

/// Canonical source references already recorded by this Runtime's owners.
#[derive(Debug, Clone)]
pub struct MembershipSourceContext {
    pub import: ImportProgress,
    pub issuer: AdmissionIssuerConfig,
}

impl OrchestrationRuntime {
    /// Read a verified import and configured issuer for a caller-owned Session.
    /// Initial registration does not require a membership that it will create.
    ///
    /// # Errors
    /// Rejects invalid/foreign/closed Sessions, missing verified source context
    /// or issuer, and catalog failures. No context is created or guessed.
    pub fn membership_source_context(
        &self,
        workspace_id: Uuid,
        session_id: Uuid,
        principal: AuthenticatedPrincipal,
    ) -> Result<MembershipSourceContext, RuntimeError> {
        if workspace_id.is_nil() {
            return Err(RuntimeError::InvalidInput);
        }
        self.require_open_session(session_id, principal)?;
        self.membership_audit_source_context(workspace_id, session_id, principal)
    }

    /// Read exact source references for immutable audit by an owned, possibly terminal Session.
    ///
    /// # Errors
    /// Rejects missing/foreign Sessions, invalid Workspace and unavailable verified references.
    pub fn membership_audit_source_context(
        &self,
        workspace_id: Uuid,
        session_id: Uuid,
        principal: AuthenticatedPrincipal,
    ) -> Result<MembershipSourceContext, RuntimeError> {
        if workspace_id.is_nil() {
            return Err(RuntimeError::InvalidInput);
        }
        let _session = self.resolve_owned_session(session_id, principal)?;
        let catalog = self.catalog()?;
        let import = catalog
            .verified_workspace_import(WorkspaceId::from_uuid(workspace_id))?
            .ok_or(RuntimeError::SourceAuthorityUnavailable)?;
        let issuer = catalog
            .admission_issuer_config()?
            .ok_or(RuntimeError::SourceAuthorityUnavailable)?;
        Ok(MembershipSourceContext { import, issuer })
    }
}
