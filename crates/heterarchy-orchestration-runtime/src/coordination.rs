//! Caller-authorized stages over the existing Activity coordination projection.

use crate::{OrchestrationRuntime, RuntimeError};
use forge_domain::{ActivityId, AuthenticatedPrincipal, WorkspaceId};
use heterarchy_orchestration_operations::{
    EffectFactConsumerProgress, EffectFactInput, LogicalActivitySnapshot,
};
use uuid::Uuid;

impl OrchestrationRuntime {
    /// Apply already validated observation inputs through the existing atomic consumer.
    ///
    /// # Errors
    /// Rejects invalid source inputs, stale cursor or uncertain persistence outcomes.
    pub fn apply_coordination_effect_inputs(
        &self,
        migration_id: Uuid,
        inputs: &[EffectFactInput<'_>],
    ) -> Result<EffectFactConsumerProgress, RuntimeError> {
        self.catalog()?
            .apply_effect_fact_inputs(migration_id, inputs)
            .map_err(Into::into)
    }
    /// Read a verified consumer cursor after existing Workspace read authorization.
    ///
    /// # Errors
    /// Rejects foreign callers, invalid selectors and damaged/unverified source state.
    pub fn coordination_source_cursor(
        &self,
        workspace_id: Uuid,
        session_id: Option<Uuid>,
        principal: AuthenticatedPrincipal,
    ) -> Result<Option<EffectFactConsumerProgress>, RuntimeError> {
        self.require_workspace_read(workspace_id, session_id, principal)?;
        self.catalog()?
            .effect_fact_source_cursor(WorkspaceId::from_uuid(workspace_id))
            .map_err(Into::into)
    }

    /// Read the canonical bounded logical snapshot; Forge planning is a separate observation.
    ///
    /// # Errors
    /// Rejects invalid Activity, unauthorized callers and corrupt projection state.
    pub fn coordination_snapshot(
        &self,
        workspace_id: Uuid,
        activity_id: Uuid,
        session_id: Option<Uuid>,
        principal: AuthenticatedPrincipal,
    ) -> Result<LogicalActivitySnapshot, RuntimeError> {
        if activity_id.is_nil() {
            return Err(RuntimeError::InvalidInput);
        }
        self.require_workspace_read(workspace_id, session_id, principal)?;
        self.catalog()?
            .logical_activity_snapshot(
                WorkspaceId::from_uuid(workspace_id),
                ActivityId::from_uuid(activity_id),
            )
            .map_err(Into::into)
    }
}
