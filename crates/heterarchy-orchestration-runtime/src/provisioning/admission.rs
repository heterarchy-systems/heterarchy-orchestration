//! Complete `AgentRequest` admission flow shared by transport adapters.

use super::operations::ProvisionOperationFullSummary;
use super::{
    AgentRequestCommand, AgentRequestIntent, AgentRequestRequester, AgentRequestResolutionSummary,
    AgentRequestSubmit, AgentRequestSubmitSummary, ProvisioningError,
};
use crate::{OrchestrationRuntime, RuntimeError};
use forge_domain::AuthenticatedPrincipal;

#[cfg(test)]
mod tests;

/// The existing durable stage whose failure a transport must classify.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgentRequestAdmissionStage {
    /// Validate and durably submit the immutable request intent.
    Submit,
    /// Observe candidates and reserve the deterministic resolution.
    Reserve,
    /// Prepare and bind durable provider-effect targets without dispatching.
    Prepare,
    /// Reconcile confirmed participation into request fulfillment.
    Reconcile,
}

/// Same-owner read of an exact request and all currently durable stage evidence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentRequestAdmissionSnapshot {
    pub receipt: AgentRequestSubmitSummary,
    pub resolution: Option<AgentRequestResolutionSummary>,
    pub operations: Vec<ProvisionOperationFullSummary>,
}

pub(super) fn domain_command(
    command: AgentRequestSubmit,
) -> Result<AgentRequestCommand, ProvisioningError> {
    let created_at = super::timestamp(command.created_at_ms)?;
    let requester = AgentRequestRequester::new(
        super::workspace(command.workspace_id)?,
        super::activity(command.activity_id)?,
        super::agent(command.requester_agent_id)?,
        super::session(command.requester_session_id)?,
        command.coordinator_revision,
    )?;
    let intent = AgentRequestIntent::new(
        command.objective,
        command.capability,
        command.count,
        command.effect_scope,
        command.independence,
        command.reason,
    )?;
    Ok(AgentRequestCommand::new(
        command.request_id,
        requester,
        intent,
        created_at,
    )?)
}

impl OrchestrationRuntime {
    /// Read exact original intent and partial/complete stage evidence without
    /// current Source authority, Broker effects, or retry authorization.
    ///
    /// # Errors
    /// Requires the original owned open Session; rejects changed immutable
    /// intent and preserves catalog failures. A foreign Workspace is absent.
    pub fn recover_agent_request_admission(
        &self,
        command: AgentRequestSubmit,
        principal: AuthenticatedPrincipal,
    ) -> Result<Option<AgentRequestAdmissionSnapshot>, RuntimeError> {
        self.require_open_session(command.requester_session_id, principal)?;
        let request_id = command.request_id;
        let workspace_id = command.workspace_id;
        let expected = domain_command(command)?;
        let catalog = self.catalog()?;
        let Some(record) = catalog.agent_request(request_id)? else {
            return Ok(None);
        };
        if record.workspace_id().into_uuid() != workspace_id {
            return Ok(None);
        }
        if !record.matches_intent(&expected) {
            return Err(RuntimeError::Conflict);
        }
        let resolution = catalog.agent_request_resolution(request_id)?;
        let mut operations = Vec::with_capacity(usize::from(record.count()));
        if let Some(resolution) = &resolution {
            for slot in resolution.slots() {
                if let Some(operation) = catalog.agent_provision_operation_for_request_slot(
                    request_id,
                    slot.selection().slot_ordinal(),
                )? {
                    operations.push(super::operations::full_summary(&operation));
                }
            }
        }
        Ok(Some(AgentRequestAdmissionSnapshot {
            receipt: AgentRequestSubmitSummary::from_record(&record, false),
            resolution: resolution
                .as_ref()
                .map(|value| AgentRequestResolutionSummary::from_resolution(value, false)),
            operations,
        }))
    }

    /// Run the complete existing submit/reserve/prepare/reconcile sequence.
    ///
    /// The caller proves coordinator authority and owns bounded admission.
    /// Clock and error conversion stay at the adapter boundary. Clock reads
    /// occur at the same stage boundaries as the native workflow, outside
    /// catalog guards. This operation neither dispatches provider effects nor
    /// authorizes retries after an incomplete stage.
    ///
    /// # Errors
    /// Returns the adapter's clock failure or stage-classified provisioning
    /// error. Failures after submission can leave durable intent or targets;
    /// callers must retain exact request readback rather than imply rollback.
    pub fn admit_agent_request<E>(
        &self,
        command: AgentRequestSubmit,
        mut clock: impl FnMut(AgentRequestAdmissionStage) -> Result<i64, E>,
        mut map_error: impl FnMut(AgentRequestAdmissionStage, ProvisioningError) -> E,
    ) -> Result<AgentRequestSubmitSummary, E> {
        use AgentRequestAdmissionStage::{Prepare, Reconcile, Reserve, Submit};
        let submitted = self
            .submit_agent_request(command)
            .map_err(|error| map_error(Submit, error))?;
        let request_id = submitted.request_id;
        let claimed_at = clock(Reserve)?;
        let reserved_at = clock(Reserve)?;
        self.reserve_agent_request_resolution(request_id, claimed_at, reserved_at)
            .map_err(|error| map_error(Reserve, error))?;
        let prepared_at = clock(Prepare)?;
        let bound_at = clock(Prepare)?;
        self.prepare_agent_request_effect_targets(request_id, prepared_at, bound_at)
            .map_err(|error| map_error(Prepare, error))?;
        let mut reconciled = self
            .reconcile_agent_request_fulfillment(request_id, clock(Reconcile)?)
            .map_err(|error| map_error(Reconcile, error))?;
        reconciled.created = submitted.created;
        Ok(reconciled)
    }
}
