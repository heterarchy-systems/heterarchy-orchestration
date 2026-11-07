//! Runtime-owned access to immutable, user-authored Reviewer qualification rules.
//!
//! This module stores policy history only. It does not evaluate source facts,
//! qualify a person, accept a Reviewer role, grant evidence access, appoint a
//! Reviewer, or mutate a frozen case.

use forge_domain::{AgentSessionId, AuthenticatedPrincipal, WorkspaceId};
use heterarchy_orchestration_operations::reviewer_qualification_rule::{
    ReviewerQualificationRuleChange, ReviewerQualificationRuleGeneration,
    ReviewerQualificationRuleGenerationRecord, ReviewerQualificationRuleHistoryPage,
    ReviewerQualificationRuleMutationCommand, ReviewerQualificationRuleMutationReceipt,
};
use uuid::Uuid;

use crate::{OrchestrationRuntime, RuntimeError};

/// One exact user-authored rule mutation for a future-pool policy generation.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReviewerQualificationRuleMutationSpec {
    /// Exact logical Session that the Backend bound to the authenticated principal.
    pub session_id: Uuid,
    /// Stable rule identity within the exact Workspace.
    pub rule_id: Uuid,
    /// Stable idempotency identity for this mutation.
    pub request_id: Uuid,
    /// Exact head observed by the caller; `None` means the rule has no history.
    pub expected_current_generation: Option<u64>,
    /// User-authored typed policy change.
    pub change: ReviewerQualificationRuleChange,
}

impl OrchestrationRuntime {
    /// Persist one exact rule generation after host operator authorization.
    ///
    /// The Backend must additionally bind the supplied principal to an open
    /// Session, prove the current Forge Workspace scope, and hold the Broker
    /// owner fence before this method receives it. The Operations command
    /// accepts only a `DirectToken` principal and records that issuer.
    ///
    /// # Errors
    /// Rejects non-operators, non-DirectToken issuers, invalid identities,
    /// stale generation CAS, malformed policy, or durable catalog failures.
    pub fn mutate_reviewer_qualification_rule(
        &self,
        workspace_id: Uuid,
        spec: ReviewerQualificationRuleMutationSpec,
        principal: AuthenticatedPrincipal,
    ) -> Result<ReviewerQualificationRuleMutationReceipt, RuntimeError> {
        self.require_operator(principal)?;
        self.require_open_session(spec.session_id, principal)?;
        let expected_current_generation = spec
            .expected_current_generation
            .map(ReviewerQualificationRuleGeneration::new)
            .transpose()?;
        let command = ReviewerQualificationRuleMutationCommand::new(
            WorkspaceId::from_uuid(workspace_id),
            spec.rule_id,
            spec.request_id,
            expected_current_generation,
            AgentSessionId::from_uuid(spec.session_id),
            principal,
            spec.change,
        )?;
        self.catalog()?
            .mutate_reviewer_qualification_rule(command)
            .map_err(RuntimeError::persistence)
    }

    /// Read the current immutable rule generation for an authorized operator.
    ///
    /// Backend transport callers must first require the current operator's
    /// open Session and exact live Workspace source proof. This query does not
    /// make Workspace-retirement history available through HTTP.
    ///
    /// # Errors
    /// Rejects non-operators, invalid identities, corruption, or storage errors.
    pub fn reviewer_qualification_rule(
        &self,
        workspace_id: Uuid,
        rule_id: Uuid,
        principal: AuthenticatedPrincipal,
    ) -> Result<Option<ReviewerQualificationRuleGenerationRecord>, RuntimeError> {
        self.require_operator(principal)?;
        self.catalog()?
            .reviewer_qualification_rule(WorkspaceId::from_uuid(workspace_id), rule_id)
            .map_err(RuntimeError::persistence)
    }

    /// Read one exact immutable mutation receipt under a current operator.
    ///
    /// Reading a receipt never substitutes the reader as its original issuer.
    /// HTTP still requires a current authorized Session and a live Workspace
    /// source proof; retired Workspace access is not asserted here.
    ///
    /// # Errors
    /// Rejects non-operators, invalid identities, corruption, or storage errors.
    pub fn reviewer_qualification_rule_request(
        &self,
        workspace_id: Uuid,
        rule_id: Uuid,
        request_id: Uuid,
        principal: AuthenticatedPrincipal,
    ) -> Result<Option<ReviewerQualificationRuleGenerationRecord>, RuntimeError> {
        self.require_operator(principal)?;
        self.catalog()?
            .reviewer_qualification_rule_request(
                WorkspaceId::from_uuid(workspace_id),
                rule_id,
                request_id,
            )
            .map_err(RuntimeError::persistence)
    }

    /// Read an ascending bounded immutable history page under a current operator.
    ///
    /// # Errors
    /// Rejects non-operators, invalid identities/cursors/limits, corruption,
    /// or storage errors.
    pub fn reviewer_qualification_rule_history(
        &self,
        workspace_id: Uuid,
        rule_id: Uuid,
        after_generation: Option<u64>,
        limit: u32,
        principal: AuthenticatedPrincipal,
    ) -> Result<ReviewerQualificationRuleHistoryPage, RuntimeError> {
        self.require_operator(principal)?;
        let after = after_generation
            .map(ReviewerQualificationRuleGeneration::new)
            .transpose()?;
        self.catalog()?
            .reviewer_qualification_rule_history(
                WorkspaceId::from_uuid(workspace_id),
                rule_id,
                after,
                limit,
            )
            .map_err(RuntimeError::persistence)
    }
}
