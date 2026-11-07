//! Strict HTTP DTOs for user-authored Reviewer qualification-rule history.
//!
//! These requests publish policy data only. They do not submit source facts,
//! evaluate a candidate, issue qualification, grant evidence access, or alter
//! an existing case.

use heterarchy_orchestration_operations::{OperationsError, reviewer_qualification_rule as rule};
use serde::{Deserialize, Deserializer, Serialize};
use std::collections::BTreeSet;
use uuid::Uuid;

use super::WorkspaceQueryScope;

/// Upper bound for a policy request, including exact predicate allow-lists.
pub const MAX_REVIEWER_QUALIFICATION_RULE_BODY_BYTES: usize = 32 * 1024;

/// Maximum number of immutable generations returned by one history request.
pub const MAX_REVIEWER_QUALIFICATION_RULE_HISTORY_PAGE_SIZE: u32 = 64;

/// One bounded user-selected source-owned qualification rule policy.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ReviewerQualificationRulePolicyWire {
    pub domain: String,
    pub permitted_scope: String,
    pub evidence_requirements: Vec<QualificationEvidenceRequirementWire>,
}

/// One namespaced evidence owner named by the authored policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct EvidenceSourceOwnerWire {
    pub namespace: EvidenceOwnerNamespaceWire,
    pub id: Uuid,
}

/// The source namespace that owns a referenced evidence class.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceOwnerNamespaceWire {
    Forge,
    RuntimeAdapter,
}

/// Closed set of actual runtime facts that a policy can name.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ActualRuntimeFactWire {
    Provider,
    Engine,
    Model,
    ToolVersions,
    EnvironmentFingerprint,
}

/// One exact runtime text or opaque tagged-digest allow-list predicate.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(tag = "valueKind", rename_all = "snake_case", deny_unknown_fields)]
pub enum RuntimeFactPredicateWire {
    Text {
        fact: ActualRuntimeFactWire,
        allowed_values: Vec<String>,
    },
    Digest {
        fact: ActualRuntimeFactWire,
        allowed_values: Vec<RuntimeFactDigestWire>,
    },
}

/// Opaque digest bytes with the user-selected algorithm label.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RuntimeFactDigestWire {
    pub algorithm: String,
    pub bytes: Vec<u8>,
}

/// Explicit source evidence class selected by a user-authored policy.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum QualificationEvidenceRequirementWire {
    ActualRuntimeProvenance {
        source_owner: EvidenceSourceOwnerWire,
        fact_predicates: Vec<RuntimeFactPredicateWire>,
    },
    IndependentlyAcceptedWork {
        source_owner: EvidenceSourceOwnerWire,
    },
}

/// Publish or deactivate one exact rule generation.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ReviewerQualificationRuleMutationRequest {
    pub scope: WorkspaceQueryScope,
    /// Session selector, verified against the authenticated principal by Runtime.
    pub session_id: Uuid,
    pub rule_id: Uuid,
    pub request_id: Uuid,
    /// Explicit `null` creates the first generation; omission is malformed.
    #[serde(deserialize_with = "deserialize_required_option")]
    pub expected_current_generation: Option<u64>,
    pub change: ReviewerQualificationRuleChangeWire,
}

impl ReviewerQualificationRuleMutationRequest {
    /// Create a read-only query for the exact original mutation using a
    /// caller-supplied current Session. This preserves mutation identity while
    /// allowing an authorized current operator to recover after token rotation.
    #[must_use]
    pub fn readback_request(
        &self,
        current_scope: &WorkspaceQueryScope,
        current_session_id: Uuid,
    ) -> Option<ReviewerQualificationRuleRequestReadRequest> {
        (current_scope.workspace_id == self.scope.workspace_id).then(|| {
            ReviewerQualificationRuleRequestReadRequest {
                scope: current_scope.clone(),
                session_id: current_session_id,
                rule_id: self.rule_id,
                request_id: self.request_id,
            }
        })
    }

    /// Convert checked transport shape to the Operations policy/change type.
    /// Durable Operations construction validates every semantic bound again.
    pub(crate) fn operations_change(
        &self,
    ) -> Result<rule::ReviewerQualificationRuleChange, OperationsError> {
        self.change.operations_change()
    }

    /// Validate the exact selector values accepted by either mutation route.
    pub(crate) fn validate_identity(&self) -> bool {
        valid_scope(&self.scope, self.session_id)
            && !self.rule_id.is_nil()
            && !self.request_id.is_nil()
            && self
                .expected_current_generation
                .is_none_or(|value| valid_generation(value) && value < i64::MAX as u64)
    }
}

/// Closed mutation operation carried by a mutation request body.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ReviewerQualificationRuleChangeWire {
    Publish {
        policy: ReviewerQualificationRulePolicyWire,
    },
    DeactivateForFuturePools,
}

impl ReviewerQualificationRuleChangeWire {
    pub(crate) fn operations_change(
        &self,
    ) -> Result<rule::ReviewerQualificationRuleChange, OperationsError> {
        match self {
            Self::Publish { policy } => Ok(rule::ReviewerQualificationRuleChange::Publish {
                policy: policy.operations_policy()?,
            }),
            Self::DeactivateForFuturePools => {
                Ok(rule::ReviewerQualificationRuleChange::DeactivateForFuturePools)
            }
        }
    }

    #[must_use]
    pub const fn is_publish(&self) -> bool {
        matches!(self, Self::Publish { .. })
    }
}

impl ReviewerQualificationRulePolicyWire {
    fn operations_policy(&self) -> Result<rule::ReviewerQualificationRulePolicy, OperationsError> {
        let domain = rule::QualificationDomain::new(self.domain.clone())?;
        let permitted_scope = rule::QualificationScope::new(self.permitted_scope.clone())?;
        if self.evidence_requirements.is_empty()
            || self.evidence_requirements.len() > rule::MAX_QUALIFICATION_EVIDENCE_REQUIREMENTS
        {
            return Err(OperationsError::Bounds(
                "qualification evidence requirements",
            ));
        }
        let mut evidence_requirements = BTreeSet::new();
        for requirement in &self.evidence_requirements {
            if !evidence_requirements.insert(requirement.operations_requirement()?) {
                return Err(OperationsError::Bounds(
                    "duplicate qualification evidence requirement",
                ));
            }
        }
        rule::ReviewerQualificationRulePolicy::new(domain, permitted_scope, evidence_requirements)
    }

    fn from_operations(value: &rule::ReviewerQualificationRulePolicy) -> Self {
        Self {
            domain: value.domain().as_str().to_owned(),
            permitted_scope: value.permitted_scope().as_str().to_owned(),
            evidence_requirements: value
                .evidence_requirements()
                .iter()
                .map(QualificationEvidenceRequirementWire::from_operations)
                .collect(),
        }
    }
}

impl QualificationEvidenceRequirementWire {
    fn operations_requirement(
        &self,
    ) -> Result<rule::QualificationEvidenceRequirement, OperationsError> {
        match self {
            Self::ActualRuntimeProvenance {
                source_owner,
                fact_predicates,
            } => {
                if fact_predicates.is_empty()
                    || fact_predicates.len() > rule::MAX_RUNTIME_FACT_PREDICATES
                {
                    return Err(OperationsError::Bounds(
                        "qualification runtime provenance facts",
                    ));
                }
                let mut predicates = BTreeSet::new();
                for predicate in fact_predicates {
                    if !predicates.insert(predicate.operations_predicate()?) {
                        return Err(OperationsError::Bounds(
                            "duplicate qualification runtime fact predicate",
                        ));
                    }
                }
                Ok(
                    rule::QualificationEvidenceRequirement::ActualRuntimeProvenance {
                        source_owner: source_owner.operations_owner()?,
                        fact_predicates: predicates,
                    },
                )
            }
            Self::IndependentlyAcceptedWork { source_owner } => Ok(
                rule::QualificationEvidenceRequirement::IndependentlyAcceptedWork {
                    source_owner: source_owner.operations_owner()?,
                },
            ),
        }
    }

    fn from_operations(value: &rule::QualificationEvidenceRequirement) -> Self {
        match value {
            rule::QualificationEvidenceRequirement::ActualRuntimeProvenance {
                source_owner,
                fact_predicates,
            } => Self::ActualRuntimeProvenance {
                source_owner: EvidenceSourceOwnerWire::from_operations(*source_owner),
                fact_predicates: fact_predicates
                    .iter()
                    .map(RuntimeFactPredicateWire::from_operations)
                    .collect(),
            },
            rule::QualificationEvidenceRequirement::IndependentlyAcceptedWork { source_owner } => {
                Self::IndependentlyAcceptedWork {
                    source_owner: EvidenceSourceOwnerWire::from_operations(*source_owner),
                }
            }
        }
    }
}

impl EvidenceSourceOwnerWire {
    fn operations_owner(&self) -> Result<rule::EvidenceSourceOwnerId, OperationsError> {
        rule::EvidenceSourceOwnerId::new(self.namespace.into_operations(), self.id)
    }

    fn from_operations(value: rule::EvidenceSourceOwnerId) -> Self {
        Self {
            namespace: EvidenceOwnerNamespaceWire::from_operations(value.namespace()),
            id: value.id(),
        }
    }
}

impl EvidenceOwnerNamespaceWire {
    const fn into_operations(self) -> rule::EvidenceOwnerNamespace {
        match self {
            Self::Forge => rule::EvidenceOwnerNamespace::Forge,
            Self::RuntimeAdapter => rule::EvidenceOwnerNamespace::RuntimeAdapter,
        }
    }

    const fn from_operations(value: rule::EvidenceOwnerNamespace) -> Self {
        match value {
            rule::EvidenceOwnerNamespace::Forge => Self::Forge,
            rule::EvidenceOwnerNamespace::RuntimeAdapter => Self::RuntimeAdapter,
        }
    }
}

impl RuntimeFactPredicateWire {
    fn operations_predicate(&self) -> Result<rule::RuntimeFactPredicate, OperationsError> {
        match self {
            Self::Text {
                fact,
                allowed_values,
            } => {
                if allowed_values.is_empty()
                    || allowed_values.len() > rule::MAX_RUNTIME_FACT_ALLOWLIST_VALUES
                {
                    return Err(OperationsError::Bounds(
                        "qualification runtime text allow-list",
                    ));
                }
                let mut checked_values = BTreeSet::new();
                for value in allowed_values {
                    if !checked_values.insert(rule::ExactRuntimeText::new(value.clone())?) {
                        return Err(OperationsError::Bounds(
                            "duplicate qualification runtime text allow-list value",
                        ));
                    }
                }
                rule::RuntimeFactPredicate::exact_text((*fact).into_operations(), checked_values)
            }
            Self::Digest {
                fact,
                allowed_values,
            } => {
                if allowed_values.is_empty()
                    || allowed_values.len() > rule::MAX_RUNTIME_FACT_ALLOWLIST_VALUES
                {
                    return Err(OperationsError::Bounds(
                        "qualification runtime digest allow-list",
                    ));
                }
                let mut checked_values = BTreeSet::new();
                for value in allowed_values {
                    if !checked_values.insert(value.operations_digest()?) {
                        return Err(OperationsError::Bounds(
                            "duplicate qualification runtime digest allow-list value",
                        ));
                    }
                }
                rule::RuntimeFactPredicate::exact_digests((*fact).into_operations(), checked_values)
            }
        }
    }

    fn from_operations(value: &rule::RuntimeFactPredicate) -> Self {
        match value {
            rule::RuntimeFactPredicate::Text {
                fact,
                allowed_values,
            } => Self::Text {
                fact: ActualRuntimeFactWire::from_operations(*fact),
                allowed_values: allowed_values
                    .iter()
                    .map(|value| value.as_str().to_owned())
                    .collect(),
            },
            rule::RuntimeFactPredicate::Digest {
                fact,
                allowed_values,
            } => Self::Digest {
                fact: ActualRuntimeFactWire::from_operations(*fact),
                allowed_values: allowed_values
                    .iter()
                    .map(RuntimeFactDigestWire::from_operations)
                    .collect(),
            },
        }
    }
}

impl RuntimeFactDigestWire {
    fn operations_digest(&self) -> Result<rule::RuntimeFactDigest, OperationsError> {
        rule::RuntimeFactDigest::new(
            rule::ExactRuntimeText::new(self.algorithm.clone())?,
            self.bytes.clone(),
        )
    }

    fn from_operations(value: &rule::RuntimeFactDigest) -> Self {
        Self {
            algorithm: value.algorithm().as_str().to_owned(),
            bytes: value.bytes().to_vec(),
        }
    }
}

impl ActualRuntimeFactWire {
    const fn into_operations(self) -> rule::ActualRuntimeFact {
        match self {
            Self::Provider => rule::ActualRuntimeFact::Provider,
            Self::Engine => rule::ActualRuntimeFact::Engine,
            Self::Model => rule::ActualRuntimeFact::Model,
            Self::ToolVersions => rule::ActualRuntimeFact::ToolVersions,
            Self::EnvironmentFingerprint => rule::ActualRuntimeFact::EnvironmentFingerprint,
        }
    }

    const fn from_operations(value: rule::ActualRuntimeFact) -> Self {
        match value {
            rule::ActualRuntimeFact::Provider => Self::Provider,
            rule::ActualRuntimeFact::Engine => Self::Engine,
            rule::ActualRuntimeFact::Model => Self::Model,
            rule::ActualRuntimeFact::ToolVersions => Self::ToolVersions,
            rule::ActualRuntimeFact::EnvironmentFingerprint => Self::EnvironmentFingerprint,
        }
    }
}

/// Current rule state as stored in an immutable generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReviewerQualificationRuleStateWire {
    ActiveForFuturePools,
    DeactivatedForFuturePools,
}

/// Immutable, scoped policy generation and its original authoring receipt.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ReviewerQualificationRuleGenerationWire {
    pub workspace_id: Uuid,
    pub rule_id: Uuid,
    pub generation: u64,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub expected_current_generation: Option<u64>,
    pub request_id: Uuid,
    pub original_direct_token_id: Uuid,
    /// Session verified against the original authenticated `DirectToken`.
    pub original_session_id: Uuid,
    pub state: ReviewerQualificationRuleStateWire,
    pub policy: ReviewerQualificationRulePolicyWire,
    pub change: ReviewerQualificationRuleChangeWire,
    pub created_at_ms: i64,
}

impl ReviewerQualificationRuleGenerationWire {
    fn from_operations(
        value: &rule::ReviewerQualificationRuleGenerationRecord,
    ) -> Result<Self, heterarchy_orchestration_runtime::RuntimeError> {
        let original_direct_token_id = match value.original_actor() {
            forge_domain::AuthenticatedPrincipal::DirectToken(id) => id.into_uuid(),
            forge_domain::AuthenticatedPrincipal::TrustedStdio
            | forge_domain::AuthenticatedPrincipal::OAuthGrant(_) => {
                return Err(heterarchy_orchestration_runtime::RuntimeError::Conflict);
            }
        };
        Ok(Self {
            workspace_id: value.workspace_id().into_uuid(),
            rule_id: value.rule_id(),
            generation: value.generation().get(),
            expected_current_generation: value
                .expected_current_generation()
                .map(rule::ReviewerQualificationRuleGeneration::get),
            request_id: value.request_id(),
            original_direct_token_id,
            original_session_id: value.requester_session_id().into_uuid(),
            state: match value.state() {
                rule::ReviewerQualificationRuleState::ActiveForFuturePools => {
                    ReviewerQualificationRuleStateWire::ActiveForFuturePools
                }
                rule::ReviewerQualificationRuleState::DeactivatedForFuturePools => {
                    ReviewerQualificationRuleStateWire::DeactivatedForFuturePools
                }
            },
            policy: ReviewerQualificationRulePolicyWire::from_operations(value.policy()),
            change: ReviewerQualificationRuleChangeWire::from_operations(value.change()),
            created_at_ms: value.created_at_ms(),
        })
    }

    /// Require a readback generation to match the exact mutation identity and
    /// immutable original payload. This does not make missing evidence proof
    /// that an in-flight transaction cannot still commit.
    #[must_use]
    pub fn matches_mutation(&self, request: &ReviewerQualificationRuleMutationRequest) -> bool {
        let Ok(expected_change) = request.operations_change() else {
            return false;
        };
        let Ok(actual_change) = self.change.operations_change() else {
            return false;
        };
        self.workspace_id == request.scope.workspace_id
            && self.rule_id == request.rule_id
            && self.request_id == request.request_id
            && self.expected_current_generation == request.expected_current_generation
            && actual_change == expected_change
            && self.generation
                == request
                    .expected_current_generation
                    .map_or(1, |value| value.saturating_add(1))
            && self.original_direct_token_id != Uuid::nil()
            && self.original_session_id == request.session_id
            && self.created_at_ms >= 0
    }

    pub(crate) fn valid(&self) -> bool {
        let expected_generation = if self.generation == 1 {
            None
        } else {
            self.generation.checked_sub(1)
        };
        !self.workspace_id.is_nil()
            && !self.rule_id.is_nil()
            && valid_generation(self.generation)
            && self
                .expected_current_generation
                .is_none_or(valid_generation)
            && self.expected_current_generation == expected_generation
            && !self.request_id.is_nil()
            && !self.original_direct_token_id.is_nil()
            && !self.original_session_id.is_nil()
            && self.created_at_ms >= 0
            && self.policy.operations_policy().is_ok()
            && self.change.operations_change().is_ok()
            && match (&self.change, self.state) {
                (
                    ReviewerQualificationRuleChangeWire::Publish { policy },
                    ReviewerQualificationRuleStateWire::ActiveForFuturePools,
                ) => self.policy == *policy,
                (
                    ReviewerQualificationRuleChangeWire::DeactivateForFuturePools,
                    ReviewerQualificationRuleStateWire::DeactivatedForFuturePools,
                ) => true,
                _ => false,
            }
    }
}

impl ReviewerQualificationRuleChangeWire {
    fn from_operations(value: &rule::ReviewerQualificationRuleChange) -> Self {
        match value {
            rule::ReviewerQualificationRuleChange::Publish { policy } => Self::Publish {
                policy: ReviewerQualificationRulePolicyWire::from_operations(policy),
            },
            rule::ReviewerQualificationRuleChange::DeactivateForFuturePools => {
                Self::DeactivateForFuturePools
            }
        }
    }
}

/// Exact mutation outcome, including the stored generation and replay flag.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ReviewerQualificationRuleMutationResponse {
    pub scope: WorkspaceQueryScope,
    pub session_id: Uuid,
    pub rule_id: Uuid,
    pub request_id: Uuid,
    pub replayed: bool,
    pub generation: ReviewerQualificationRuleGenerationWire,
}

/// Current-generation query selector.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ReviewerQualificationRuleQueryRequest {
    pub scope: WorkspaceQueryScope,
    pub session_id: Uuid,
    pub rule_id: Uuid,
}

/// Current-generation readback. `current` is explicitly null when no rule exists.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ReviewerQualificationRuleQueryResponse {
    pub scope: WorkspaceQueryScope,
    pub session_id: Uuid,
    pub rule_id: Uuid,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub current: Option<ReviewerQualificationRuleGenerationWire>,
}

/// Exact immutable mutation receipt selector.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ReviewerQualificationRuleRequestReadRequest {
    pub scope: WorkspaceQueryScope,
    pub session_id: Uuid,
    pub rule_id: Uuid,
    pub request_id: Uuid,
}

/// Read-only exact request result; missing remains an explicit absence.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ReviewerQualificationRuleRequestReadResponse {
    pub scope: WorkspaceQueryScope,
    pub session_id: Uuid,
    pub rule_id: Uuid,
    pub request_id: Uuid,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub generation: Option<ReviewerQualificationRuleGenerationWire>,
}

/// Bounded page selector for immutable rule history.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ReviewerQualificationRuleHistoryRequest {
    pub scope: WorkspaceQueryScope,
    pub session_id: Uuid,
    pub rule_id: Uuid,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub after_generation: Option<u64>,
    pub limit: u32,
}

/// Ascending immutable generation page with a revision cursor.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ReviewerQualificationRuleHistoryResponse {
    pub scope: WorkspaceQueryScope,
    pub session_id: Uuid,
    pub rule_id: Uuid,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub after_generation: Option<u64>,
    pub limit: u32,
    pub entries: Vec<ReviewerQualificationRuleGenerationWire>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub next_after_generation: Option<u64>,
    pub has_more: bool,
}

/// Project one validated durable Operations generation to transport fields.
pub(crate) fn generation_wire(
    value: &rule::ReviewerQualificationRuleGenerationRecord,
) -> Result<ReviewerQualificationRuleGenerationWire, heterarchy_orchestration_runtime::RuntimeError>
{
    ReviewerQualificationRuleGenerationWire::from_operations(value)
}

/// Project a typed Operations history page into a deterministic HTTP response.
pub(crate) fn history_response(
    scope: WorkspaceQueryScope,
    session_id: Uuid,
    rule_id: Uuid,
    request: &ReviewerQualificationRuleHistoryRequest,
    page: &rule::ReviewerQualificationRuleHistoryPage,
) -> Result<ReviewerQualificationRuleHistoryResponse, heterarchy_orchestration_runtime::RuntimeError>
{
    Ok(ReviewerQualificationRuleHistoryResponse {
        scope,
        session_id,
        rule_id,
        after_generation: request.after_generation,
        limit: request.limit,
        entries: page
            .entries()
            .iter()
            .map(generation_wire)
            .collect::<Result<Vec<_>, _>>()?,
        next_after_generation: page
            .next_after()
            .map(rule::ReviewerQualificationRuleGeneration::get),
        has_more: page.has_more(),
    })
}

/// Validate canonical transport selector fields before the source proof.
pub(crate) fn valid_scope(scope: &WorkspaceQueryScope, session_id: Uuid) -> bool {
    !scope.workspace_id.is_nil()
        && !scope.location_id.is_nil()
        && scope.location_revision > 0
        && scope.workspace_revision > 0
        && !scope.locator.is_empty()
        && scope.locator.len() <= 4096
        && !scope.locator.chars().any(char::is_control)
        && !session_id.is_nil()
}

pub(crate) const fn valid_generation(value: u64) -> bool {
    value > 0 && value <= i64::MAX as u64
}

fn deserialize_required_option<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, json};

    fn scope() -> WorkspaceQueryScope {
        WorkspaceQueryScope {
            workspace_id: Uuid::from_u128(1),
            location_id: Uuid::from_u128(2),
            location_revision: 3,
            workspace_revision: 4,
            locator: "hf1:qualification-test".to_owned(),
        }
    }

    fn policy() -> ReviewerQualificationRulePolicyWire {
        ReviewerQualificationRulePolicyWire {
            domain: "user-selected-domain".to_owned(),
            permitted_scope: "user-selected-scope".to_owned(),
            evidence_requirements: vec![
                QualificationEvidenceRequirementWire::ActualRuntimeProvenance {
                    source_owner: EvidenceSourceOwnerWire {
                        namespace: EvidenceOwnerNamespaceWire::RuntimeAdapter,
                        id: Uuid::from_u128(3),
                    },
                    fact_predicates: vec![
                        RuntimeFactPredicateWire::Text {
                            fact: ActualRuntimeFactWire::Model,
                            allowed_values: vec!["user-selected-model".to_owned()],
                        },
                        RuntimeFactPredicateWire::Text {
                            fact: ActualRuntimeFactWire::ToolVersions,
                            allowed_values: vec!["user-selected-tools".to_owned()],
                        },
                        RuntimeFactPredicateWire::Digest {
                            fact: ActualRuntimeFactWire::EnvironmentFingerprint,
                            allowed_values: vec![RuntimeFactDigestWire {
                                algorithm: "user-selected-algorithm".to_owned(),
                                bytes: vec![1, 2, 3],
                            }],
                        },
                    ],
                },
            ],
        }
    }

    fn mutation() -> ReviewerQualificationRuleMutationRequest {
        ReviewerQualificationRuleMutationRequest {
            scope: scope(),
            session_id: Uuid::from_u128(4),
            rule_id: Uuid::from_u128(5),
            request_id: Uuid::from_u128(6),
            expected_current_generation: None,
            change: ReviewerQualificationRuleChangeWire::Publish { policy: policy() },
        }
    }

    fn publish_policy_mut(
        request: &mut ReviewerQualificationRuleMutationRequest,
    ) -> Result<&mut ReviewerQualificationRulePolicyWire, std::io::Error> {
        match &mut request.change {
            ReviewerQualificationRuleChangeWire::Publish { policy } => Ok(policy),
            ReviewerQualificationRuleChangeWire::DeactivateForFuturePools => {
                Err(std::io::Error::other("expected publish fixture"))
            }
        }
    }

    fn runtime_fact_predicates_mut(
        policy: &mut ReviewerQualificationRulePolicyWire,
    ) -> Result<&mut Vec<RuntimeFactPredicateWire>, std::io::Error> {
        match policy.evidence_requirements.first_mut() {
            Some(QualificationEvidenceRequirementWire::ActualRuntimeProvenance {
                fact_predicates,
                ..
            }) => Ok(fact_predicates),
            _ => Err(std::io::Error::other("expected runtime provenance fixture")),
        }
    }

    #[test]
    fn mutation_requires_explicit_nullable_generation_and_rejects_extra_fields()
    -> Result<(), Box<dyn std::error::Error>> {
        let value = serde_json::to_value(mutation())?;
        assert!(
            serde_json::from_value::<ReviewerQualificationRuleMutationRequest>(value.clone())
                .is_ok()
        );

        let mut missing_generation = value.clone();
        missing_generation
            .as_object_mut()
            .ok_or("mutation object")?
            .remove("expectedCurrentGeneration");
        assert!(
            serde_json::from_value::<ReviewerQualificationRuleMutationRequest>(missing_generation)
                .is_err()
        );

        let mut extra = value;
        extra
            .as_object_mut()
            .ok_or("mutation object")?
            .insert("issuerId".to_owned(), json!(Uuid::from_u128(99)));
        assert!(serde_json::from_value::<ReviewerQualificationRuleMutationRequest>(extra).is_err());
        Ok(())
    }

    #[test]
    fn typed_predicates_validate_bounds_and_require_source_owned_runtime_facts()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut request = mutation();
        let policy = publish_policy_mut(&mut request)?;
        let fact_predicates = runtime_fact_predicates_mut(policy)?;
        let RuntimeFactPredicateWire::Text { allowed_values, .. } = &mut fact_predicates[0] else {
            return Err(std::io::Error::other("expected text predicate fixture").into());
        };
        allowed_values.resize(33, "bounded-fixture".to_owned());
        assert!(request.operations_change().is_err());

        let mut incomplete = mutation();
        let policy = publish_policy_mut(&mut incomplete)?;
        let fact_predicates = runtime_fact_predicates_mut(policy)?;
        fact_predicates.retain(|predicate| {
            !matches!(
                predicate,
                RuntimeFactPredicateWire::Digest {
                    fact: ActualRuntimeFactWire::EnvironmentFingerprint,
                    ..
                }
            )
        });
        assert!(incomplete.operations_change().is_err());
        Ok(())
    }

    #[test]
    fn wire_conversion_rejects_identical_duplicate_fact_predicates_before_set_conversion()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut request = mutation();
        let policy = publish_policy_mut(&mut request)?;
        let fact_predicates = runtime_fact_predicates_mut(policy)?;
        let duplicate = fact_predicates
            .first()
            .cloned()
            .ok_or_else(|| std::io::Error::other("missing predicate fixture"))?;
        fact_predicates.push(duplicate);

        assert!(request.operations_change().is_err());
        Ok(())
    }

    #[test]
    fn wire_conversion_rejects_duplicate_values_and_evidence_requirements()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut duplicate_text = mutation();
        let policy = publish_policy_mut(&mut duplicate_text)?;
        let fact_predicates = runtime_fact_predicates_mut(policy)?;
        let RuntimeFactPredicateWire::Text { allowed_values, .. } = &mut fact_predicates[0] else {
            return Err(std::io::Error::other("expected text predicate fixture").into());
        };
        let duplicate = allowed_values
            .first()
            .cloned()
            .ok_or_else(|| std::io::Error::other("missing text value fixture"))?;
        allowed_values.push(duplicate);
        assert!(duplicate_text.operations_change().is_err());

        let mut duplicate_digest = mutation();
        let policy = publish_policy_mut(&mut duplicate_digest)?;
        let fact_predicates = runtime_fact_predicates_mut(policy)?;
        let RuntimeFactPredicateWire::Digest { allowed_values, .. } = &mut fact_predicates[2]
        else {
            return Err(std::io::Error::other("expected digest predicate fixture").into());
        };
        let duplicate = allowed_values
            .first()
            .cloned()
            .ok_or_else(|| std::io::Error::other("missing digest value fixture"))?;
        allowed_values.push(duplicate);
        assert!(duplicate_digest.operations_change().is_err());

        let mut duplicate_requirement = mutation();
        let policy = publish_policy_mut(&mut duplicate_requirement)?;
        let duplicate = policy
            .evidence_requirements
            .first()
            .cloned()
            .ok_or_else(|| std::io::Error::other("missing evidence requirement fixture"))?;
        policy.evidence_requirements.push(duplicate);
        assert!(duplicate_requirement.operations_change().is_err());
        Ok(())
    }

    #[test]
    fn wire_conversion_bounds_raw_nested_vectors_before_collecting()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut too_many_requirements = mutation();
        let policy = publish_policy_mut(&mut too_many_requirements)?;
        let requirement = policy
            .evidence_requirements
            .first()
            .cloned()
            .ok_or_else(|| std::io::Error::other("missing evidence requirement fixture"))?;
        policy.evidence_requirements.resize(
            rule::MAX_QUALIFICATION_EVIDENCE_REQUIREMENTS + 1,
            requirement,
        );
        assert!(too_many_requirements.operations_change().is_err());

        let mut too_many_predicates = mutation();
        let policy = publish_policy_mut(&mut too_many_predicates)?;
        let predicates = runtime_fact_predicates_mut(policy)?;
        let predicate = predicates
            .first()
            .cloned()
            .ok_or_else(|| std::io::Error::other("missing fact predicate fixture"))?;
        predicates.resize(rule::MAX_RUNTIME_FACT_PREDICATES + 1, predicate);
        assert!(too_many_predicates.operations_change().is_err());

        let mut too_many_digests = mutation();
        let policy = publish_policy_mut(&mut too_many_digests)?;
        let predicates = runtime_fact_predicates_mut(policy)?;
        let RuntimeFactPredicateWire::Digest { allowed_values, .. } = &mut predicates[2] else {
            return Err(std::io::Error::other("expected digest predicate fixture").into());
        };
        let digest = allowed_values
            .first()
            .cloned()
            .ok_or_else(|| std::io::Error::other("missing digest fixture"))?;
        allowed_values.resize(rule::MAX_RUNTIME_FACT_ALLOWLIST_VALUES + 1, digest);
        assert!(too_many_digests.operations_change().is_err());
        Ok(())
    }

    #[test]
    fn request_history_distinguishes_explicit_null_and_bounds_page_size()
    -> Result<(), Box<dyn std::error::Error>> {
        let request = ReviewerQualificationRuleHistoryRequest {
            scope: scope(),
            session_id: Uuid::from_u128(4),
            rule_id: Uuid::from_u128(5),
            after_generation: None,
            limit: MAX_REVIEWER_QUALIFICATION_RULE_HISTORY_PAGE_SIZE,
        };
        let value = serde_json::to_value(request)?;
        assert!(
            serde_json::from_value::<ReviewerQualificationRuleHistoryRequest>(value.clone())
                .is_ok()
        );
        let mut missing_cursor: Value = value;
        missing_cursor
            .as_object_mut()
            .ok_or("history object")?
            .remove("afterGeneration");
        assert!(
            serde_json::from_value::<ReviewerQualificationRuleHistoryRequest>(missing_cursor)
                .is_err()
        );
        Ok(())
    }
}
