//! Workspace-scoped, append-only Reviewer qualification rule policy data.
//!
//! This module stores user-authored policy generations only. It does not
//! evaluate eligibility, accept Reviewer roles, grant case access, freeze a
//! pool, score work, or decide a case. A generation is a candidate for future
//! pool freezes; it cannot rewrite a frozen pool or case. Safety revocation of
//! source evidence or access belongs to a separate sourced D1 event.
//! Runtime predicates are bounded exact allow-lists chosen by the user. A
//! future evaluator must use one source-owned compatible provenance bundle
//! bound to the same logical Agent and capability scope; unsupported or
//! mismatched evidence remains Pending.
//!
//! Future case/pool creation must read and freeze the selected rule generation
//! inside the same owner transaction that creates that case/pool. The rule's
//! creation timestamp is audit metadata only; wall-clock order cannot establish
//! publication order if the clock regresses.
//!
//! The public in-process Operations API accepts only the
//! `AuthenticatedPrincipal::DirectToken` identity shape and records its ID.
//! That value alone does not authorize policy publication. Before invoking
//! this API, the trusted Runtime/Backend caller must authenticate the current
//! Session, enforce host `OperatorPolicy`, validate current Workspace/source
//! scope, and hold the current Broker-owner fence. This is not an HTTP grant.
//! Never deserialize the issuer from request data. Historical reads return the
//! original issuer without changing it.

mod schema;

use crate::{OperationsCatalog, OperationsError};
use forge_domain::{AgentSessionId, AuthenticatedPrincipal, DirectTokenId, WorkspaceId};
use rusqlite::{Connection, OptionalExtension, Row, Transaction, TransactionBehavior, params};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeSet,
    time::{SystemTime, UNIX_EPOCH},
};
use uuid::Uuid;

const MAX_POLICY_LABEL_BYTES: usize = 128;
/// Maximum number of typed evidence requirements in one persisted rule.
/// This is a bounded data-shape limit, not a qualification threshold.
pub const MAX_QUALIFICATION_EVIDENCE_REQUIREMENTS: usize = 16;
const MAX_HISTORY_PAGE_SIZE: u32 = 64;
const MAX_POLICY_JSON_BYTES: usize = 16_384;
const MAX_RUNTIME_TEXT_BYTES: usize = 512;
const MAX_RUNTIME_DIGEST_BYTES: usize = 128;
/// Maximum user-selected exact values in one runtime-fact allow-list.
/// This is a bounded data-shape limit, not a qualification threshold.
pub const MAX_RUNTIME_FACT_ALLOWLIST_VALUES: usize = 32;

/// Schema objects used by the Operations catalog's additive migration.
pub(crate) const fn schema_objects() -> &'static [(&'static str, &'static str, &'static str)] {
    schema::schema_objects()
}

/// Create the rule tables inside the catalog migration transaction.
pub(crate) fn create_schema(transaction: &Transaction<'_>) -> Result<(), OperationsError> {
    schema::create_schema(transaction)
}

/// A checked domain/capability name supplied by the user-authored rule.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct QualificationDomain(String);

impl QualificationDomain {
    /// Construct a bounded, non-empty domain identifier.
    ///
    /// # Errors
    /// Returns an Operations bounds error for empty, untrimmed, control-bearing
    /// or oversized identifiers.
    pub fn new(value: String) -> Result<Self, OperationsError> {
        validate_policy_label(&value, "qualification domain")?;
        Ok(Self(value))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    fn validate(&self) -> Result<(), OperationsError> {
        validate_policy_label(&self.0, "qualification domain")
    }
}

/// A checked, user-selected permitted review scope within one Workspace.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct QualificationScope(String);

impl QualificationScope {
    /// Construct a bounded, non-empty scope identifier.
    ///
    /// # Errors
    /// Returns an Operations bounds error for empty, untrimmed, control-bearing
    /// or oversized identifiers.
    pub fn new(value: String) -> Result<Self, OperationsError> {
        validate_policy_label(&value, "qualification scope")?;
        Ok(Self(value))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    fn validate(&self) -> Result<(), OperationsError> {
        validate_policy_label(&self.0, "qualification scope")
    }
}

/// Closed owner namespaces used by the accepted evidence classes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceOwnerNamespace {
    Forge,
    RuntimeAdapter,
}

/// Namespaced identity of a source owner named by the user-authored policy.
///
/// Storing this identity does not authenticate it. A future evaluator must
/// verify both namespace and owner ID in the issuer's own namespace.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct EvidenceSourceOwnerId {
    namespace: EvidenceOwnerNamespace,
    id: Uuid,
}

impl EvidenceSourceOwnerId {
    /// Construct a non-nil namespaced source-owner identity.
    ///
    /// # Errors
    /// Returns an Operations bounds error for the nil owner UUID.
    pub fn new(namespace: EvidenceOwnerNamespace, id: Uuid) -> Result<Self, OperationsError> {
        if id.is_nil() {
            return Err(OperationsError::Bounds("qualification evidence owner"));
        }
        Ok(Self { namespace, id })
    }

    #[must_use]
    pub const fn namespace(self) -> EvidenceOwnerNamespace {
        self.namespace
    }

    #[must_use]
    pub const fn id(self) -> Uuid {
        self.id
    }

    fn validate(self) -> Result<(), OperationsError> {
        if self.id.is_nil() {
            return Err(OperationsError::Bounds("qualification evidence owner"));
        }
        Ok(())
    }
}

/// Operator identity accepted by this internal authoring surface.
///
/// The type can represent only a `DirectToken` principal ID. It deliberately
/// does not retain token bytes or accept OAuth, stdio, participant, or role
/// labels. Its constructor is an internal trust boundary: callers must derive
/// the value from an authenticated `AuthenticatedPrincipal::DirectToken` and
/// authorize that principal through the host-configured `OperatorPolicy`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DirectTokenOperatorId(Uuid);

impl DirectTokenOperatorId {
    fn from_direct_token_id(value: DirectTokenId) -> Result<Self, OperationsError> {
        let id = value.into_uuid();
        if id.is_nil() {
            return Err(OperationsError::Bounds("qualification rule operator"));
        }
        Ok(Self(id))
    }

    const fn into_uuid(self) -> Uuid {
        self.0
    }

    fn as_principal(self) -> AuthenticatedPrincipal {
        AuthenticatedPrincipal::DirectToken(DirectTokenId::from_uuid(self.0))
    }
}

/// Exact runtime facts a source owner must provide for a policy requirement.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActualRuntimeFact {
    Provider,
    Engine,
    Model,
    ToolVersions,
    EnvironmentFingerprint,
}

const ACTUAL_RUNTIME_FACTS: [ActualRuntimeFact; 5] = [
    ActualRuntimeFact::Provider,
    ActualRuntimeFact::Engine,
    ActualRuntimeFact::Model,
    ActualRuntimeFact::ToolVersions,
    ActualRuntimeFact::EnvironmentFingerprint,
];

/// Maximum distinct runtime-fact predicates defined by the closed fact enum.
/// This bounds wire vectors before Operations converts them into a set.
pub const MAX_RUNTIME_FACT_PREDICATES: usize = ACTUAL_RUNTIME_FACTS.len();

/// Bounded exact text selected by the user for a runtime-fact allow-list.
/// No case folding, trimming, version parsing, or other normalization occurs.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ExactRuntimeText(String);

impl ExactRuntimeText {
    /// Construct a bounded exact UTF-8 value.
    ///
    /// # Errors
    /// Rejects empty, control-bearing, or oversized text.
    pub fn new(value: String) -> Result<Self, OperationsError> {
        validate_exact_runtime_text(&value)?;
        Ok(Self(value))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    fn validate(&self) -> Result<(), OperationsError> {
        validate_exact_runtime_text(&self.0)
    }
}

/// Opaque, algorithm-tagged digest selected by the user for exact comparison.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RuntimeFactDigest {
    algorithm: ExactRuntimeText,
    bytes: Vec<u8>,
}

impl RuntimeFactDigest {
    /// Construct an exact digest value without assuming a digest algorithm.
    ///
    /// # Errors
    /// Rejects an empty/invalid algorithm or empty/oversized digest bytes.
    pub fn new(algorithm: ExactRuntimeText, bytes: Vec<u8>) -> Result<Self, OperationsError> {
        let value = Self { algorithm, bytes };
        value.validate()?;
        Ok(value)
    }

    #[must_use]
    pub fn algorithm(&self) -> &ExactRuntimeText {
        &self.algorithm
    }

    #[must_use]
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    fn validate(&self) -> Result<(), OperationsError> {
        self.algorithm.validate()?;
        if self.bytes.is_empty() || self.bytes.len() > MAX_RUNTIME_DIGEST_BYTES {
            return Err(OperationsError::Bounds("qualification runtime digest"));
        }
        Ok(())
    }
}

/// One user-selected exact allow-list for a single runtime fact.
///
/// The closed variants make text and digest values non-interchangeable. A
/// future evaluator must leave a rule Pending if its source fact uses an
/// unsupported value kind or normalization.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(tag = "valueKind", rename_all = "snake_case", deny_unknown_fields)]
pub enum RuntimeFactPredicate {
    Text {
        fact: ActualRuntimeFact,
        allowed_values: BTreeSet<ExactRuntimeText>,
    },
    Digest {
        fact: ActualRuntimeFact,
        allowed_values: BTreeSet<RuntimeFactDigest>,
    },
}

impl RuntimeFactPredicate {
    /// Require exact match against user-selected text values.
    ///
    /// # Errors
    /// Rejects empty or excessive allow-lists and invalid exact values.
    pub fn exact_text(
        fact: ActualRuntimeFact,
        allowed_values: BTreeSet<ExactRuntimeText>,
    ) -> Result<Self, OperationsError> {
        let predicate = Self::Text {
            fact,
            allowed_values,
        };
        predicate.validate()?;
        Ok(predicate)
    }

    /// Require exact match against user-selected algorithm-tagged digests.
    ///
    /// # Errors
    /// Rejects empty or excessive allow-lists and invalid digest values.
    pub fn exact_digests(
        fact: ActualRuntimeFact,
        allowed_values: BTreeSet<RuntimeFactDigest>,
    ) -> Result<Self, OperationsError> {
        let predicate = Self::Digest {
            fact,
            allowed_values,
        };
        predicate.validate()?;
        Ok(predicate)
    }

    #[must_use]
    pub const fn fact(&self) -> ActualRuntimeFact {
        match self {
            Self::Text { fact, .. } | Self::Digest { fact, .. } => *fact,
        }
    }

    fn validate(&self) -> Result<(), OperationsError> {
        match self {
            Self::Text { allowed_values, .. } => {
                if allowed_values.is_empty()
                    || allowed_values.len() > MAX_RUNTIME_FACT_ALLOWLIST_VALUES
                {
                    return Err(OperationsError::Bounds(
                        "qualification runtime text allow-list",
                    ));
                }
                for value in allowed_values {
                    value.validate()?;
                }
            }
            Self::Digest { allowed_values, .. } => {
                if allowed_values.is_empty()
                    || allowed_values.len() > MAX_RUNTIME_FACT_ALLOWLIST_VALUES
                {
                    return Err(OperationsError::Bounds(
                        "qualification runtime digest allow-list",
                    ));
                }
                for value in allowed_values {
                    value.validate()?;
                }
            }
        }
        Ok(())
    }
}

/// Typed source evidence required by one qualification rule.
///
/// Independent work is opt-in: a policy includes that variant only when the
/// user selected independent quality acceptance as evidence. This data does
/// not itself attest that any receipt exists or is independent. An evaluator
/// must verify the exact source submission/version, independent acceptance
/// receipt and historical logical identity; `WorkCredit` or self-authored
/// evidence alone cannot satisfy it.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum QualificationEvidenceRequirement {
    /// Exact runtime predicates read from one owner-namespaced source bundle.
    /// A future evaluator must bind the bundle to one candidate Agent, scope,
    /// and compatible runtime/source generation.
    ActualRuntimeProvenance {
        source_owner: EvidenceSourceOwnerId,
        fact_predicates: BTreeSet<RuntimeFactPredicate>,
    },
    /// User-selected independent quality-accepted work, separate from runtime
    /// facts and bound by a future evaluator to the same logical Agent/scope.
    IndependentlyAcceptedWork { source_owner: EvidenceSourceOwnerId },
}

/// User-authored rule policy. It is not a qualification result.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ReviewerQualificationRulePolicy {
    domain: QualificationDomain,
    permitted_scope: QualificationScope,
    evidence_requirements: BTreeSet<QualificationEvidenceRequirement>,
}

impl ReviewerQualificationRulePolicy {
    /// Build policy data with explicit source-owned runtime predicates.
    ///
    /// Every policy must provide user-selected exact allow-lists for model,
    /// tool-version and environment-fingerprint facts from one runtime source
    /// owner. This data does not evaluate receipts or establish compatibility
    /// with a candidate Agent/run. No amount, score, pass flag, time-to-live,
    /// exam, or default threshold is implied.
    ///
    /// # Errors
    /// Rejects invalid labels, missing required runtime facts, nil source
    /// owners, empty requirements, or excessive requirement counts.
    pub fn new(
        domain: QualificationDomain,
        permitted_scope: QualificationScope,
        evidence_requirements: BTreeSet<QualificationEvidenceRequirement>,
    ) -> Result<Self, OperationsError> {
        let value = Self {
            domain,
            permitted_scope,
            evidence_requirements,
        };
        value.validate()?;
        Ok(value)
    }

    #[must_use]
    pub fn domain(&self) -> &QualificationDomain {
        &self.domain
    }

    #[must_use]
    pub fn permitted_scope(&self) -> &QualificationScope {
        &self.permitted_scope
    }

    #[must_use]
    pub fn evidence_requirements(&self) -> &BTreeSet<QualificationEvidenceRequirement> {
        &self.evidence_requirements
    }

    fn validate(&self) -> Result<(), OperationsError> {
        self.domain.validate()?;
        self.permitted_scope.validate()?;
        if self.evidence_requirements.is_empty()
            || self.evidence_requirements.len() > MAX_QUALIFICATION_EVIDENCE_REQUIREMENTS
        {
            return Err(OperationsError::Bounds(
                "qualification evidence requirements",
            ));
        }

        let mut actual_runtime_facts = BTreeSet::new();
        let mut actual_runtime_bundle_seen = false;
        for requirement in &self.evidence_requirements {
            match requirement {
                QualificationEvidenceRequirement::ActualRuntimeProvenance {
                    source_owner,
                    fact_predicates,
                } => {
                    if actual_runtime_bundle_seen {
                        return Err(OperationsError::Bounds(
                            "qualification runtime provenance bundle",
                        ));
                    }
                    actual_runtime_bundle_seen = true;
                    source_owner.validate()?;
                    if source_owner.namespace() != EvidenceOwnerNamespace::RuntimeAdapter {
                        return Err(OperationsError::Bounds("runtime evidence owner namespace"));
                    }
                    if fact_predicates.is_empty() {
                        return Err(OperationsError::Bounds(
                            "qualification runtime provenance facts",
                        ));
                    }
                    if fact_predicates.len() > MAX_RUNTIME_FACT_PREDICATES {
                        return Err(OperationsError::Bounds(
                            "qualification runtime fact predicates",
                        ));
                    }
                    for predicate in fact_predicates {
                        predicate.validate()?;
                        let fact = predicate.fact();
                        if !actual_runtime_facts.insert(fact) {
                            return Err(OperationsError::Bounds(
                                "duplicate runtime fact predicate",
                            ));
                        }
                    }
                }
                QualificationEvidenceRequirement::IndependentlyAcceptedWork { source_owner } => {
                    source_owner.validate()?;
                    if source_owner.namespace() != EvidenceOwnerNamespace::Forge {
                        return Err(OperationsError::Bounds(
                            "independent work evidence owner namespace",
                        ));
                    }
                }
            }
        }

        let required_actual_runtime_facts = BTreeSet::from([
            ActualRuntimeFact::Model,
            ActualRuntimeFact::ToolVersions,
            ActualRuntimeFact::EnvironmentFingerprint,
        ]);
        if !required_actual_runtime_facts.is_subset(&actual_runtime_facts) {
            return Err(OperationsError::Bounds(
                "qualification actual runtime provenance",
            ));
        }
        Ok(())
    }
}

/// One future-only rule mutation payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ReviewerQualificationRuleChange {
    Publish {
        policy: ReviewerQualificationRulePolicy,
    },
    /// Deactivation only affects future pool freezes. It is not a source
    /// evidence/access revocation and does not edit a frozen case.
    DeactivateForFuturePools,
}

impl ReviewerQualificationRuleChange {
    fn validate(&self) -> Result<(), OperationsError> {
        if let Self::Publish { policy } = self {
            policy.validate()?;
        }
        Ok(())
    }
}

/// Positive, SQLite-representable rule generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct ReviewerQualificationRuleGeneration(u64);

impl ReviewerQualificationRuleGeneration {
    /// Construct a positive generation no larger than SQLite's signed range.
    ///
    /// # Errors
    /// Returns an Operations bounds error for zero or an out-of-range value.
    pub fn new(value: u64) -> Result<Self, OperationsError> {
        if value == 0 || i64::try_from(value).is_err() {
            return Err(OperationsError::Bounds("qualification rule generation"));
        }
        Ok(Self(value))
    }

    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    fn to_sql(self) -> Result<i64, OperationsError> {
        i64::try_from(self.0).map_err(|_| OperationsError::Bounds("qualification rule generation"))
    }

    fn from_sql(value: i64) -> Result<Self, OperationsError> {
        let value = u64::try_from(value)
            .map_err(|_| OperationsError::Corrupt("qualification rule generation"))?;
        Self::new(value).map_err(|_| OperationsError::Corrupt("qualification rule generation"))
    }
}

/// Request to publish a rule generation or deactivate future use.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReviewerQualificationRuleMutationCommand {
    workspace_id: WorkspaceId,
    rule_id: Uuid,
    request_id: Uuid,
    expected_current_generation: Option<ReviewerQualificationRuleGeneration>,
    requester_session_id: AgentSessionId,
    actor: DirectTokenOperatorId,
    change: ReviewerQualificationRuleChange,
}

impl ReviewerQualificationRuleMutationCommand {
    /// Construct an exact Workspace/rule/request/actor mutation.
    ///
    /// `None` means the caller expects no prior rule generation. A present
    /// generation is an exact compare-and-set expectation.
    ///
    /// This checks only the principal kind. An `AuthenticatedPrincipal` value
    /// alone does not authorize policy publication. The trusted caller must
    /// first authenticate the current Session, enforce host `OperatorPolicy`,
    /// validate current Workspace/source scope, and hold the current
    /// Broker-owner fence. Do not deserialize `principal` from request data.
    /// This is an in-process Operations boundary, not an HTTP permission grant.
    ///
    /// # Errors
    /// Rejects non-DirectToken principals, nil identities, and invalid policy.
    pub fn new(
        workspace_id: WorkspaceId,
        rule_id: Uuid,
        request_id: Uuid,
        expected_current_generation: Option<ReviewerQualificationRuleGeneration>,
        requester_session_id: AgentSessionId,
        principal: AuthenticatedPrincipal,
        change: ReviewerQualificationRuleChange,
    ) -> Result<Self, OperationsError> {
        if workspace_id.into_uuid().is_nil()
            || rule_id.is_nil()
            || request_id.is_nil()
            || requester_session_id.into_uuid().is_nil()
        {
            return Err(OperationsError::Bounds("qualification rule identity"));
        }
        let actor = match principal {
            AuthenticatedPrincipal::DirectToken(id) => {
                DirectTokenOperatorId::from_direct_token_id(id)?
            }
            AuthenticatedPrincipal::TrustedStdio | AuthenticatedPrincipal::OAuthGrant(_) => {
                return Err(OperationsError::Conflict(
                    "qualification rule issuer must be DirectToken",
                ));
            }
        };
        change.validate()?;
        Ok(Self {
            workspace_id,
            rule_id,
            request_id,
            expected_current_generation,
            requester_session_id,
            actor,
            change,
        })
    }
}

/// Whether this immutable generation can be used by future pool freezes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReviewerQualificationRuleState {
    ActiveForFuturePools,
    DeactivatedForFuturePools,
}

impl ReviewerQualificationRuleState {
    const fn to_sql(self) -> i64 {
        match self {
            Self::ActiveForFuturePools => 0,
            Self::DeactivatedForFuturePools => 1,
        }
    }

    fn from_sql(value: i64) -> Result<Self, OperationsError> {
        match value {
            0 => Ok(Self::ActiveForFuturePools),
            1 => Ok(Self::DeactivatedForFuturePools),
            _ => Err(OperationsError::Corrupt("qualification rule state")),
        }
    }
}

/// Immutable persisted rule-generation and mutation receipt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReviewerQualificationRuleGenerationRecord {
    workspace_id: WorkspaceId,
    rule_id: Uuid,
    generation: ReviewerQualificationRuleGeneration,
    expected_current_generation: Option<ReviewerQualificationRuleGeneration>,
    request_id: Uuid,
    requester_session_id: AgentSessionId,
    original_actor: DirectTokenOperatorId,
    state: ReviewerQualificationRuleState,
    policy: ReviewerQualificationRulePolicy,
    change: ReviewerQualificationRuleChange,
    created_at_ms: i64,
}

impl ReviewerQualificationRuleGenerationRecord {
    #[must_use]
    pub const fn workspace_id(&self) -> WorkspaceId {
        self.workspace_id
    }

    #[must_use]
    pub const fn rule_id(&self) -> Uuid {
        self.rule_id
    }

    #[must_use]
    pub const fn generation(&self) -> ReviewerQualificationRuleGeneration {
        self.generation
    }

    #[must_use]
    pub const fn expected_current_generation(&self) -> Option<ReviewerQualificationRuleGeneration> {
        self.expected_current_generation
    }

    #[must_use]
    pub const fn request_id(&self) -> Uuid {
        self.request_id
    }

    /// Session that authenticated the original mutation request.
    pub const fn requester_session_id(&self) -> AgentSessionId {
        self.requester_session_id
    }

    pub fn original_actor(&self) -> AuthenticatedPrincipal {
        self.original_actor.as_principal()
    }

    #[must_use]
    pub const fn state(&self) -> ReviewerQualificationRuleState {
        self.state
    }

    #[must_use]
    pub fn policy(&self) -> &ReviewerQualificationRulePolicy {
        &self.policy
    }

    #[must_use]
    pub const fn change(&self) -> &ReviewerQualificationRuleChange {
        &self.change
    }

    #[must_use]
    pub const fn created_at_ms(&self) -> i64 {
        self.created_at_ms
    }
}

/// Result of a rule mutation; replay returns the original immutable receipt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReviewerQualificationRuleMutationReceipt {
    generation: ReviewerQualificationRuleGenerationRecord,
    replayed: bool,
}

impl ReviewerQualificationRuleMutationReceipt {
    #[must_use]
    pub const fn generation(&self) -> &ReviewerQualificationRuleGenerationRecord {
        &self.generation
    }

    #[must_use]
    pub const fn replayed(&self) -> bool {
        self.replayed
    }
}

/// Bounded ascending page of immutable rule generations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReviewerQualificationRuleHistoryPage {
    entries: Vec<ReviewerQualificationRuleGenerationRecord>,
    next_after: Option<ReviewerQualificationRuleGeneration>,
    has_more: bool,
}

impl ReviewerQualificationRuleHistoryPage {
    #[must_use]
    pub fn entries(&self) -> &[ReviewerQualificationRuleGenerationRecord] {
        &self.entries
    }

    #[must_use]
    pub const fn next_after(&self) -> Option<ReviewerQualificationRuleGeneration> {
        self.next_after
    }

    #[must_use]
    pub const fn has_more(&self) -> bool {
        self.has_more
    }
}

struct RuleMutationIntent {
    workspace_id: WorkspaceId,
    rule_id: Uuid,
    request_id: Uuid,
    expected_current_generation: Option<ReviewerQualificationRuleGeneration>,
    requester_session_id: AgentSessionId,
    actor: DirectTokenOperatorId,
    change: ReviewerQualificationRuleChange,
}

struct PreparedRuleGeneration {
    generation: ReviewerQualificationRuleGeneration,
    previous_generation: Option<ReviewerQualificationRuleGeneration>,
    expected_generation_sql: Option<i64>,
    policy: ReviewerQualificationRulePolicy,
    state: ReviewerQualificationRuleState,
}

impl ReviewerQualificationRuleMutationCommand {
    fn into_intent(self) -> RuleMutationIntent {
        RuleMutationIntent {
            workspace_id: self.workspace_id,
            rule_id: self.rule_id,
            request_id: self.request_id,
            expected_current_generation: self.expected_current_generation,
            requester_session_id: self.requester_session_id,
            actor: self.actor,
            change: self.change,
        }
    }
}

fn load_exact_rule_replay(
    transaction: &Transaction<'_>,
    intent: &RuleMutationIntent,
) -> Result<Option<ReviewerQualificationRuleGenerationRecord>, OperationsError> {
    let Some(existing) = load_rule_generation_by_request(transaction, intent.request_id)? else {
        return Ok(None);
    };
    let exact_replay = existing.workspace_id == intent.workspace_id
        && existing.rule_id == intent.rule_id
        && existing.expected_current_generation == intent.expected_current_generation
        && existing.requester_session_id == intent.requester_session_id
        && existing.original_actor == intent.actor
        && existing.change == intent.change;
    if exact_replay {
        Ok(Some(existing))
    } else {
        Err(OperationsError::Conflict(
            "qualification rule request replay",
        ))
    }
}

fn prepare_rule_generation(
    transaction: &Transaction<'_>,
    intent: &RuleMutationIntent,
) -> Result<PreparedRuleGeneration, OperationsError> {
    intent.change.validate()?;
    let current = load_current_rule_generation(transaction, intent.workspace_id, intent.rule_id)?;
    let previous_generation = current.as_ref().map(|record| record.generation);
    if previous_generation != intent.expected_current_generation {
        return Err(OperationsError::Conflict("qualification rule generation"));
    }

    let (policy, state) = match &intent.change {
        ReviewerQualificationRuleChange::Publish { policy } => {
            policy.validate()?;
            (
                policy.clone(),
                ReviewerQualificationRuleState::ActiveForFuturePools,
            )
        }
        ReviewerQualificationRuleChange::DeactivateForFuturePools => {
            let Some(current) = current.as_ref() else {
                return Err(OperationsError::Conflict("qualification rule deactivation"));
            };
            if current.state != ReviewerQualificationRuleState::ActiveForFuturePools {
                return Err(OperationsError::Conflict("qualification rule deactivation"));
            }
            (
                current.policy.clone(),
                ReviewerQualificationRuleState::DeactivatedForFuturePools,
            )
        }
    };

    let generation_value = match previous_generation {
        None => 1,
        Some(previous) => previous
            .get()
            .checked_add(1)
            .ok_or(OperationsError::Bounds("qualification rule generation"))?,
    };
    let generation = ReviewerQualificationRuleGeneration::new(generation_value)?;
    let expected_generation_sql = intent
        .expected_current_generation
        .map(ReviewerQualificationRuleGeneration::to_sql)
        .transpose()?;
    Ok(PreparedRuleGeneration {
        generation,
        previous_generation,
        expected_generation_sql,
        policy,
        state,
    })
}

fn persist_rule_generation(
    transaction: &Transaction<'_>,
    intent: &RuleMutationIntent,
    prepared: &PreparedRuleGeneration,
    created_at_ms: i64,
) -> Result<ReviewerQualificationRuleGenerationRecord, OperationsError> {
    let policy_json = serialize_json(&prepared.policy)?;
    let change_json = serialize_json(&intent.change)?;
    if policy_json.len() > MAX_POLICY_JSON_BYTES || change_json.len() > MAX_POLICY_JSON_BYTES {
        return Err(OperationsError::Bounds("qualification rule payload"));
    }
    let generation_sql = prepared.generation.to_sql()?;

    transaction.execute(
        "INSERT INTO ops_reviewer_qualification_rule_generations
         (workspace_id,rule_id,generation,expected_generation,request_id,
          requester_session_id,actor_direct_token_id,state,domain,permitted_scope,
          policy_json,change_json,created_at_ms)
         VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13)",
        params![
            intent.workspace_id.into_uuid().as_bytes(),
            intent.rule_id.as_bytes(),
            generation_sql,
            prepared.expected_generation_sql,
            intent.request_id.as_bytes(),
            intent.requester_session_id.into_uuid().as_bytes(),
            intent.actor.into_uuid().as_bytes(),
            prepared.state.to_sql(),
            prepared.policy.domain.as_str(),
            prepared.policy.permitted_scope.as_str(),
            policy_json,
            change_json,
            created_at_ms,
        ],
    )?;
    advance_rule_head(
        transaction,
        intent,
        prepared.previous_generation,
        generation_sql,
    )?;
    load_rule_generation_by_request(transaction, intent.request_id)?.ok_or(
        OperationsError::Corrupt("qualification rule mutation receipt"),
    )
}

fn advance_rule_head(
    transaction: &Transaction<'_>,
    intent: &RuleMutationIntent,
    previous_generation: Option<ReviewerQualificationRuleGeneration>,
    generation_sql: i64,
) -> Result<(), OperationsError> {
    let affected = match previous_generation {
        None => transaction.execute(
            "INSERT INTO ops_reviewer_qualification_rule_heads
             (workspace_id,rule_id,current_generation) VALUES(?1,?2,?3)",
            params![
                intent.workspace_id.into_uuid().as_bytes(),
                intent.rule_id.as_bytes(),
                generation_sql,
            ],
        )?,
        Some(previous) => transaction.execute(
            "UPDATE ops_reviewer_qualification_rule_heads
             SET current_generation=?3
             WHERE workspace_id=?1 AND rule_id=?2 AND current_generation=?4",
            params![
                intent.workspace_id.into_uuid().as_bytes(),
                intent.rule_id.as_bytes(),
                generation_sql,
                previous.to_sql()?,
            ],
        )?,
    };
    if affected == 1 {
        Ok(())
    } else {
        Err(OperationsError::Conflict("qualification rule generation"))
    }
}

impl OperationsCatalog {
    /// Publish or deactivate one rule generation under an IMMEDIATE owner
    /// transaction. The current verified Workspace import is checked inside
    /// that transaction before replay or a fresh write. Identical
    /// request/actor/payload replay is then checked before the generation CAS.
    /// A different actor must use exact readback; its identity never replaces
    /// the original issuer on the receipt.
    ///
    /// Creation time is sampled by this trusted persistence boundary after the
    /// transaction begins. It is never accepted from a request.
    ///
    /// # Errors
    /// Rejects invalid policy, request replay conflicts, stale generation CAS,
    /// storage errors, or an unconfirmed commit. After an unconfirmed commit,
    /// read by the same request ID before any retry.
    pub fn mutate_reviewer_qualification_rule(
        &mut self,
        command: ReviewerQualificationRuleMutationCommand,
    ) -> Result<ReviewerQualificationRuleMutationReceipt, OperationsError> {
        let intent = command.into_intent();
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;

        crate::teams::ensure_verified_workspace(&transaction, intent.workspace_id)?;
        if let Some(existing) = load_exact_rule_replay(&transaction, &intent)? {
            transaction.commit()?;
            return Ok(ReviewerQualificationRuleMutationReceipt {
                generation: existing,
                replayed: true,
            });
        }

        let prepared = prepare_rule_generation(&transaction, &intent)?;
        let generation_record =
            persist_rule_generation(&transaction, &intent, &prepared, trusted_now_ms()?)?;
        transaction
            .commit()
            .map_err(|source| OperationsError::OperationCommitUnconfirmed {
                workspace_id: intent.workspace_id,
                request_id: intent.request_id,
                source,
            })?;
        Ok(ReviewerQualificationRuleMutationReceipt {
            generation: generation_record,
            replayed: false,
        })
    }

    /// Read the current generation for an exact Workspace and rule identity.
    /// A deactivated generation remains readable and does not qualify anyone.
    ///
    /// # Errors
    /// Returns bounds, corruption, or storage errors.
    pub fn reviewer_qualification_rule(
        &self,
        workspace_id: WorkspaceId,
        rule_id: Uuid,
    ) -> Result<Option<ReviewerQualificationRuleGenerationRecord>, OperationsError> {
        validate_read_identity(workspace_id, rule_id)?;
        let transaction = self.connection.unchecked_transaction()?;
        let result = load_current_rule_generation(&transaction, workspace_id, rule_id)?;
        transaction.commit()?;
        Ok(result)
    }

    /// Read an immutable mutation receipt by exact Workspace, rule and request
    /// IDs after normal caller authorization. A new authorized operator
    /// receives the original `DirectToken` issuer identity and does not become
    /// the issuer by reading.
    ///
    /// # Errors
    /// Returns bounds, corruption, or storage errors.
    pub fn reviewer_qualification_rule_request(
        &self,
        workspace_id: WorkspaceId,
        rule_id: Uuid,
        request_id: Uuid,
    ) -> Result<Option<ReviewerQualificationRuleGenerationRecord>, OperationsError> {
        validate_read_identity(workspace_id, rule_id)?;
        if request_id.is_nil() {
            return Err(OperationsError::Bounds("qualification rule request"));
        }
        load_scoped_rule_generation_by_request(&self.connection, workspace_id, rule_id, request_id)
    }

    /// Read an ascending bounded page of immutable generations for one exact
    /// Workspace and rule identity.
    ///
    /// # Errors
    /// Rejects invalid identities, cursors, or page bounds and reports corrupt
    /// stored generations or storage failures.
    pub fn reviewer_qualification_rule_history(
        &self,
        workspace_id: WorkspaceId,
        rule_id: Uuid,
        after: Option<ReviewerQualificationRuleGeneration>,
        limit: u32,
    ) -> Result<ReviewerQualificationRuleHistoryPage, OperationsError> {
        validate_read_identity(workspace_id, rule_id)?;
        if limit == 0 || limit > MAX_HISTORY_PAGE_SIZE {
            return Err(OperationsError::Bounds("qualification rule history limit"));
        }
        let page_limit = usize::try_from(limit)
            .map_err(|_| OperationsError::Bounds("qualification rule history limit"))?;
        let after_sql = after.map_or(Ok(0), ReviewerQualificationRuleGeneration::to_sql)?;
        let query_limit = i64::from(limit) + 1;
        let transaction = self.connection.unchecked_transaction()?;
        let mut statement = transaction.prepare(
            "SELECT workspace_id,rule_id,generation,expected_generation,request_id,
                    requester_session_id,actor_direct_token_id,state,domain,permitted_scope,policy_json,
                    change_json,created_at_ms
             FROM ops_reviewer_qualification_rule_generations
             WHERE workspace_id=?1 AND rule_id=?2 AND generation>?3
             ORDER BY generation ASC LIMIT ?4",
        )?;
        let rows = statement.query_map(
            params![
                workspace_id.into_uuid().as_bytes(),
                rule_id.as_bytes(),
                after_sql,
                query_limit,
            ],
            read_raw_rule_generation,
        )?;
        let raw_records = rows.collect::<Result<Vec<_>, _>>()?;
        drop(statement);
        transaction.commit()?;

        let mut entries = raw_records
            .iter()
            .map(decode_rule_generation)
            .collect::<Result<Vec<_>, _>>()?;
        let has_more = entries.len() > page_limit;
        if has_more {
            entries.truncate(page_limit);
        }
        let next_after = has_more
            .then(|| entries.last().map(|entry| entry.generation))
            .flatten();
        Ok(ReviewerQualificationRuleHistoryPage {
            entries,
            next_after,
            has_more,
        })
    }
}

fn validate_read_identity(workspace_id: WorkspaceId, rule_id: Uuid) -> Result<(), OperationsError> {
    if workspace_id.into_uuid().is_nil() || rule_id.is_nil() {
        return Err(OperationsError::Bounds("qualification rule identity"));
    }
    Ok(())
}

fn validate_policy_label(value: &str, field: &'static str) -> Result<(), OperationsError> {
    if value.is_empty()
        || value.len() > MAX_POLICY_LABEL_BYTES
        || value.trim() != value
        || value.chars().any(char::is_control)
    {
        return Err(OperationsError::Bounds(field));
    }
    Ok(())
}

fn validate_exact_runtime_text(value: &str) -> Result<(), OperationsError> {
    if value.is_empty()
        || value.len() > MAX_RUNTIME_TEXT_BYTES
        || value.chars().any(char::is_control)
    {
        return Err(OperationsError::Bounds("qualification exact runtime text"));
    }
    Ok(())
}

fn trusted_now_ms() -> Result<i64, OperationsError> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| OperationsError::Bounds("qualification rule creation time"))?;
    i64::try_from(duration.as_millis())
        .map_err(|_| OperationsError::Bounds("qualification rule creation time"))
}

fn serialize_json<T: Serialize>(value: &T) -> Result<String, OperationsError> {
    serde_json::to_string(value)
        .map_err(|_| OperationsError::Corrupt("qualification rule serialization"))
}

fn deserialize_json<T: for<'de> Deserialize<'de>>(
    value: &str,
    field: &'static str,
) -> Result<T, OperationsError> {
    serde_json::from_str(value).map_err(|_| OperationsError::Corrupt(field))
}

fn load_current_rule_generation(
    connection: &Connection,
    workspace_id: WorkspaceId,
    rule_id: Uuid,
) -> Result<Option<ReviewerQualificationRuleGenerationRecord>, OperationsError> {
    let generation = connection
        .query_row(
            "SELECT current_generation FROM ops_reviewer_qualification_rule_heads
             WHERE workspace_id=?1 AND rule_id=?2",
            params![workspace_id.into_uuid().as_bytes(), rule_id.as_bytes()],
            |row| row.get::<_, i64>(0),
        )
        .optional()?;
    let Some(generation) = generation else {
        let has_history: bool = connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM ops_reviewer_qualification_rule_generations
             WHERE workspace_id=?1 AND rule_id=?2)",
            params![workspace_id.into_uuid().as_bytes(), rule_id.as_bytes()],
            |row| row.get(0),
        )?;
        if has_history {
            return Err(OperationsError::Corrupt("qualification rule head missing"));
        }
        return Ok(None);
    };
    let generation = ReviewerQualificationRuleGeneration::from_sql(generation)?;
    let raw = connection.query_row(
        "SELECT workspace_id,rule_id,generation,expected_generation,request_id,
                requester_session_id,actor_direct_token_id,state,domain,permitted_scope,policy_json,
                change_json,created_at_ms
         FROM ops_reviewer_qualification_rule_generations
         WHERE workspace_id=?1 AND rule_id=?2 AND generation=?3",
        params![
            workspace_id.into_uuid().as_bytes(),
            rule_id.as_bytes(),
            generation.to_sql()?,
        ],
        read_raw_rule_generation,
    )?;
    let record = decode_rule_generation(&raw)?;
    if record.workspace_id != workspace_id
        || record.rule_id != rule_id
        || record.generation != generation
    {
        return Err(OperationsError::Corrupt("qualification rule head identity"));
    }
    Ok(Some(record))
}

fn load_rule_generation_by_request(
    connection: &Connection,
    request_id: Uuid,
) -> Result<Option<ReviewerQualificationRuleGenerationRecord>, OperationsError> {
    connection
        .query_row(
            "SELECT workspace_id,rule_id,generation,expected_generation,request_id,
                    requester_session_id,actor_direct_token_id,state,domain,permitted_scope,policy_json,
                    change_json,created_at_ms
             FROM ops_reviewer_qualification_rule_generations WHERE request_id=?1",
            params![request_id.as_bytes()],
            read_raw_rule_generation,
        )
        .optional()?
        .map(|raw| decode_rule_generation(&raw))
        .transpose()
}

fn load_scoped_rule_generation_by_request(
    connection: &Connection,
    workspace_id: WorkspaceId,
    rule_id: Uuid,
    request_id: Uuid,
) -> Result<Option<ReviewerQualificationRuleGenerationRecord>, OperationsError> {
    connection
        .query_row(
            "SELECT workspace_id,rule_id,generation,expected_generation,request_id,
                    requester_session_id,actor_direct_token_id,state,domain,permitted_scope,policy_json,
                    change_json,created_at_ms
             FROM ops_reviewer_qualification_rule_generations
             WHERE workspace_id=?1 AND rule_id=?2 AND request_id=?3",
            params![
                workspace_id.into_uuid().as_bytes(),
                rule_id.as_bytes(),
                request_id.as_bytes(),
            ],
            read_raw_rule_generation,
        )
        .optional()?
        .map(|raw| decode_rule_generation(&raw))
        .transpose()
}

struct RawRuleGeneration {
    workspace_id: Vec<u8>,
    rule_id: Vec<u8>,
    generation: i64,
    expected_generation: Option<i64>,
    request_id: Vec<u8>,
    requester_session_id: Vec<u8>,
    actor_direct_token_id: Vec<u8>,
    state: i64,
    domain: String,
    permitted_scope: String,
    policy_json: String,
    change_json: String,
    created_at_ms: i64,
}

fn read_raw_rule_generation(row: &Row<'_>) -> rusqlite::Result<RawRuleGeneration> {
    Ok(RawRuleGeneration {
        workspace_id: row.get(0)?,
        rule_id: row.get(1)?,
        generation: row.get(2)?,
        expected_generation: row.get(3)?,
        request_id: row.get(4)?,
        requester_session_id: row.get(5)?,
        actor_direct_token_id: row.get(6)?,
        state: row.get(7)?,
        domain: row.get(8)?,
        permitted_scope: row.get(9)?,
        policy_json: row.get(10)?,
        change_json: row.get(11)?,
        created_at_ms: row.get(12)?,
    })
}

fn decode_rule_generation(
    raw: &RawRuleGeneration,
) -> Result<ReviewerQualificationRuleGenerationRecord, OperationsError> {
    if raw.created_at_ms < 0
        || raw.policy_json.len() > MAX_POLICY_JSON_BYTES
        || raw.change_json.len() > MAX_POLICY_JSON_BYTES
    {
        return Err(OperationsError::Corrupt(
            "qualification rule receipt bounds",
        ));
    }
    let workspace_id = WorkspaceId::from_uuid(decode_uuid(
        "qualification rule Workspace",
        &raw.workspace_id,
    )?);
    let rule_id = decode_uuid("qualification rule identity", &raw.rule_id)?;
    let generation = ReviewerQualificationRuleGeneration::from_sql(raw.generation)?;
    let expected_current_generation = raw
        .expected_generation
        .map(ReviewerQualificationRuleGeneration::from_sql)
        .transpose()?;
    let request_id = decode_uuid("qualification rule request", &raw.request_id)?;
    let requester_session_id = AgentSessionId::from_uuid(decode_uuid(
        "qualification rule requester Session",
        &raw.requester_session_id,
    )?);
    let original_actor =
        DirectTokenOperatorId::from_direct_token_id(DirectTokenId::from_uuid(decode_uuid(
            "qualification rule DirectToken actor",
            &raw.actor_direct_token_id,
        )?))
        .map_err(|_| OperationsError::Corrupt("qualification rule DirectToken actor"))?;
    let state = ReviewerQualificationRuleState::from_sql(raw.state)?;
    let policy: ReviewerQualificationRulePolicy =
        deserialize_json(&raw.policy_json, "qualification rule policy")?;
    policy
        .validate()
        .map_err(|_| OperationsError::Corrupt("qualification rule policy"))?;
    let change: ReviewerQualificationRuleChange =
        deserialize_json(&raw.change_json, "qualification rule change")?;
    change
        .validate()
        .map_err(|_| OperationsError::Corrupt("qualification rule change"))?;

    let generation_expected = if generation.get() == 1 {
        None
    } else {
        Some(ReviewerQualificationRuleGeneration::new(
            generation.get() - 1,
        )?)
    };
    if workspace_id.into_uuid().is_nil()
        || rule_id.is_nil()
        || request_id.is_nil()
        || expected_current_generation != generation_expected
        || raw.domain != policy.domain.as_str()
        || raw.permitted_scope != policy.permitted_scope.as_str()
        || !matches!(
            (&change, state),
            (
                ReviewerQualificationRuleChange::Publish { policy: published },
                ReviewerQualificationRuleState::ActiveForFuturePools
            ) if published == &policy
        ) && !matches!(
            (&change, state),
            (
                ReviewerQualificationRuleChange::DeactivateForFuturePools,
                ReviewerQualificationRuleState::DeactivatedForFuturePools
            )
        )
    {
        return Err(OperationsError::Corrupt("qualification rule receipt shape"));
    }

    Ok(ReviewerQualificationRuleGenerationRecord {
        workspace_id,
        rule_id,
        generation,
        expected_current_generation,
        request_id,
        requester_session_id,
        original_actor,
        state,
        policy,
        change,
        created_at_ms: raw.created_at_ms,
    })
}

fn decode_uuid(field: &'static str, bytes: &[u8]) -> Result<Uuid, OperationsError> {
    let id = Uuid::from_slice(bytes).map_err(|_| OperationsError::Corrupt(field))?;
    if id.is_nil() {
        return Err(OperationsError::Corrupt(field));
    }
    Ok(id)
}

#[cfg(test)]
mod tests;
