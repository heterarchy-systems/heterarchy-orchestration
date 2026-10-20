//! Scoped full projections and cancellation for durable provision operations.
//!
//! The Operations catalog remains the mutation owner. These methods bind an
//! operation to its canonical `AgentRequest` Workspace before reading or
//! cancelling it, so a globally unique operation ID cannot cross a scope.

use forge_domain::{AgentSessionId, AuthenticatedPrincipal, TimestampMillis, WorkspaceId};
use heterarchy_orchestration_operations::{
    AgentProvisionEffectKind, AgentProvisionFailureCode, AgentProvisionOperation,
    AgentProvisionProviderFamily, AgentProvisionUncertaintyRetirement,
    AgentProvisionUncertaintyRetirementCommand, AgentProvisionUncertaintyRetirementTarget,
    OperationsError, WorkspaceProjectReference,
};
use uuid::Uuid;

use super::provision_state_label;
use crate::{OrchestrationRuntime, RuntimeError};

/// Full transport-neutral projection of one durable provision operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProvisionOperationFullSummary {
    pub operation_id: Uuid,
    pub request_id: Uuid,
    pub slot_ordinal: u8,
    pub effect_kind: &'static str,
    pub provider_family: &'static str,
    pub profile_id: Option<Uuid>,
    pub runtime_session_id: Option<Uuid>,
    pub state: &'static str,
    pub provider_conversation_id: Option<String>,
    pub reported_failure_code: Option<&'static str>,
    pub prepared_at_ms: i64,
    pub updated_at_ms: i64,
    pub revision: u64,
    pub uncertainty_retirement: Option<AgentProvisionUncertaintyRetirement>,
    pub project: Option<WorkspaceProjectReference>,
}

/// Cancellation result retaining whether this call committed a state change.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProvisionOperationCancelSummary {
    pub operation: ProvisionOperationFullSummary,
    pub changed: bool,
}

/// Durable retirement result retaining the unchanged unknown effect truth.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProvisionOperationUncertaintyRetirementSummary {
    pub operation: ProvisionOperationFullSummary,
    pub retirement: AgentProvisionUncertaintyRetirement,
    pub changed: bool,
}

/// Authenticated, source-scoped request to stop recovery for one unknown
/// provider effect. Server time remains an explicit Runtime observation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProvisionOperationUncertaintyRetirement {
    pub principal: AuthenticatedPrincipal,
    pub workspace_id: Uuid,
    pub requester_session_id: Uuid,
    pub retirement_id: Uuid,
    pub operation_id: Uuid,
    pub expected_unknown_revision: u64,
    pub recovery_version: u8,
    pub retired_at_ms: i64,
}

impl OrchestrationRuntime {
    /// Read one operation only when its canonical `AgentRequest` belongs to the
    /// requested Workspace.
    ///
    /// # Errors
    /// Returns invalid identities, forbidden cross-Workspace selectors, or
    /// durable catalog failures.
    pub fn provision_operation(
        &self,
        workspace_id: Uuid,
        operation_id: Uuid,
    ) -> Result<Option<ProvisionOperationFullSummary>, RuntimeError> {
        validate_identity(workspace_id)?;
        validate_identity(operation_id)?;
        let catalog = self.catalog()?;
        let Some(operation) = catalog.agent_provision_operation(operation_id)? else {
            return Ok(None);
        };
        ensure_operation_workspace(&catalog, workspace_id, &operation)?;
        Ok(Some(full_summary(&operation)))
    }

    /// Cancel one prepared operation after binding its request Workspace in
    /// the same held Operations owner guard. Already-cancelled operations are
    /// returned unchanged regardless of the supplied expected revision.
    ///
    /// # Errors
    /// Returns invalid identities/timestamps, forbidden cross-Workspace
    /// selectors, illegal lifecycle transitions, stale fences, or catalog
    /// failures.
    pub fn cancel_provision_operation(
        &self,
        workspace_id: Uuid,
        operation_id: Uuid,
        expected_revision: u64,
        now_ms: i64,
    ) -> Result<ProvisionOperationCancelSummary, RuntimeError> {
        validate_identity(workspace_id)?;
        validate_identity(operation_id)?;
        if expected_revision == 0 {
            return Err(RuntimeError::InvalidInput);
        }
        let observed_at = TimestampMillis::new(now_ms).map_err(|_| RuntimeError::InvalidInput)?;
        let mut catalog = self.catalog()?;
        let operation = catalog
            .agent_provision_operation(operation_id)?
            .ok_or(RuntimeError::NotFound)?;
        ensure_operation_workspace(&catalog, workspace_id, &operation)?;
        let before_revision = operation.revision();
        let before_state = operation.state();
        let operation = catalog.cancel_agent_provision_operation(
            operation_id,
            expected_revision,
            observed_at,
        )?;
        let changed = operation.revision() != before_revision || operation.state() != before_state;
        Ok(ProvisionOperationCancelSummary {
            operation: full_summary(&operation),
            changed,
        })
    }

    /// Retire recovery for one unobserved `ProvisionNew` effect while keeping
    /// the provider outcome unknown. The exact caller-owned open Session and
    /// canonical operation Workspace are verified before the Operations owner
    /// receives the immutable command.
    ///
    /// # Errors
    /// Returns invalid identities/timestamps, foreign or closed Session
    /// authority, unsupported operation state/kind, stale fences, or durable
    /// catalog failures.
    pub fn retire_unobserved_provision_operation(
        &self,
        request: ProvisionOperationUncertaintyRetirement,
    ) -> Result<ProvisionOperationUncertaintyRetirementSummary, RuntimeError> {
        self.require_operator(request.principal)?;
        validate_identity(request.workspace_id)?;
        validate_identity(request.requester_session_id)?;
        validate_identity(request.retirement_id)?;
        validate_identity(request.operation_id)?;
        let retired_at =
            TimestampMillis::new(request.retired_at_ms).map_err(|_| RuntimeError::InvalidInput)?;
        self.require_open_session(request.requester_session_id, request.principal)?;

        let mut catalog = self.catalog()?;
        let operation = catalog
            .agent_provision_operation(request.operation_id)?
            .ok_or(RuntimeError::NotFound)?;
        ensure_operation_workspace(&catalog, request.workspace_id, &operation)?;
        let target = AgentProvisionUncertaintyRetirementTarget::new(
            request.operation_id,
            request.expected_unknown_revision,
            WorkspaceId::from_uuid(request.workspace_id),
            AgentSessionId::from_uuid(request.requester_session_id),
        )?;
        let command = AgentProvisionUncertaintyRetirementCommand::new(
            request.retirement_id,
            target,
            request.principal,
            request.recovery_version,
            retired_at,
        )?;
        let outcome = catalog.retire_agent_provision_uncertainty(command)?;
        let changed = outcome.created();
        let retirement = outcome.retirement().clone();
        let operation = catalog
            .agent_provision_operation(request.operation_id)?
            .ok_or(OperationsError::Corrupt(
                "retired Agent provision operation missing",
            ))?;
        ensure_operation_workspace(&catalog, request.workspace_id, &operation)?;
        Ok(ProvisionOperationUncertaintyRetirementSummary {
            operation: full_summary(&operation),
            retirement,
            changed,
        })
    }
}

fn validate_identity(value: Uuid) -> Result<(), RuntimeError> {
    if value.is_nil() {
        Err(RuntimeError::InvalidInput)
    } else {
        Ok(())
    }
}

fn ensure_operation_workspace(
    catalog: &heterarchy_orchestration_operations::OperationsCatalog,
    workspace_id: Uuid,
    operation: &AgentProvisionOperation,
) -> Result<(), RuntimeError> {
    let request = catalog
        .agent_request(operation.request_id())?
        .ok_or(OperationsError::Corrupt("Agent provision request missing"))?;
    if request.workspace_id().into_uuid() != workspace_id {
        return Err(RuntimeError::Forbidden);
    }
    Ok(())
}

pub(super) fn full_summary(operation: &AgentProvisionOperation) -> ProvisionOperationFullSummary {
    ProvisionOperationFullSummary {
        operation_id: operation.operation_id(),
        request_id: operation.request_id(),
        slot_ordinal: operation.slot_ordinal(),
        effect_kind: effect_kind_label(operation.effect_kind()),
        provider_family: provider_family_label(operation.provider_family()),
        profile_id: operation
            .profile_id()
            .map(heterarchy_orchestration_sessions::AgentProfileId::into_uuid),
        runtime_session_id: operation
            .runtime_session_id()
            .map(heterarchy_orchestration_sessions::AgentProfileRuntimeSessionId::into_uuid),
        state: provision_state_label(operation.state()),
        provider_conversation_id: operation.provider_conversation_id().map(str::to_owned),
        reported_failure_code: operation
            .reported_failure_code()
            .map(AgentProvisionFailureCode::as_str),
        prepared_at_ms: operation.prepared_at().get(),
        updated_at_ms: operation.updated_at().get(),
        revision: operation.revision(),
        uncertainty_retirement: operation.uncertainty_retirement().cloned(),
        project: operation.project(),
    }
}

const fn effect_kind_label(kind: AgentProvisionEffectKind) -> &'static str {
    match kind {
        AgentProvisionEffectKind::WakeExistingRuntime => "wake_existing_runtime",
        AgentProvisionEffectKind::WakeNewRuntime => "wake_new_runtime",
        AgentProvisionEffectKind::ProvisionNew => "provision_new",
        AgentProvisionEffectKind::ReuseReadyRuntime => "reuse_ready_runtime",
    }
}

const fn provider_family_label(family: AgentProvisionProviderFamily) -> &'static str {
    match family {
        AgentProvisionProviderFamily::ChatGpt => "chatgpt",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::access::OperatorPolicy;
    use crate::provisioning::{
        AgentRequestEffectScope, AgentRequestIndependence, AgentRequestSubmit,
    };
    use forge_domain::{DirectTokenId, OAuthGrantId};
    use heterarchy_orchestration_operations::{AgentProvisionDispatchAction, AgentProvisionState};

    #[test]
    fn scoped_operation_query_cancel_and_cancel_noop_use_canonical_owner()
    -> Result<(), Box<dyn std::error::Error>> {
        let root =
            std::env::temp_dir().join(format!("provision-operation-scope-{}", Uuid::now_v7()));
        std::fs::create_dir_all(&root)?;
        let runtime = OrchestrationRuntime::open(&root)?;
        let workspace_id = Uuid::from_u128(0xA101);
        crate::test_support::ensure_workspace_project_binding(&runtime, workspace_id, 1)?;
        let request_id = Uuid::from_u128(0xA105);
        runtime.submit_agent_request(AgentRequestSubmit {
            workspace_id,
            activity_id: Uuid::from_u128(0xA102),
            requester_agent_id: Uuid::from_u128(0xA103),
            requester_session_id: Uuid::from_u128(0xA104),
            coordinator_revision: 1,
            request_id,
            objective: "prepare one provider target".to_owned(),
            capability: "chat".to_owned(),
            count: 2,
            effect_scope: AgentRequestEffectScope::SourceWrite,
            independence: AgentRequestIndependence::None,
            reason: "operation scope test".to_owned(),
            created_at_ms: 1,
        })?;
        runtime.reserve_agent_request_resolution(request_id, 2, 3)?;
        let prepared = runtime.prepare_agent_request_effect_targets(request_id, 6, 7)?;
        assert_eq!(prepared.len(), 2);
        let operation = &prepared[0];
        let queried = runtime
            .provision_operation(workspace_id, operation.operation_id)?
            .ok_or("operation query")?;
        assert_eq!(queried.operation_id, operation.operation_id);
        assert!(matches!(
            runtime.provision_operation(Uuid::from_u128(0xA999), operation.operation_id),
            Err(RuntimeError::Forbidden)
        ));
        assert!(matches!(
            runtime.cancel_provision_operation(
                Uuid::from_u128(0xA999),
                operation.operation_id,
                operation.revision,
                8,
            ),
            Err(RuntimeError::Forbidden)
        ));
        let unchanged = runtime
            .provision_operation(workspace_id, operation.operation_id)?
            .ok_or("foreign cancel readback")?;
        assert_eq!(unchanged.revision, operation.revision);

        let cancelled = runtime.cancel_provision_operation(
            workspace_id,
            operation.operation_id,
            operation.revision,
            8,
        )?;
        assert!(cancelled.changed);
        assert_eq!(cancelled.operation.state, "cancelled");
        let noop =
            runtime.cancel_provision_operation(workspace_id, operation.operation_id, 999, 9)?;
        assert!(!noop.changed);
        assert_eq!(noop.operation.revision, cancelled.operation.revision);

        let released = {
            let mut catalog = runtime.catalog()?;
            catalog
                .next_agent_provision_dispatch(TimestampMillis::new(10)?)?
                .ok_or("pending dispatch")?
                .operation()
                .clone()
        };
        assert_eq!(released.state(), AgentProvisionState::EffectPending);
        assert!(matches!(
            runtime.cancel_provision_operation(
                workspace_id,
                released.operation_id(),
                released.revision(),
                11,
            ),
            Err(RuntimeError::Conflict)
        ));

        drop(runtime);
        std::fs::remove_dir_all(root)?;
        Ok(())
    }

    struct RetirementFixture {
        root: std::path::PathBuf,
        runtime: OrchestrationRuntime,
        owner: AuthenticatedPrincipal,
        other_operator: AuthenticatedPrincipal,
        session: Uuid,
        workspace_id: Uuid,
        unknown: AgentProvisionOperation,
    }

    impl RetirementFixture {
        fn new() -> Result<Self, Box<dyn std::error::Error>> {
            let root =
                std::env::temp_dir().join(format!("provision-retirement-{}", Uuid::now_v7()));
            std::fs::create_dir_all(&root)?;
            let owner = AuthenticatedPrincipal::DirectToken(DirectTokenId::from_uuid(
                Uuid::from_u128(0xB001),
            ));
            let other_operator = AuthenticatedPrincipal::OAuthGrant(OAuthGrantId::from_uuid(
                Uuid::from_u128(0xB002),
            ));
            let runtime = OrchestrationRuntime::open(&root)?
                .with_operator_policy(OperatorPolicy::new(vec![owner, other_operator])?);
            let session = runtime
                .open_session(Uuid::from_u128(0xB003), owner, 1)?
                .session
                .session_id;
            let workspace_id = Uuid::from_u128(0xB004);
            crate::test_support::ensure_workspace_project_binding(&runtime, workspace_id, 2)?;
            let request_id = Uuid::from_u128(0xB008);
            runtime.submit_agent_request(AgentRequestSubmit {
                workspace_id,
                activity_id: Uuid::from_u128(0xB005),
                requester_agent_id: Uuid::from_u128(0xB006),
                requester_session_id: session,
                coordinator_revision: 1,
                request_id,
                objective: "prepare two independent provider targets".to_owned(),
                capability: "chat".to_owned(),
                count: 2,
                effect_scope: AgentRequestEffectScope::SourceWrite,
                independence: AgentRequestIndependence::None,
                reason: "retire only the first unobserved effect".to_owned(),
                created_at_ms: 2,
            })?;
            runtime.reserve_agent_request_resolution(request_id, 3, 4)?;
            let prepared = runtime.prepare_agent_request_effect_targets(request_id, 5, 6)?;
            assert_eq!(prepared.len(), 2);
            let first = runtime
                .provider_worker_next_provision(owner, 7)?
                .ok_or("first provider dispatch")?;
            let first_fence = first.dispatch().ok_or("first Execute fence")?;
            assert_eq!(first_fence.action(), AgentProvisionDispatchAction::Execute);
            let unknown = runtime.provider_worker_mark_provision_unknown(
                owner,
                first_fence.operation_id(),
                first_fence.operation_revision(),
                8,
            )?;
            assert_eq!(unknown.state(), AgentProvisionState::EffectUnknown);
            Ok(Self {
                root,
                runtime,
                owner,
                other_operator,
                session,
                workspace_id,
                unknown,
            })
        }

        fn retirement(&self) -> ProvisionOperationUncertaintyRetirement {
            ProvisionOperationUncertaintyRetirement {
                principal: self.owner,
                workspace_id: self.workspace_id,
                requester_session_id: self.session,
                retirement_id: Uuid::from_u128(0xB009),
                operation_id: self.unknown.operation_id(),
                expected_unknown_revision: self.unknown.revision(),
                recovery_version: 1,
                retired_at_ms: 9,
            }
        }

        fn finish(self) -> Result<(), Box<dyn std::error::Error>> {
            let Self { root, runtime, .. } = self;
            drop(runtime);
            std::fs::remove_dir_all(root)?;
            Ok(())
        }
    }

    #[test]
    fn uncertainty_retirement_rejects_foreign_stale_and_invalid_authority()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = RetirementFixture::new()?;
        let retirement = fixture.retirement();
        assert!(matches!(
            fixture.runtime.retire_unobserved_provision_operation(
                ProvisionOperationUncertaintyRetirement {
                    principal: fixture.other_operator,
                    ..retirement
                }
            ),
            Err(RuntimeError::Forbidden)
        ));
        assert!(matches!(
            fixture.runtime.retire_unobserved_provision_operation(
                ProvisionOperationUncertaintyRetirement {
                    workspace_id: Uuid::from_u128(0xB999),
                    ..retirement
                }
            ),
            Err(RuntimeError::Forbidden)
        ));
        assert!(matches!(
            fixture.runtime.retire_unobserved_provision_operation(
                ProvisionOperationUncertaintyRetirement {
                    expected_unknown_revision: fixture.unknown.revision() + 1,
                    ..retirement
                }
            ),
            Err(RuntimeError::Conflict)
        ));
        assert!(matches!(
            fixture.runtime.retire_unobserved_provision_operation(
                ProvisionOperationUncertaintyRetirement {
                    recovery_version: 2,
                    ..retirement
                }
            ),
            Err(RuntimeError::Conflict)
        ));
        assert!(matches!(
            fixture.runtime.retire_unobserved_provision_operation(
                ProvisionOperationUncertaintyRetirement {
                    retired_at_ms: -1,
                    ..retirement
                }
            ),
            Err(RuntimeError::InvalidInput)
        ));
        fixture.finish()
    }

    #[test]
    fn uncertainty_retirement_replays_and_releases_next_candidate()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = RetirementFixture::new()?;
        let retirement = fixture.retirement();
        let retired = fixture
            .runtime
            .retire_unobserved_provision_operation(retirement)?;
        assert!(retired.changed);
        assert_eq!(retired.operation.state, "effect_unknown");
        assert_eq!(retired.operation.revision, fixture.unknown.revision() + 1);
        assert_eq!(
            retired.operation.uncertainty_retirement.as_ref(),
            Some(&retired.retirement)
        );
        let replay = fixture.runtime.retire_unobserved_provision_operation(
            ProvisionOperationUncertaintyRetirement {
                retired_at_ms: 100,
                ..retirement
            },
        )?;
        assert!(!replay.changed);
        assert_eq!(replay.retirement, retired.retirement);
        assert_eq!(replay.operation, retired.operation);
        assert!(matches!(
            fixture.runtime.provider_worker_confirm_provision(
                fixture.owner,
                fixture.unknown.operation_id(),
                retired.operation.revision,
                "late-conversation",
                10,
            ),
            Err(RuntimeError::Conflict)
        ));
        assert!(matches!(
            fixture.runtime.provider_worker_fail_provision(
                fixture.owner,
                fixture.unknown.operation_id(),
                retired.operation.revision,
                10,
                None,
            ),
            Err(RuntimeError::Conflict)
        ));
        let next = fixture
            .runtime
            .provider_worker_next_provision(fixture.owner, 11)?
            .ok_or("second provider dispatch")?;
        let next_fence = next.dispatch().ok_or("second Execute fence")?;
        assert_eq!(next_fence.action(), AgentProvisionDispatchAction::Execute);
        assert_ne!(next_fence.operation_id(), fixture.unknown.operation_id());
        fixture
            .runtime
            .close_session(fixture.session, fixture.owner, 1, 12)?;
        assert!(matches!(
            fixture.runtime.retire_unobserved_provision_operation(
                ProvisionOperationUncertaintyRetirement {
                    retired_at_ms: 13,
                    ..retirement
                }
            ),
            Err(RuntimeError::Forbidden)
        ));
        fixture.finish()
    }
}
