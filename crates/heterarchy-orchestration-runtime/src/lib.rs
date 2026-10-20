//! Protocol-neutral Orchestration host runtime.
//!
//! This crate owns process-local access to Orchestration durable catalogs. HTTP,
//! and other consumers must call this owner instead of opening a second
//! mutable catalog or rebuilding Orchestration domain decisions in transport
//! code.

use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use forge_domain::{TimestampMillis, WorkspaceId};
use heterarchy_orchestration_operations::{
    AdmissionIssuerConfig, AgentRequestRecord, AgentRequestState, ApprovalPolicyView,
    BrokerClusterConfig, BrokerOwnerAuthority, BrokerOwnerAuthorityError, BrokerOwnerObservation,
    BrokerOwnerRecoveryApproval, BrokerOwnerRecoveryError, ConfigureAdmissionIssuerCommand,
    OperationsCatalog, OperationsError, TeamRecord, TeamState,
    TrustedLegacyBindingRejectionDeployment,
};
use heterarchy_orchestration_sessions::{
    AgentProfile, AgentProfileCatalog, AgentProfileCreationOrigin, AgentProfileError,
    AgentProfileId, AgentProfileLifecycleState, AgentProfileProviderFamily, AgentProfileRevision,
    CreateAgentProfile,
};
use thiserror::Error;
use tokio::sync::{Mutex as AsyncMutex, OwnedSemaphorePermit, Semaphore};
use uuid::Uuid;

mod workspace_project;

#[cfg(test)]
pub(crate) mod test_support {
    use forge_domain::TimestampMillis;
    use heterarchy_orchestration_operations::{
        ChatGptProjectId, WorkspaceProjectConfigureCommand, WorkspaceProjectScope,
    };
    use uuid::Uuid;

    use super::{OrchestrationRuntime, RuntimeError};

    pub(crate) fn ensure_workspace_project_binding(
        runtime: &OrchestrationRuntime,
        workspace_id: Uuid,
        configured_at_ms: i64,
    ) -> Result<(), RuntimeError> {
        let mut catalog = runtime.catalog()?;
        if catalog.workspace_project_binding(workspace_id)?.is_none() {
            catalog.configure_workspace_project(
                WorkspaceProjectConfigureCommand::new(
                    Uuid::now_v7(),
                    WorkspaceProjectScope::new(
                        workspace_id,
                        Uuid::from_u128(0xFEEE),
                        1,
                        1,
                        "runtime-test-workspace".to_owned(),
                    )?,
                    None,
                    ChatGptProjectId::try_from("g-p-6ad231f7ad7c8191bfc1d5ff782c171f")?,
                    TimestampMillis::new(configured_at_ms)
                        .map_err(|_| RuntimeError::InvalidInput)?,
                )?,
                &[],
            )?;
        }
        Ok(())
    }
}

pub mod access;
pub mod approval;
pub mod chat;
pub mod coordination;
pub mod departure;
pub mod imports;
pub mod leadership;
pub mod member_list;
pub mod membership;
pub mod membership_source;
pub mod node_assignment_admission;
pub mod node_team;
pub mod participation;
pub mod profile_deletion;
pub mod profiles;
pub mod provider_chat;
pub mod provider_worker;
pub mod provisioning;
pub mod sessions;
pub mod teams;
pub mod work_credit;

const OPERATIONS_CATALOG_FILE_NAME: &str = "agent-operations.sqlite3";
const PROFILE_CATALOG_FILE_NAME: &str = "agent-profiles.sqlite3";
const DESTINATION_FILE_NAME: &str = "agent-operations-destination";
const BROKER_OWNER_FILE_NAME: &str = "broker-command-session.json";
const DESTINATION_BYTES: usize = 16;

/// A shared catalog capability that also retains the data-root owner lease.
/// Clones share the same catalog mutex; the owner pin adds no lock acquisition
/// and survives a transport/runtime dropping its own handle.
#[derive(Debug)]
pub struct CatalogHandle<T> {
    catalog: Arc<Mutex<T>>,
    owner_pin: Arc<Mutex<rusqlite::Connection>>,
}

impl<T> Clone for CatalogHandle<T> {
    fn clone(&self) -> Self {
        Self {
            catalog: Arc::clone(&self.catalog),
            owner_pin: Arc::clone(&self.owner_pin),
        }
    }
}

impl<T> CatalogHandle<T> {
    /// Enter the existing catalog mutex, retaining ordinary poison semantics.
    ///
    /// # Errors
    /// Returns the mutex poison error after a prior owner panic.
    pub fn lock(&self) -> std::sync::LockResult<std::sync::MutexGuard<'_, T>> {
        self.catalog.lock()
    }
}

/// Process-local owner of the durable Orchestration operations catalog.
///
/// The SQLite connection is intentionally singular. Callers may share the
/// runtime across transport handlers, while this type serializes access to the
/// single synchronous catalog without holding a lock across an async await.
pub struct OrchestrationRuntime {
    data_root: PathBuf,
    destination_id: Uuid,
    operations: Arc<Mutex<OperationsCatalog>>,
    profiles: Arc<Mutex<AgentProfileCatalog>>,
    broker_cluster: BrokerClusterConfig,
    broker_owner: AsyncMutex<BrokerOwnerAuthority>,
    governance_tick: Arc<Semaphore>,
    // Held from before catalog initialization until this runtime is dropped.
    // The mutex makes the Send-but-not-Sync SQLite connection shareable; it is
    // never used for catalog work.
    owner_lock: Arc<Mutex<rusqlite::Connection>>,
    catalog_bridge_slots: Arc<Semaphore>,
    operator_policy: access::OperatorPolicy,
    trusted_binding_rejection_legacy_deployments: Vec<TrustedLegacyBindingRejectionDeployment>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApprovalPolicySummary {
    pub workspace_id: Uuid,
    pub enabled: bool,
    pub revision: u64,
    pub auto_scope_count: usize,
    pub auto_scopes: Vec<ApprovalAutoScopeSummary>,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApprovalAutoScopeSummary {
    pub scope_id: Uuid,
    pub action: &'static str,
    pub permission_ceiling: approval::PermissionCeilingSummary,
    pub max_uses: Option<u16>,
    pub expires_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TeamSummary {
    pub team_id: Uuid,
    pub workspace_id: Uuid,
    pub name: String,
    pub purpose: String,
    pub state: &'static str,
    pub revision: u64,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
    pub formed_at_ms: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TeamPage {
    pub items: Vec<TeamSummary>,
    pub truncated: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentRequestSummary {
    pub request_id: Uuid,
    pub workspace_id: String,
    pub activity_id: String,
    pub objective: String,
    pub capability: String,
    pub requested_count: u8,
    pub reason: String,
    pub state: &'static str,
    pub created_at_ms: i64,
    pub revision: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentProfileCreateCommand {
    pub request_id: Uuid,
    pub stable_name: String,
    pub display_name: String,
    pub preferred_model: Option<String>,
    pub preferred_reasoning: Option<String>,
    pub base_capabilities: Vec<String>,
    pub created_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentProfileCreateSummary {
    pub profile: AgentProfileSummary,
    pub created: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentProfileSummary {
    pub profile_id: Uuid,
    pub request_id: Uuid,
    pub stable_name: String,
    pub display_name: String,
    pub provider_family: &'static str,
    pub preferred_model: Option<String>,
    pub preferred_reasoning: Option<String>,
    pub base_capabilities: Vec<String>,
    pub lifecycle_state: &'static str,
    pub creation_origin: &'static str,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
    pub last_used_at_ms: Option<i64>,
    pub revision: i64,
}

impl OrchestrationRuntime {
    /// Open the canonical Orchestration durable state rooted at the supplied directory.
    ///
    /// # Errors
    /// Rejects relative, missing, symlinked, corrupt or otherwise unavailable
    /// roots and catalog identities.
    pub fn open(data_root: &Path) -> Result<Self, RuntimeError> {
        Self::open_with_broker_cluster(data_root, BrokerClusterConfig::default())
    }

    /// Open the canonical Orchestration durable state with one explicit Broker
    /// topology.
    ///
    /// # Errors
    /// Rejects invalid storage roots, damaged catalogs, or an unavailable
    /// Broker owner authority.
    pub fn open_with_broker_cluster(
        data_root: &Path,
        broker_cluster: BrokerClusterConfig,
    ) -> Result<Self, RuntimeError> {
        validate_data_root(data_root)?;
        let owner_lock = acquire_owner_lock(data_root)?;
        let destination_id = read_or_create_destination(&data_root.join(DESTINATION_FILE_NAME))?;
        let operations = OperationsCatalog::open(
            &data_root.join(OPERATIONS_CATALOG_FILE_NAME),
            destination_id,
        )?;
        let profiles = AgentProfileCatalog::open(data_root.join(PROFILE_CATALOG_FILE_NAME))
            .map_err(RuntimeError::ProfileCatalog)?;
        let broker_owner = BrokerOwnerAuthority::new_with_cluster(
            data_root.join(BROKER_OWNER_FILE_NAME),
            destination_id,
            Uuid::now_v7(),
            broker_cluster,
        )
        .map_err(RuntimeError::BrokerOwner)?;
        Ok(Self {
            data_root: data_root.to_path_buf(),
            destination_id,
            operations: Arc::new(Mutex::new(operations)),
            profiles: Arc::new(Mutex::new(profiles)),
            broker_cluster,
            broker_owner: AsyncMutex::new(broker_owner),
            governance_tick: Arc::new(Semaphore::new(1)),
            owner_lock: Arc::new(Mutex::new(owner_lock)),
            catalog_bridge_slots: Arc::new(Semaphore::new(1)),
            operator_policy: access::OperatorPolicy::default(),
            trusted_binding_rejection_legacy_deployments: Vec::new(),
        })
    }

    #[must_use]
    pub fn with_trusted_binding_rejection_legacy_deployments(
        mut self,
        deployments: Vec<TrustedLegacyBindingRejectionDeployment>,
    ) -> Self {
        self.trusted_binding_rejection_legacy_deployments = deployments;
        self
    }

    #[must_use]
    pub fn data_root(&self) -> &Path {
        &self.data_root
    }

    /// Select immutable grants while the trusted host owns startup composition.
    #[must_use]
    pub fn with_operator_policy(mut self, policy: access::OperatorPolicy) -> Self {
        self.operator_policy = policy;
        self
    }

    /// Authorize an operator operation independently of bearer authentication.
    ///
    /// # Errors
    /// Returns forbidden unless trusted host configuration granted this identity.
    pub fn require_operator(
        &self,
        principal: forge_domain::AuthenticatedPrincipal,
    ) -> Result<(), RuntimeError> {
        self.operator_policy.authorize(principal)
    }

    #[must_use]
    pub const fn destination_id(&self) -> Uuid {
        self.destination_id
    }

    #[must_use]
    pub fn operations_catalog(&self) -> CatalogHandle<OperationsCatalog> {
        CatalogHandle {
            catalog: Arc::clone(&self.operations),
            owner_pin: Arc::clone(&self.owner_lock),
        }
    }

    #[must_use]
    pub fn profile_catalog(&self) -> CatalogHandle<AgentProfileCatalog> {
        CatalogHandle {
            catalog: Arc::clone(&self.profiles),
            owner_pin: Arc::clone(&self.owner_lock),
        }
    }

    pub const fn broker_cluster_config(&self) -> BrokerClusterConfig {
        self.broker_cluster
    }

    #[must_use]
    pub fn try_governance_tick(&self) -> Option<OwnedSemaphorePermit> {
        Arc::clone(&self.governance_tick).try_acquire_owned().ok()
    }

    /// Revalidate the process Broker command-session epoch before an
    /// authoritative Orchestration mutation.
    ///
    /// # Errors
    /// Returns the Broker authority error without weakening its fail-closed
    /// semantics.
    pub async fn ensure_broker_owner(&self) -> Result<(), BrokerOwnerAuthorityError> {
        self.broker_owner
            .lock()
            .await
            .ensure_current_owner()
            .await
            .map(|_| ())
    }

    /// Read the Broker-committed owner of this destination without changing any
    /// epoch or granting mutation authority. Safe for a stale process.
    ///
    /// # Errors
    /// Returns the fail-closed Broker authority classification.
    pub async fn broker_owner_status(
        &self,
    ) -> Result<BrokerOwnerObservation, BrokerOwnerAuthorityError> {
        self.broker_owner.lock().await.owner_status().await
    }

    /// Operator-approved recovery of a stale local Broker owner receipt. Never
    /// invoked automatically; the approval must name the live Broker owner.
    ///
    /// # Errors
    /// Returns a refusal or the fail-closed Broker authority classification.
    pub async fn recover_broker_owner(
        &self,
        approval: &BrokerOwnerRecoveryApproval,
    ) -> Result<(), BrokerOwnerRecoveryError> {
        self.broker_owner
            .lock()
            .await
            .recover_stale_owner(approval)
            .await
            .map(|_| ())
    }

    /// Persist or verify the Forge admission issuer configured for this
    /// Orchestration host.
    ///
    /// # Errors
    /// Returns catalog ownership, conflict, or durability failures.
    pub fn ensure_admission_issuer(
        &self,
        command: ConfigureAdmissionIssuerCommand,
    ) -> Result<AdmissionIssuerConfig, RuntimeError> {
        let mut catalog = self.catalog()?;
        catalog.ensure_admission_issuer(command).map_err(Into::into)
    }

    /// Open the existing operations catalog without creating or migrating it.
    ///
    /// # Errors
    /// Rejects invalid roots, missing/corrupt destination identity, and catalog
    /// schema/open failures.
    pub fn open_existing_operations_read_only(
        data_root: &Path,
    ) -> Result<OperationsCatalog, RuntimeError> {
        validate_data_root(data_root)?;
        let mut identity = open_existing_destination(&data_root.join(DESTINATION_FILE_NAME))?;
        let destination = read_destination(&mut identity)?;
        OperationsCatalog::open_existing_read_only(
            &data_root.join(OPERATIONS_CATALOG_FILE_NAME),
            destination,
        )
        .map_err(Into::into)
    }

    /// Prove that the runtime can still enter its catalog owner.
    ///
    /// # Errors
    /// Returns a poisoned-owner error after a previous panic while holding the
    /// catalog lock.
    pub fn probe(&self) -> Result<(), RuntimeError> {
        let guard = self
            .operations
            .lock()
            .map_err(|_| RuntimeError::CatalogOwnerPoisoned)?;
        drop(guard);
        Ok(())
    }

    /// Read one Workspace approval policy from Orchestration authority.
    ///
    /// # Errors
    /// Returns durable catalog failures or a poisoned-owner error.
    pub fn approval_policy_summary(
        &self,
        workspace_id: Uuid,
    ) -> Result<Option<ApprovalPolicySummary>, RuntimeError> {
        let catalog = self.catalog()?;
        Ok(catalog
            .approval_policy(WorkspaceId::from_uuid(workspace_id))?
            .as_ref()
            .map(approval_policy_summary))
    }

    /// Read a bounded Team prefix for one Workspace.
    ///
    /// # Errors
    /// Returns durable catalog failures, invalid limits, or a poisoned-owner
    /// error.
    pub fn teams(&self, workspace_id: Uuid, limit: u32) -> Result<TeamPage, RuntimeError> {
        let catalog = self.catalog()?;
        let page = catalog.teams(WorkspaceId::from_uuid(workspace_id), limit)?;
        let items = page.items().iter().map(team_summary).collect();
        Ok(TeamPage {
            items,
            truncated: page.truncated(),
        })
    }

    /// Read one durable `AgentRequest` without performing provider effects.
    ///
    /// # Errors
    /// Returns durable catalog failures or a poisoned-owner error.
    pub fn agent_request(
        &self,
        request_id: Uuid,
    ) -> Result<Option<AgentRequestSummary>, RuntimeError> {
        let catalog = self.catalog()?;
        Ok(catalog
            .agent_request(request_id)?
            .map(|record| agent_request_summary(&record)))
    }

    /// Read a bounded, newest-first page of the app-global Agent Profile
    /// catalog.
    ///
    /// # Errors
    /// Returns out-of-bounds limits, catalog ownership, or persistence
    /// failures.
    pub fn agent_profiles(&self, limit: u32) -> Result<Vec<AgentProfileSummary>, RuntimeError> {
        let catalog = self.profile_catalog_guard()?;
        catalog
            .list_profiles(usize::try_from(limit).map_err(|_| RuntimeError::InvalidInput)?)
            .map_err(profile_error_to_runtime)
            .map(|profiles| profiles.iter().map(agent_profile_summary).collect())
    }

    /// Resolve one app-global Agent Profile by stable identity.
    ///
    /// # Errors
    /// Returns invalid identity, catalog ownership, or persistence failures.
    pub fn agent_profile(
        &self,
        profile_id: Uuid,
    ) -> Result<Option<AgentProfileSummary>, RuntimeError> {
        let profile_id = AgentProfileId::from_uuid(profile_id).map_err(profile_error_to_runtime)?;
        let catalog = self.profile_catalog_guard()?;
        catalog
            .resolve(profile_id)
            .map(|profile| profile.as_ref().map(agent_profile_summary))
            .map_err(profile_error_to_runtime)
    }

    /// Create or replay one app-global reusable Agent Profile.
    ///
    /// Broker owner validation must be completed by the caller before this
    /// synchronous catalog mutation is admitted.
    ///
    /// # Errors
    /// Returns invalid input, replay conflict, catalog ownership, or persistence
    /// failures.
    pub fn create_agent_profile(
        &self,
        command: AgentProfileCreateCommand,
    ) -> Result<AgentProfileCreateSummary, RuntimeError> {
        let profile_id =
            AgentProfileId::from_uuid(Uuid::now_v7()).map_err(profile_error_to_runtime)?;
        let created_at = TimestampMillis::new(command.created_at_ms)
            .map_err(AgentProfileError::from)
            .map_err(profile_error_to_runtime)?;
        let create = CreateAgentProfile::new(
            profile_id,
            command.request_id,
            command.stable_name,
            command.display_name,
            AgentProfileProviderFamily::ChatGpt,
            command.preferred_model,
            command.preferred_reasoning,
            command.base_capabilities,
            AgentProfileCreationOrigin::UserManual,
            created_at,
        )
        .map_err(profile_error_to_runtime)?;
        let mut catalog = self.profile_catalog_guard()?;
        let outcome = catalog
            .create_profile(&create)
            .map_err(profile_error_to_runtime)?;
        Ok(AgentProfileCreateSummary {
            profile: agent_profile_summary(outcome.profile()),
            created: outcome.created(),
        })
    }

    /// Retire one Agent Profile from fresh admission.
    ///
    /// Broker owner validation must be completed by the caller before this
    /// synchronous catalog mutation is admitted.
    ///
    /// # Errors
    /// Returns missing Profile, revision conflict, live Runtime Session, invalid
    /// timestamp, or persistence failures.
    pub fn retire_agent_profile(
        &self,
        profile_id: Uuid,
        expected_revision: i64,
        observed_at_ms: i64,
    ) -> Result<AgentProfileSummary, RuntimeError> {
        let profile_id = AgentProfileId::from_uuid(profile_id).map_err(profile_error_to_runtime)?;
        let expected_revision =
            AgentProfileRevision::new(expected_revision).map_err(profile_error_to_runtime)?;
        let observed_at = TimestampMillis::new(observed_at_ms)
            .map_err(AgentProfileError::from)
            .map_err(profile_error_to_runtime)?;
        let mut catalog = self.profile_catalog_guard()?;
        catalog
            .retire_profile(profile_id, expected_revision, observed_at)
            .map(|profile| agent_profile_summary(&profile))
            .map_err(profile_error_to_runtime)
    }

    fn catalog(&self) -> Result<std::sync::MutexGuard<'_, OperationsCatalog>, RuntimeError> {
        self.operations
            .lock()
            .map_err(|_| RuntimeError::CatalogOwnerPoisoned)
    }

    /// Run one synchronous saga stage outside executor threads. Admission for
    /// the complete saga belongs to its caller; only one stage may enter the
    /// blocking pool at a time. The data-root owner survives a dropped waiter
    /// until an already-started stage completes.
    async fn run_catalog_stage<T, F>(&self, operation: F) -> Result<T, RuntimeError>
    where
        T: Send + 'static,
        F: FnOnce(&mut OperationsCatalog) -> Result<T, RuntimeError> + Send + 'static,
    {
        let permit = Arc::clone(&self.catalog_bridge_slots)
            .acquire_owned()
            .await
            .map_err(|_| {
                RuntimeError::persistence(OperationsError::Corrupt("catalog bridge closed"))
            })?;
        let catalog = Arc::clone(&self.operations);
        let owner = Arc::clone(&self.owner_lock);
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let _owner = owner;
            let mut catalog = catalog
                .lock()
                .map_err(|_| RuntimeError::CatalogOwnerPoisoned)?;
            operation(&mut catalog)
        })
        .await
        .map_err(|_| {
            RuntimeError::persistence(OperationsError::Corrupt("catalog stage join failed"))
        })?
    }

    fn profile_catalog_guard(
        &self,
    ) -> Result<std::sync::MutexGuard<'_, AgentProfileCatalog>, RuntimeError> {
        self.profiles
            .lock()
            .map_err(|_| RuntimeError::CatalogOwnerPoisoned)
    }
}

fn approval_policy_summary(policy: &ApprovalPolicyView) -> ApprovalPolicySummary {
    ApprovalPolicySummary {
        workspace_id: policy.workspace_id().into_uuid(),
        enabled: policy.enabled(),
        revision: policy.revision(),
        auto_scope_count: policy.auto_scopes().len(),
        auto_scopes: policy
            .auto_scopes()
            .iter()
            .map(|scope| ApprovalAutoScopeSummary {
                scope_id: scope.scope_id(),
                action: approval::action_label(scope.action()),
                permission_ceiling: approval::permission_ceiling(scope.permission_ceiling()),
                max_uses: scope.max_uses().get(),
                expires_at_ms: scope.expires_at_ms(),
            })
            .collect(),
        created_at_ms: policy.created_at_ms(),
        updated_at_ms: policy.updated_at_ms(),
    }
}

fn team_summary(team: &TeamRecord) -> TeamSummary {
    TeamSummary {
        team_id: team.team_id(),
        workspace_id: team.workspace_id().into_uuid(),
        name: team.name().to_owned(),
        purpose: team.purpose().to_owned(),
        state: match team.state() {
            TeamState::Active => "active",
            TeamState::Retired => "retired",
        },
        revision: team.revision(),
        created_at_ms: team.created_at().get(),
        updated_at_ms: team.updated_at().get(),
        formed_at_ms: team.formed_at().map(forge_domain::TimestampMillis::get),
    }
}

fn agent_request_summary(request: &AgentRequestRecord) -> AgentRequestSummary {
    AgentRequestSummary {
        request_id: request.request_id(),
        workspace_id: request.workspace_id().to_string(),
        activity_id: request.activity_id().to_string(),
        objective: request.objective().to_owned(),
        capability: request.capability().to_owned(),
        requested_count: request.count(),
        reason: request.reason().to_owned(),
        state: match request.state() {
            AgentRequestState::ResolutionPending => "resolution_pending",
            AgentRequestState::ResolutionReserved => "resolution_reserved",
            AgentRequestState::Fulfilled => "fulfilled",
        },
        created_at_ms: request.created_at().get(),
        revision: request.revision(),
    }
}

fn agent_profile_summary(profile: &AgentProfile) -> AgentProfileSummary {
    AgentProfileSummary {
        profile_id: profile.id().into_uuid(),
        request_id: profile.request_id(),
        stable_name: profile.stable_name().to_owned(),
        display_name: profile.display_name().to_owned(),
        provider_family: match profile.provider_family() {
            AgentProfileProviderFamily::ChatGpt => "chatgpt",
        },
        preferred_model: profile.preferred_model().map(str::to_owned),
        preferred_reasoning: profile.preferred_reasoning().map(str::to_owned),
        base_capabilities: profile.base_capabilities().to_vec(),
        lifecycle_state: match profile.lifecycle_state() {
            AgentProfileLifecycleState::Active => "active",
            AgentProfileLifecycleState::Disabled => "disabled",
            AgentProfileLifecycleState::Retired => "retired",
        },
        creation_origin: match profile.creation_origin() {
            AgentProfileCreationOrigin::UserManual => "user_manual",
            AgentProfileCreationOrigin::AgentRequest => "agent_request",
            AgentProfileCreationOrigin::Imported => "imported",
            AgentProfileCreationOrigin::SystemBootstrap => "system_bootstrap",
        },
        created_at_ms: profile.created_at().get(),
        updated_at_ms: profile.updated_at().get(),
        last_used_at_ms: profile.last_used_at().map(TimestampMillis::get),
        revision: profile.revision().get(),
    }
}

impl std::fmt::Debug for OrchestrationRuntime {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("OrchestrationRuntime")
            .field("data_root", &self.data_root)
            .field("destination_id", &self.destination_id)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Error)]
pub enum RuntimeError {
    /// Preserve conversation recovery and definite-admission error distinctions.
    #[error("conversation operation failed")]
    Chat(#[source] Box<chat::ChatError>),
    /// Trusted source authority could not be revalidated before local completion.
    #[error("source authority is unavailable")]
    SourceAuthorityUnavailable,

    #[error("Orchestration data root is invalid")]
    InvalidDataRoot,
    #[error("Orchestration destination identity is corrupt")]
    CorruptDestinationIdentity,
    #[error("Orchestration destination identity creation raced")]
    DestinationIdentityConflict,
    #[error("Orchestration catalog owner is poisoned")]
    CatalogOwnerPoisoned,
    #[error("Orchestration input violates a bounded contract")]
    InvalidInput,
    #[error("Orchestration operation is forbidden for this principal")]
    Forbidden,
    #[error("Orchestration logical session catalog failed")]
    SessionCatalog(#[source] heterarchy_orchestration_sessions::AgentSessionCatalogError),
    #[error("Orchestration state conflict")]
    Conflict,
    #[error("Orchestration resource was not found")]
    NotFound,
    #[error("Orchestration state requires resynchronization")]
    ResyncRequired,
    #[error("Orchestration binding rejection target is unsupported")]
    BindingRejectionTargetUnsupported,
    #[error("another mutable Orchestration runtime already owns this data-root ({0})")]
    OwnerAlreadyActive(String),
    #[error("Orchestration Agent Profile catalog failed")]
    ProfileCatalog(#[source] AgentProfileError),
    #[error("Orchestration Broker owner authority failed")]
    BrokerOwner(#[source] BrokerOwnerAuthorityError),
    #[error("Orchestration persistence failed")]
    Persistence(#[source] Box<OperationsError>),
    #[error("Orchestration filesystem failed")]
    Io(#[from] std::io::Error),
}

impl RuntimeError {
    /// Preserve a typed Operations source behind the runtime error boundary.
    #[must_use]
    pub fn persistence(error: OperationsError) -> Self {
        Self::Persistence(Box::new(error))
    }
}

impl From<OperationsError> for RuntimeError {
    fn from(error: OperationsError) -> Self {
        match error {
            OperationsError::ProfileDeletionEvidence(error) => profile_error_to_runtime(error),
            OperationsError::Bounds(_) => Self::InvalidInput,
            OperationsError::Conflict(_) => Self::Conflict,
            OperationsError::ResyncRequired(_) => Self::ResyncRequired,
            other => Self::persistence(other),
        }
    }
}

fn profile_error_to_runtime(error: AgentProfileError) -> RuntimeError {
    match error {
        AgentProfileError::InvalidIdentity(_)
        | AgentProfileError::InvalidRevision(_)
        | AgentProfileError::InvalidText { .. }
        | AgentProfileError::TooManyCapabilities { .. }
        | AgentProfileError::DuplicateCapability
        | AgentProfileError::Timestamp(_) => RuntimeError::InvalidInput,
        AgentProfileError::ProfileMissing(_) => RuntimeError::NotFound,
        AgentProfileError::ProfileRevisionConflict { .. }
        | AgentProfileError::ProfileClockRegression { .. }
        | AgentProfileError::LiveRuntimeSessions(_)
        | AgentProfileError::ReplayConflict(_)
        | AgentProfileError::ProfileMutationReplayConflict(_)
        | AgentProfileError::LogicalSessionReplayConflict(_)
        | AgentProfileError::LogicalSessionProfileConflict { .. }
        | AgentProfileError::ProfileNotActive { .. }
        | AgentProfileError::ProfileIdConflict(_) => RuntimeError::Conflict,
        other => RuntimeError::ProfileCatalog(other),
    }
}

impl From<AgentProfileError> for RuntimeError {
    fn from(error: AgentProfileError) -> Self {
        profile_error_to_runtime(error)
    }
}

fn validate_data_root(path: &Path) -> Result<(), RuntimeError> {
    if !path.is_absolute() {
        return Err(RuntimeError::InvalidDataRoot);
    }
    let metadata = std::fs::symlink_metadata(path).map_err(RuntimeError::Io)?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(RuntimeError::InvalidDataRoot);
    }
    let canonical = std::fs::canonicalize(path).map_err(RuntimeError::Io)?;
    if canonical != path {
        return Err(RuntimeError::InvalidDataRoot);
    }
    Ok(())
}

fn read_or_create_destination(path: &Path) -> Result<Uuid, RuntimeError> {
    match open_existing_destination(path) {
        Ok(mut file) => read_destination(&mut file),
        Err(RuntimeError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
            let destination = Uuid::now_v7();
            write_destination_once(path, destination)?;
            Ok(destination)
        }
        Err(error) => Err(error),
    }
}

fn open_existing_destination(path: &Path) -> Result<File, RuntimeError> {
    let metadata = std::fs::symlink_metadata(path).map_err(RuntimeError::Io)?;
    if !metadata.is_file() || metadata.file_type().is_symlink() {
        return Err(RuntimeError::CorruptDestinationIdentity);
    }
    File::open(path).map_err(RuntimeError::Io)
}

fn read_destination(file: &mut File) -> Result<Uuid, RuntimeError> {
    let mut bytes = Vec::with_capacity(DESTINATION_BYTES + 1);
    file.take((DESTINATION_BYTES + 1) as u64)
        .read_to_end(&mut bytes)?;
    if bytes.len() != DESTINATION_BYTES {
        return Err(RuntimeError::CorruptDestinationIdentity);
    }
    let destination =
        Uuid::from_slice(&bytes).map_err(|_| RuntimeError::CorruptDestinationIdentity)?;
    if destination.is_nil() {
        return Err(RuntimeError::CorruptDestinationIdentity);
    }
    Ok(destination)
}

const OWNER_LOCK_FILE_NAME: &str = "runtime-owner.sqlite3";

/// Take the process-spanning mutable-owner marker for one data-root.
///
/// The marker is a dedicated SQLite file opened with an uncommitted
/// `BEGIN EXCLUSIVE` transaction that lives as long as the returned
/// connection. A second mutable runtime on the same data-root fails its own
/// `BEGIN EXCLUSIVE` immediately and is refused with
/// [`RuntimeError::OwnerAlreadyActive`]; a crashed owner releases the lock
/// through ordinary SQLite journal recovery on the next open, so restart
/// semantics stay consistent without stale-marker cleanup.
fn acquire_owner_lock(root: &Path) -> Result<rusqlite::Connection, RuntimeError> {
    let connection = rusqlite::Connection::open_with_flags(
        root.join(OWNER_LOCK_FILE_NAME),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
            | rusqlite::OpenFlags::SQLITE_OPEN_CREATE
            | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX
            | rusqlite::OpenFlags::SQLITE_OPEN_NOFOLLOW,
    )
    .map_err(owner_lock_error)?;
    connection
        .busy_timeout(std::time::Duration::ZERO)
        .map_err(owner_lock_error)?;
    // No second connection or diagnostic query on conflict: it could wait on
    // the very exclusive lock that just refused admission. The held lock,
    // rather than a persisted boot row, is the ownership evidence.
    connection
        .execute_batch("PRAGMA journal_mode=DELETE; BEGIN EXCLUSIVE")
        .map_err(owner_lock_error)?;
    Ok(connection)
}

fn owner_lock_error(error: rusqlite::Error) -> RuntimeError {
    if matches!(
        &error,
        rusqlite::Error::SqliteFailure(failure, _)
            if matches!(failure.code, rusqlite::ErrorCode::DatabaseBusy | rusqlite::ErrorCode::DatabaseLocked)
    ) {
        RuntimeError::OwnerAlreadyActive("exclusive data-root lock is held".to_owned())
    } else {
        RuntimeError::persistence(OperationsError::Sqlite(error))
    }
}

fn write_destination_once(path: &Path, destination: Uuid) -> Result<(), RuntimeError> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    match options.open(path) {
        Ok(mut file) => {
            file.write_all(destination.as_bytes())?;
            file.sync_all()?;
            Ok(())
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            Err(RuntimeError::DestinationIdentityConflict)
        }
        Err(error) => Err(RuntimeError::Io(error)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exported_catalog_handles_retain_single_owner_after_runtime_drop()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = std::env::temp_dir().join(format!("catalog-owner-pin-{}", Uuid::now_v7()));
        std::fs::create_dir(&root)?;
        let runtime = OrchestrationRuntime::open(&root)?;
        let operations = runtime.operations_catalog();
        let profiles = runtime.profile_catalog();
        let operations_clone = operations.clone();
        drop(runtime);
        assert!(matches!(
            OrchestrationRuntime::open(&root),
            Err(RuntimeError::OwnerAlreadyActive(_))
        ));
        drop(operations);
        drop(profiles);
        assert!(matches!(
            OrchestrationRuntime::open(&root),
            Err(RuntimeError::OwnerAlreadyActive(_))
        ));
        drop(operations_clone);
        let reopened = OrchestrationRuntime::open(&root)?;
        reopened.probe()?;
        drop(reopened);
        std::fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn destination_identity_survives_runtime_reopen() -> Result<(), Box<dyn std::error::Error>> {
        let root = std::env::temp_dir().join(format!("orchestration-runtime-{}", Uuid::now_v7()));
        std::fs::create_dir(&root)?;
        let first = OrchestrationRuntime::open(&root)?;
        let destination = first.destination_id();
        drop(first);

        let reopened = OrchestrationRuntime::open(&root)?;
        assert_eq!(reopened.destination_id(), destination);
        reopened.probe()?;

        drop(reopened);
        std::fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn second_mutable_owner_on_the_same_data_root_is_refused()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = std::env::temp_dir().join(format!("orchestration-owner-{}", Uuid::now_v7()));
        std::fs::create_dir(&root)?;
        let first = OrchestrationRuntime::open(&root)?;

        let Err(second_error) = OrchestrationRuntime::open(&root) else {
            return Err("expected the second owner to be refused".into());
        };
        assert!(
            matches!(second_error, RuntimeError::OwnerAlreadyActive(_)),
            "expected owner conflict, got {second_error:?}"
        );
        // Refusal happens before a second mutable catalog can be returned.
        first.probe()?;

        // Crash/restart semantics: dropping the owner releases the lock.
        drop(first);
        let reopened = OrchestrationRuntime::open(&root)?;
        reopened.probe()?;
        drop(reopened);

        std::fs::remove_dir_all(root)?;
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn read_only_open_rejects_symlinked_destination_identity()
    -> Result<(), Box<dyn std::error::Error>> {
        use std::os::unix::fs::symlink;

        let root = std::env::temp_dir().join(format!(
            "orchestration-runtime-symlink-destination-{}",
            Uuid::now_v7()
        ));
        std::fs::create_dir(&root)?;
        let runtime = OrchestrationRuntime::open(&root)?;
        let destination = runtime.destination_id();
        drop(runtime);

        let identity_path = root.join(DESTINATION_FILE_NAME);
        let target_path = root.join("destination-target");
        std::fs::write(&target_path, destination.as_bytes())?;
        std::fs::remove_file(&identity_path)?;
        symlink(&target_path, &identity_path)?;

        let result = OrchestrationRuntime::open_existing_operations_read_only(&root);
        assert!(matches!(
            result,
            Err(RuntimeError::CorruptDestinationIdentity)
        ));

        std::fs::remove_dir_all(root)?;
        Ok(())
    }
}
