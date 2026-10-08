//! Shared HTTP wire contracts for the backend and its official client.
//! Identity strings retain server-side canonical validation; principal identity
//! is never accepted in a request body.

use heterarchy_orchestration_runtime::sessions::SessionSummary;
use heterarchy_orchestration_runtime::{AgentProfileCreateSummary, AgentProfileSummary};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub mod coordination;
pub mod departure;
pub mod handoff;
pub mod member_list;
pub mod membership;
pub mod node_chat;
pub mod node_team;
pub mod operations_import;
pub mod participation;
pub mod team_commands;
pub mod teams;
pub use teams::{
    TeamCreatorWire, TeamGetQueryRequest, TeamGetQueryResponse, TeamListQueryRequest,
    TeamListQueryResponse, TeamMemberWire, TeamMutationWire, TeamWire,
};
pub mod agent_request_admission;
pub mod agent_requests;
pub mod forge_reads;
pub mod profile_deletion;
pub mod profiles;
pub mod provider_worker;
pub mod provisioning_operations;
pub mod provisioning_participation;
pub mod work_credit;
pub use forge_reads::{
    DagCurrentQueryRequest, DagCurrentQueryResponse, DagCurrentWire, DagEdgeWire, DagLifecycleWire,
    DagNodeWire, DagStatusWire, DiscoveryDispositionWire, DiscoveryKindWire, DiscoveryReasonWire,
    DiscoverySignalWire, DiscoverySignalsWire, FileEntryKindWire, FileEntryWire,
    FileListQueryRequest, FileListQueryResponse, FileReadSliceQueryRequest,
    FileReadSliceQueryResponse, GoalGetQueryRequest, GoalGetQueryResponse, GoalListQueryRequest,
    GoalListQueryResponse, GoalStatusKindWire, GoalStatusWire, GoalVerificationKindWire,
    GoalVerificationWire, GoalWire, LocationListRequest, LocationListResponse, LocationRowWire,
    LocationStateWire, SourceEolWire, SourceScopeWire, TextSearchMatchWire, TextSearchQueryRequest,
    TextSearchQueryResponse, WorkspaceCatalogEntryWire, WorkspaceDiscoverRequest,
    WorkspaceDiscoverResponse, WorkspaceDiscoveryEntryWire, WorkspaceListRequest,
    WorkspaceListResponse, WorkspaceResolveRequest, WorkspaceResolveResponse,
};
pub use operations_import::{
    ImportPhaseWire, ImportPrepareRequest, ImportProgressResponse, ImportProgressWire,
    ImportStepRequest,
};
pub use profiles::{ProfileLifecycleRequest, ProfileMutationResponse, ProfileUpdateRequest};
pub use work_credit::{
    WorkCreditFulfillmentWire, WorkCreditProvenanceWire, WorkCreditScoreQueryRequest,
    WorkCreditScoreQueryResponse, WorkCreditScoreWire, WorkCreditSubjectFulfillmentWire,
    WorkCreditSubjectIdentityWire, WorkCreditSubjectKindWire, WorkCreditSubjectScoreWire,
};
pub mod leadership;
pub mod leadership_ballots;
pub mod leadership_lifecycle;
pub mod leadership_recalls;
pub mod leadership_reviews;
pub mod leadership_statements;
pub use leadership::{
    ElectionWire, LeadershipQueryRequest, LeadershipQueryResponse, LeadershipViewResponse,
    LeadershipWire, ReviewHistoryEntryWire, ReviewHistoryWire, WorkspaceQueryScope,
};
pub mod approval;
pub use approval::{
    ApprovalAdmissionWire, ApprovalAutoScopeWire, ApprovalDecideRequest, ApprovalDecideResponse,
    ApprovalListQueryRequest, ApprovalPermissionWire, ApprovalPolicyQueryRequest,
    ApprovalPolicySetRequest, ApprovalPolicyWire, ApprovalRequestGetRequest,
    ApprovalRequestGetResponse, ApprovalRequestWire, ApprovalSubmitRequest, ApprovalSubmitResponse,
    ListQueryResponse, PolicyQueryResponse, SetResponse,
};

pub use crate::profile_sessions::{ProfileSessionBindRequest, ProfileSessionBindResponse};

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
/// Profile creation intent; caller authority comes from the HTTP credential.
pub struct AgentProfileCreateRequest {
    pub request_id: String,
    pub stable_name: String,
    pub display_name: String,
    pub preferred_model: Option<String>,
    pub preferred_reasoning: Option<String>,
    pub base_capabilities: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
/// Canonical global Profile readback.
pub struct AgentProfileResponse {
    pub profile_id: Uuid,
    pub request_id: Uuid,
    pub stable_name: String,
    pub display_name: String,
    pub provider_family: String,
    pub preferred_model: Option<String>,
    pub preferred_reasoning: Option<String>,
    pub base_capabilities: Vec<String>,
    pub lifecycle_state: String,
    pub creation_origin: String,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
    pub last_used_at_ms: Option<i64>,
    pub revision: i64,
}

impl From<AgentProfileSummary> for AgentProfileResponse {
    fn from(value: AgentProfileSummary) -> Self {
        Self {
            profile_id: value.profile_id,
            request_id: value.request_id,
            stable_name: value.stable_name,
            display_name: value.display_name,
            provider_family: value.provider_family.to_owned(),
            preferred_model: value.preferred_model,
            preferred_reasoning: value.preferred_reasoning,
            base_capabilities: value.base_capabilities,
            lifecycle_state: value.lifecycle_state.to_owned(),
            creation_origin: value.creation_origin.to_owned(),
            created_at_ms: value.created_at_ms,
            updated_at_ms: value.updated_at_ms,
            last_used_at_ms: value.last_used_at_ms,
            revision: value.revision,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
/// Profile creation or exact nonce replay outcome.
pub struct AgentProfileCreateResponse {
    pub profile: AgentProfileResponse,
    pub created: bool,
}

impl From<AgentProfileCreateSummary> for AgentProfileCreateResponse {
    fn from(value: AgentProfileCreateSummary) -> Self {
        Self {
            profile: value.profile.into(),
            created: value.created,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
/// Exact replay identity for opening a principal-owned logical Session.
pub struct AgentSessionOpenRequest {
    pub request_id: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
/// Exact revision for closing a principal-owned logical Session.
pub struct AgentSessionCloseRequest {
    pub expected_revision: i64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
/// Session creation or exact nonce replay outcome.
pub struct AgentSessionOpenResponse {
    pub session: AgentSessionResponse,
    pub created: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
/// Principal-bound readback for one exact Session-open nonce.
pub struct AgentSessionOpenReadbackResponse {
    pub request_id: Uuid,
    pub session: Option<AgentSessionResponse>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
/// Canonical principal-owned Session readback.
pub struct AgentSessionResponse {
    pub session_id: Uuid,
    pub request_id: Uuid,
    pub state: String,
    pub revision: i64,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

impl From<SessionSummary> for AgentSessionResponse {
    fn from(session: SessionSummary) -> Self {
        Self {
            session_id: session.session_id,
            request_id: session.request_id,
            state: session.state.to_owned(),
            revision: session.revision,
            created_at_ms: session.created_at_ms,
            updated_at_ms: session.updated_at_ms,
        }
    }
}
