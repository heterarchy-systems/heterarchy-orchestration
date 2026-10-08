//! Bounded typed wire contracts for Forge source read queries.
//!
//! These endpoints observe the configured Forge source. No response field is
//! a permission: read rows never promote to scope membership, lease
//! ownership, actor identity, or mutation grants.

pub mod coordinator;
pub mod file;
pub mod goal_dag;
pub mod leases;
pub mod workspace;

pub use coordinator::{CoordinatorBindingWire, CoordinatorGetRequest, CoordinatorGetResponse};
pub use file::{
    FileEntryKindWire, FileEntryWire, FileListQueryRequest, FileListQueryResponse,
    FileReadSliceQueryRequest, FileReadSliceQueryResponse, SourceEolWire, TextSearchMatchWire,
    TextSearchQueryRequest, TextSearchQueryResponse,
};
pub use goal_dag::{
    DagCurrentQueryRequest, DagCurrentQueryResponse, DagCurrentWire, DagEdgeWire, DagLifecycleWire,
    DagNodeWire, DagStatusWire, GoalGetQueryRequest, GoalGetQueryResponse, GoalListQueryRequest,
    GoalListQueryResponse, GoalStatusKindWire, GoalStatusWire, GoalVerificationKindWire,
    GoalVerificationWire, GoalWire,
};
pub use leases::{
    CommandListQueryRequest, CommandListQueryResponse, DiffArtifactHealthWire, DiffChangeKindWire,
    DiffDoctorQueryRequest, DiffDoctorQueryResponse, DiffEntrySubjectWire, DiffEntryWire,
    DiffEvidenceStateWire, DiffEvidenceWire, DiffListQueryRequest, DiffListQueryResponse,
    DiffOriginWire, DiffReadQueryRequest, DiffReadQueryResponse, DiffRenderKindWire,
    ProjectCommandClassWire, ProjectCommandWire, WriterLeaseGetQueryRequest,
    WriterLeaseGetQueryResponse, WriterLeaseListQueryRequest, WriterLeaseListQueryResponse,
    WriterLeaseProjectionWire, WriterLeaseScopeKindWire, WriterLeaseScopeWire,
    WriterLeaseStateWire, WriterLeaseStatusWire,
};
pub use workspace::{
    DiscoveryDispositionWire, DiscoveryKindWire, DiscoveryReasonWire, DiscoverySignalWire,
    DiscoverySignalsWire, LocationListRequest, LocationListResponse, LocationRowWire,
    LocationStateWire, SourceScopeWire, WorkspaceCatalogEntryWire, WorkspaceDiscoverRequest,
    WorkspaceDiscoverResponse, WorkspaceDiscoveryEntryWire, WorkspaceListRequest,
    WorkspaceListResponse, WorkspaceResolveRequest, WorkspaceResolveResponse,
};
