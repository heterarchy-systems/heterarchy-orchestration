//! Orchestration-owned participant, event and inbox persistence.
//! Imported effect references are migration evidence; Forge retains live work truth.
mod activation;
mod adoption;
mod agent_profile_view;
mod agent_provision;
mod agent_request;
mod agent_request_resolution;
mod approval;
mod approval_versions;
mod broker_cluster;
mod broker_fence;
mod broker_owner;
mod catalog;
mod coordination;
mod cursor;
mod departure;
mod departure_observation;
mod effect_binding;
mod effect_fact;
mod election_subject;
mod events;
mod import_handoff;
mod importer;
mod inbox;
mod leadership;
mod members;
mod membership;
mod node_team;
mod operational_actor;
pub use node_team::{
    LegacyNodeTeamBindingRetirementAuthority, LegacyNodeTeamBindingRetirementReceipt,
    MAX_NODE_TEAM_BINDINGS_PER_READ, NodeTeamBinding, NodeTeamBindingPage, NodeTeamBindingScope,
    RetireLegacyNodeTeamBindingCommand, SetNodeTeamBindingCommand,
};
mod operational_messages;
mod presence;
mod profile_deletion;
mod provider_deletion;
mod registration;
mod registration_profile;
mod replacement_registration;
mod session_replacement;
mod team_assignment;
mod teams;
mod wire;
mod work_credit;
mod work_credit_subject;
mod workspace_chat_target;
mod workspace_participation;
mod workspace_project;

pub use workspace_chat_target::{WorkspaceChatTarget, WorkspaceChatTargetPage};
pub use workspace_project::{
    ChatGptProjectId, MAX_WORKSPACE_AUTHORITY_SCOPES, WorkspaceProjectBinding,
    WorkspaceProjectBindingReceipt, WorkspaceProjectBindingRevision,
    WorkspaceProjectConfigureCommand, WorkspaceProjectConfigureOutcome, WorkspaceProjectEnrollment,
    WorkspaceProjectReference, WorkspaceProjectScope,
};

mod binding_observation;
mod binding_rejection;
pub use activation::RegistrationActivationReceipt;
pub use agent_profile_view::{
    AgentProfileSessionRuntimeObservation, AgentProfileWorkspaceCurrentIdentityView,
    AgentProfileWorkspaceEffectView, AgentProfileWorkspaceReadError, AgentProfileWorkspaceReadView,
    AgentProfileWorkspaceSessionBindingKind, AgentProfileWorkspaceSessionView,
    MAX_AGENT_PROFILE_VERIFIED_SESSIONS,
};
pub use agent_provision::{
    AgentProvisionDispatch, AgentProvisionDispatchAction, AgentProvisionDispatchOutcome,
    AgentProvisionEffectKind, AgentProvisionFailureCode, AgentProvisionOperation,
    AgentProvisionPrepareOutcome, AgentProvisionProviderFamily, AgentProvisionState,
    AgentProvisionUncertaintyRetirement, AgentProvisionUncertaintyRetirementCommand,
    AgentProvisionUncertaintyRetirementOutcome, AgentProvisionUncertaintyRetirementTarget,
};
pub use agent_request::{
    AgentRequestCommand, AgentRequestCurrentScopeProfiles, AgentRequestEffectScope,
    AgentRequestIndependence, AgentRequestIntent, AgentRequestRecord, AgentRequestRequester,
    AgentRequestState, AgentRequestSubmitOutcome, MAX_AGENT_REQUEST_CURRENT_SCOPE_PROFILES,
};
pub use agent_request_resolution::{
    AgentRequestResolution, AgentRequestResolutionKind, AgentRequestResolutionOutcome,
    AgentRequestResolutionSelection, AgentRequestResolutionSlot, CommitAgentRequestResolution,
};
pub use approval::{
    AdmissionIssuance, AdmissionIssuerConfig, ApprovalAutoScope, ApprovalDecisionOutcome,
    ApprovalExpiryReconciliation, ApprovalPolicyDisableOutcome, ApprovalPolicyRuntimeView,
    ApprovalPolicyVersionChange, ApprovalPolicyView, ApprovalRequestQuery, ApprovalRequestRecord,
    ApprovalRequestVersionChange, ApprovalScopeRuntimeView, ApprovalSubmission,
    ConfigureAdmissionIssuerCommand, DecideApprovalCommand, MAX_ADMISSION_TTL_MS,
    MAX_APPROVAL_AUTO_SCOPES, MAX_APPROVAL_LIST_LIMIT, MAX_APPROVAL_OBSERVATION_CHANGES,
    MAX_APPROVAL_POLICY_JSON_BYTES, MAX_REQUEST_TTL_MS, MIN_ADMISSION_TTL_MS, MIN_REQUEST_TTL_MS,
    SetApprovalPolicyCommand, SubmitApprovalCommand,
};
pub use binding_observation::{
    BindingActivationObservation, BindingRejectionObservation, BindingReservationObservation,
};
pub use binding_rejection::{
    BindingRejectionAttemptDisposition, BindingRejectionAttemptRequest,
    BindingRejectionAttemptResolution, BindingRejectionSourceRequestPolicy,
    LegacyBindingRejectionEnvelopePolicy, RegistrationBindingRejectionReceipt,
    ResolveBindingRejectionAttempt, TrustedLegacyBindingRejectionDeployment,
};
pub use broker_cluster::{
    BrokerClusterConfig, BrokerClusterNodeProbe, BrokerClusterProbe, DEFAULT_BROKER_CLUSTER_TIMEOUT,
};
pub use broker_fence::{
    BrokerAuthorityFence, BrokerFenceConfig, DEFAULT_FENCE_TIMEOUT, LeadershipFenceError,
};
pub use broker_owner::{
    BrokerOwnerAuthority, BrokerOwnerAuthorityError, BrokerOwnerLease, BrokerOwnerObservation,
    BrokerOwnerRecoveryApproval, BrokerOwnerRecoveryError,
};
pub use catalog::{ImportPhase, ImportProgress, OperationsCatalog};
pub use coordination::{
    LogicalActivityJoinedEvent, LogicalActivityParticipant, LogicalActivitySnapshot,
};
pub use departure::{
    LogicalDepartureKind, LogicalDepartureReceipt, LogicalDetachCommand, LogicalHandoffCommand,
};
pub use departure_observation::{DepartureObservation, DepartureObservationParts};
pub use effect_binding::{
    LogicalSessionEffectBinding, LogicalSessionIdentityProjection,
    MAX_LOGICAL_SESSION_EFFECT_BINDINGS,
};
pub use effect_fact::{
    AssignmentOfferProjectionRow, AssignmentOfferedFact, AssignmentReviewProjectionRow,
    AssignmentReviewedFact, AssignmentSubmitProjectionRow, AssignmentSubmittedFact,
    ChangeSetProjectionRow, ChangeSetTerminalFact, EffectFactConsumerProgress, EffectFactInput,
    ExecutionTerminalFact, ExecutionTerminalProjectionRow,
};
pub use election_subject::ElectionSubjectCutoverReceipt;
pub use events::{InboxReceiptRecord, PresenceRecord};
pub use forge_domain::AgentBindingRejectionReason;
pub use forge_persistence::{
    AgentDepartureCommand, AgentDepartureKind, AgentDepartureReconciliationRequest,
    AgentOperationsExportCursor, AgentOperationsExportPage, AgentOperationsFreeze,
    AssignmentReviewedOutcome, AssignmentSubmittedSourceRef, DagNodeAssignmentSnapshot,
    DagNodeEvidenceScope,
};
pub use import_handoff::{CurrentImportHandoff, ImportHandoffSeal, ImportHandoffSealCommand};
pub use importer::ImportVerificationTarget;
pub use inbox::{LogicalInboxAck, LogicalInboxEntry, LogicalInboxQuery, LogicalInboxTarget};
pub use leadership::ballots::LeadershipBallotSnapshot;
pub use leadership::confirm_saga::{
    FencedConfirmCommand, FencedConfirmOutcome, LeadershipFenceExecutor, ProductionFence,
    complete_fenced_confirm, replay_fenced_confirm, run_fenced_confirm,
};
pub use leadership::work_exchange::{
    LeadershipWorkExchangePage, LeadershipWorkExchangeReason, LeadershipWorkExchangeRecord,
    LeadershipWorkExchangeState,
};
pub use leadership::work_roster::{
    LeadershipWorkRoster, LeadershipWorkRosterBinding, LeadershipWorkRosterBindingOrigin,
    LeadershipWorkRosterDispatch, LeadershipWorkRosterPrepareOutcome, LeadershipWorkRosterSession,
    LeadershipWorkRosterSide, LeadershipWorkRosterState,
};
pub use leadership::{
    BALLOT_WINDOW_MS, BallotChoice, BallotReceipt, CastBallotCommand, CloseElectionCommand,
    ConfirmBootstrapEvidenceCommand, ConfirmPendingBootstrapCommand, CurrentLeadershipAuthority,
    ElectionState, ElectionTally, LEADER_UNRESPONSIVE_AFTER_MS, LeadershipConfirmIntent,
    LeadershipConfirmIntentState, LeadershipConfirmReadback, LeadershipElectionSummary,
    LeadershipFenceExclusionProof, LeadershipFenceOutcome, LeadershipFenceRequest,
    LeadershipMutationReceipt, LeadershipRecoveryReason, LeadershipRecoveryResolution,
    LeadershipScope, LeadershipState, LeadershipView, MarkRecoveryRequiredCommand,
    MarkStableCommand, MarkUnresponsiveFailoverCommand, PrepareLeadershipConfirmCommand,
    PrepareLeadershipConfirmOutcome, PreparedLeadershipConfirm, RecallElectionCommand,
    RecallElectionReceipt, ReconcileRecoveryCommand, STABILIZATION_WINDOW_MS,
};
pub use membership::{
    MembershipCommitReceipt, VisibleLogicalEvent, VisibleLogicalMember, VisibleLogicalReceipt,
};
pub use operational_actor::OperationalActor;
pub use operational_messages::governance::{
    GovernanceInvitation, GovernanceInvitationPhase, GovernanceInvitationQueueOutcome,
};
pub use operational_messages::{
    AppendWorkspaceMessageCommand, EnqueueProviderMessageCommand, IncomingProviderMessageOutcome,
    MAX_MESSAGE_BODY_BYTES, MAX_MESSAGE_PAGE_SIZE, MAX_PROVIDER_ID_BYTES,
    MAX_PROVIDER_MESSAGE_JSON_BYTES, MessageAppendOutcome, MessageAuthor, MessageChannel,
    MessageContext, MessageId, MessageOrigin, MessagePage, MessageReadScope,
    NodeMessageRouteTarget, NodeMessageRouteWitness, ProviderMessageDeliveryReceipt,
    ProviderMessageDispatch, ProviderMessageDispatchAction, ProviderMessageDispatchOutcome,
    ProviderMessageInFlightPage, ProviderMessageOutbox, ProviderMessageQueueOutcome,
    ProviderMessageSource, ProviderMessageState, ProviderMessageTarget,
    ProviderMessageUnknownIsolationCommand, ProviderMessageUnknownIsolationReason,
    ProviderMessageUnknownIsolationReceipt, ProviderTurnObservation,
    RecordIncomingProviderMessageCommand, WorkspaceMessage, provider_message_text,
    validate_chat_text,
};
pub use presence::{LogicalPresenceSnapshot, LogicalPresenceTarget, LogicalPresenceTouch};
pub use profile_deletion::{AgentProfileDeletionBarrier, AgentProfileDeletionInFlightCounts};
pub use provider_deletion::{
    AgentProfileDeletionFreeze, AgentProfileDeletionProgress, AgentProfileDeletionProgressAdvance,
    AgentProfileDeletionProgressDeferredReason, AgentProfileDeletionProgressPhase,
    AgentProfileDeletionProviderMessageReference, AgentProfileDeletionProviderMessageReferencePage,
    AgentProfileDeletionProvisionReference, AgentProfileDeletionProvisionReferencePage,
    AgentProfileDeletionRuntimeReferenceSummary, AgentProfileProviderDeletionAction,
    AgentProfileProviderDeletionDeferredReason, AgentProfileProviderDeletionDispatch,
    AgentProfileProviderDeletionOperation, AgentProfileProviderDeletionPage,
    AgentProfileProviderDeletionPrepareOutcome, AgentProfileProviderDeletionReceipt,
    AgentProfileProviderDeletionReceiptOutcome, AgentProfileProviderDeletionState,
    AgentProfileProviderDeletionSummary, MAX_AGENT_PROFILE_DELETION_PAGE_SIZE,
};
// Existing typed source witnesses used by public registration contracts.
// Composition passes the original Forge receipt; no parallel witness type is
// introduced and Runtime gains no Forge persistence/effect dependency.
pub use forge_persistence::{
    AgentBindingActivation, AgentBindingActivationRequest, AgentBindingRejectionOperation,
    AgentBindingRejectionReceipt, AgentBindingReservation, AgentBindingReservationRequest,
    AgentOperationsDelegation,
};
pub use registration::{
    RegistrationPhase, RegistrationProgress, RegistrationRequest, native_session_participation_id,
};
pub use registration_profile::RegistrationProfileScope;
pub use replacement_registration::{
    WorkspaceParticipationReplacementActivationReceipt,
    WorkspaceParticipationReplacementCommitReceipt, WorkspaceParticipationReplacementProgress,
};
pub use session_replacement::{
    WorkspaceParticipationSessionExpectation, WorkspaceParticipationSessionRebindCommand,
    WorkspaceParticipationSessionRebindReceipt,
};
pub use teams::{
    MAX_TEAM_MEMBERS_PER_READ, MAX_TEAMS_PER_READ, TeamCreateCommand, TeamCreator,
    TeamFormationCandidate, TeamFormationCandidatePage, TeamJoinCommand, TeamLeaveCommand,
    TeamMemberRecord, TeamMembershipState, TeamMutationRecord, TeamQuerySnapshot, TeamReadSet,
    TeamRecord, TeamRetireCommand, TeamRosterMemberInput, TeamState,
};
pub use work_credit::{
    ProvenanceDimension, WORK_CREDIT_WINDOW_MS, WorkCreditEntryRow, WorkCreditProvenanceMatch,
    WorkCreditScoreSnapshot, WorkCreditSubmitProvenanceRow,
};
pub use work_credit_subject::{
    WorkCreditElectionSnapshotReceipt, WorkCreditSubjectFulfillment, WorkCreditSubjectIdentity,
    WorkCreditSubjectKind, WorkCreditSubjectScore,
};
pub use workspace_participation::{
    MAX_WORKSPACE_PARTICIPATIONS_PER_READ, WorkspaceParticipationId,
    WorkspaceParticipationMappingReceipt, WorkspaceParticipationReadSet,
    WorkspaceParticipationSessionMapping, WorkspaceParticipationSnapshot,
};

#[derive(Debug, thiserror::Error)]
pub enum OperationsError {
    #[error("Sessions Profile deletion evidence failed")]
    ProfileDeletionEvidence(#[source] heterarchy_orchestration_sessions::AgentProfileError),
    #[error(
        "operations catalog {destination_id} commit is unconfirmed; reopen the same catalog identity"
    )]
    CatalogCommitUnconfirmed {
        destination_id: uuid::Uuid,
        #[source]
        source: rusqlite::Error,
    },
    #[error("operations import {migration_id} commit is unconfirmed; read the same identity")]
    CommitUnconfirmed {
        migration_id: uuid::Uuid,
        #[source]
        source: rusqlite::Error,
    },
    #[error("operations registration {request_id} commit is unconfirmed; read the same identity")]
    RegistrationCommitUnconfirmed {
        request_id: uuid::Uuid,
        #[source]
        source: rusqlite::Error,
    },
    #[error("operations membership commit {request_id} is unconfirmed; read the same identity")]
    MembershipCommitUnconfirmed {
        request_id: uuid::Uuid,
        #[source]
        source: rusqlite::Error,
    },
    #[error("operations activation {request_id} commit is unconfirmed; read the same identity")]
    ActivationCommitUnconfirmed {
        request_id: uuid::Uuid,
        #[source]
        source: rusqlite::Error,
    },
    #[error(
        "operations binding rejection {request_id} commit is unconfirmed; read the same identity"
    )]
    BindingRejectionCommitUnconfirmed {
        request_id: uuid::Uuid,
        #[source]
        source: rusqlite::Error,
    },
    #[error("legacy adoption {request_id} commit is unconfirmed; read the same adoption identity")]
    AdoptionCommitUnconfirmed {
        request_id: uuid::Uuid,
        #[source]
        source: rusqlite::Error,
    },
    #[error(
        "presence commit outcome is unknown for workspace {workspace_id} participant {participant_id}: {source}"
    )]
    PresenceCommitUnconfirmed {
        workspace_id: uuid::Uuid,
        participant_id: uuid::Uuid,
        #[source]
        source: rusqlite::Error,
    },
    #[error(
        "departure commit outcome is unknown for workspace {workspace_id} participant {participant_id}: {source}"
    )]
    DepartureCommitUnconfirmed {
        workspace_id: uuid::Uuid,
        participant_id: uuid::Uuid,
        #[source]
        source: rusqlite::Error,
    },
    #[error(
        "inbox acknowledgement commit outcome is unknown for workspace {workspace_id} participant {participant_id} event {event_id}: {source}"
    )]
    InboxCommitUnconfirmed {
        workspace_id: uuid::Uuid,
        participant_id: uuid::Uuid,
        event_id: uuid::Uuid,
        #[source]
        source: rusqlite::Error,
    },
    #[error("leadership commit outcome is unknown for workspace {workspace_id}: {source}")]
    LeadershipCommitUnconfirmed {
        workspace_id: uuid::Uuid,
        #[source]
        source: rusqlite::Error,
    },
    #[error("approval commit outcome is unknown for workspace {workspace_id}: {source}")]
    ApprovalCommitUnconfirmed {
        workspace_id: uuid::Uuid,
        #[source]
        source: rusqlite::Error,
    },
    #[error(
        "leadership confirm fence outcome is unknown for workspace {workspace_id} election {election_id} request {request_id}: the Broker authority record is unresolved and the intent stays resumable"
    )]
    LeadershipFenceUnconfirmed {
        workspace_id: uuid::Uuid,
        election_id: uuid::Uuid,
        request_id: uuid::Uuid,
    },
    #[error(
        "Workspace participation Session rebind {request_id} commit is unconfirmed for catalog {destination_id}; read back the same request identity"
    )]
    SessionRebindCommitUnconfirmed {
        destination_id: uuid::Uuid,
        request_id: uuid::Uuid,
        #[source]
        source: rusqlite::Error,
    },
    #[error(
        "Workspace participation replacement registration {request_id} commit is unconfirmed for catalog {destination_id}; read back the same request identity"
    )]
    ReplacementRegistrationCommitUnconfirmed {
        destination_id: uuid::Uuid,
        request_id: uuid::Uuid,
        #[source]
        source: rusqlite::Error,
    },
    #[error("AgentRequest {request_id} commit is unconfirmed; read back the same request identity")]
    AgentRequestCommitUnconfirmed {
        request_id: uuid::Uuid,
        #[source]
        source: rusqlite::Error,
    },
    #[error(
        "Agent provision operation {operation_id} commit is unconfirmed; read back the same operation identity"
    )]
    AgentProvisionCommitUnconfirmed {
        operation_id: uuid::Uuid,
        #[source]
        source: rusqlite::Error,
    },
    #[error(
        "Agent provision uncertainty retirement {retirement_id} for operation {operation_id} commit is unconfirmed in catalog {destination_id}; query the same operation identity before retrying"
    )]
    AgentProvisionUncertaintyRetirementCommitUnconfirmed {
        operation_id: uuid::Uuid,
        retirement_id: uuid::Uuid,
        destination_id: uuid::Uuid,
        #[source]
        source: rusqlite::Error,
    },
    #[error(
        "Agent Profile deletion barrier {request_id} commit is unconfirmed; read back the same request identity"
    )]
    AgentProfileDeletionBarrierCommitUnconfirmed {
        request_id: uuid::Uuid,
        #[source]
        source: rusqlite::Error,
    },
    #[error(
        "Agent Profile deletion progress {request_id} commit is unconfirmed; read back the same request"
    )]
    AgentProfileDeletionProgressCommitUnconfirmed {
        request_id: uuid::Uuid,
        #[source]
        source: rusqlite::Error,
    },
    #[error(
        "Agent Profile provider deletion {operation_id} attempt commit is unconfirmed; read back the same operation"
    )]
    AgentProfileProviderDeletionCommitUnconfirmed {
        operation_id: uuid::Uuid,
        attempt_number: Option<u64>,
        #[source]
        source: rusqlite::Error,
    },
    #[error("operations commit is unconfirmed for Workspace {workspace_id}, request {request_id}")]
    OperationCommitUnconfirmed {
        workspace_id: forge_domain::WorkspaceId,
        request_id: uuid::Uuid,
        #[source]
        source: rusqlite::Error,
    },
    #[error(transparent)]
    Sqlite(#[from] rusqlite::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("node binding {scope:?} commit is unconfirmed; read the exact scope before retrying")]
    NodeTeamBindingCommitUnconfirmed {
        scope: Box<node_team::NodeTeamBindingScope>,
        #[source]
        source: rusqlite::Error,
    },
    #[error("operations state is corrupt: {0}")]
    Corrupt(&'static str),
    #[error("operations request conflicts: {0}")]
    Conflict(&'static str),
    #[error("operations projection requires source resynchronization: {0}")]
    ResyncRequired(&'static str),
    #[error("operations bound exceeded: {0}")]
    Bounds(&'static str),
    #[error("operations source failed: {0}")]
    Source(#[from] forge_persistence::WorkspaceStoreError),
}

#[cfg(test)]
mod tests;

pub use team_assignment::{
    AssignmentAffinityCommand, AssignmentAffinityDisposition, AssignmentAffinityReceipt,
    AssignmentAffinityReservationPage, AssignmentOfferSealEvidence,
    AssignmentOfferSealObservationKind, MAX_ASSIGNMENT_AFFINITY_RESERVATION_PAGE_SIZE,
    MAX_ORIGINAL_OFFER_PAYLOAD_BYTES,
};

pub use leadership::governance::{
    LeadershipIdentitySnapshot, LeadershipReviewCommand, LeadershipReviewPhase,
    LeadershipReviewQuery, LeadershipReviewReceipt, LeadershipReviewSnapshot,
    LeadershipStatementCommand, LeadershipStatementKind, LeadershipStatementPage,
    LeadershipStatementReadback, LeadershipStatementReceipt, LeadershipStatementRecord,
    LeadershipVoterSnapshot,
};

pub use leadership::candidates::LeadershipCandidate;

pub use leadership::governance::{LeadershipReviewHistory, LeadershipReviewHistoryEntry};
