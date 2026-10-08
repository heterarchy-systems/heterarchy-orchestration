//! Durable Workspace conversation messages and provider delivery effects.
//!
//! This module owns the small native persistence boundary used by the
//! Workbench.  It deliberately stores public message content and opaque Forge
//! references only.  Provider transport, authentication, and Forge reference
//! validation remain at their respective ingress boundaries.

use forge_domain::{
    ActivityId, AgentSessionId, DagNodeId, DagRevisionNumber, GoalId, GoalRevision,
    TimestampMillis, WorkspaceId,
};
use heterarchy_orchestration_sessions::{
    AgentProfileId, AgentProfileProviderFamily, AgentProfileRuntimeSessionId,
};
use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use std::fmt::Write as _;
use uuid::Uuid;

use crate::{
    OperationalActor, OperationsCatalog, OperationsError, WorkspaceParticipationId,
    operational_actor::{ResolvedOperationalActor, resolve_actor},
};

pub(crate) mod governance;
mod provider_observation;
pub use provider_observation::{ProviderMessageInFlightPage, ProviderTurnObservation};
mod node_route;

pub use node_route::{NodeMessageRouteTarget, NodeMessageRouteWitness};

/// Public Workspace message bodies are bounded to 16 KiB of UTF-8 bytes.
pub const MAX_MESSAGE_BODY_BYTES: usize = 16 * 1024;
/// Provider conversation and turn identities are opaque, bounded strings.
pub const MAX_PROVIDER_ID_BYTES: usize = 512;
/// Cursor reads never return more than 128 rows.
pub const MAX_MESSAGE_PAGE_SIZE: usize = 128;
/// Provider JSON payloads remain below the native bridge's bounded frame.
pub const MAX_PROVIDER_MESSAGE_JSON_BYTES: usize = 48 * 1024;
const MAX_DISPLAY_NAME_BYTES: usize = 128;
const MAX_PROVIDER_SOURCE_LABEL_BYTES: usize = 256;

/// Caller-stable message/request identity.  It is intentionally the same
/// opaque UUID used as the durable message primary key.
pub type MessageId = Uuid;

const PROVIDER_ROUTES_SQL: &str = r"
CREATE TABLE ops_provider_message_routes (
    provider_family INTEGER NOT NULL CHECK(provider_family=1),
    runtime_session_id BLOB NOT NULL CHECK(length(runtime_session_id)=16 AND runtime_session_id != zeroblob(16)),
    provider_conversation_id TEXT NOT NULL CHECK(length(CAST(provider_conversation_id AS BLOB)) BETWEEN 1 AND 512),
    workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16),
    recipient_profile_id BLOB NOT NULL CHECK(length(recipient_profile_id)=16 AND recipient_profile_id != zeroblob(16)),
    recipient_participation_id BLOB NOT NULL CHECK(length(recipient_participation_id)=16 AND recipient_participation_id != zeroblob(16)),
    created_at_ms INTEGER NOT NULL CHECK(created_at_ms>=0),
    PRIMARY KEY(provider_family,runtime_session_id,provider_conversation_id),
    UNIQUE(provider_family,runtime_session_id),
    UNIQUE(provider_family,provider_conversation_id),
    FOREIGN KEY(workspace_id,recipient_profile_id) REFERENCES ops_workspace_participations(workspace_id,agent_profile_id),
    FOREIGN KEY(workspace_id,recipient_participation_id) REFERENCES ops_workspace_participations(workspace_id,participation_id)
);
";

const WORKSPACE_MESSAGES_SQL_V31: &str = r"
CREATE TABLE ops_workspace_messages (
    message_id BLOB PRIMARY KEY NOT NULL CHECK(length(message_id)=16 AND message_id != zeroblob(16)),
    workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16),
    channel_kind INTEGER NOT NULL CHECK(channel_kind IN(0,1)),
    channel_participation_id BLOB CHECK(channel_participation_id IS NULL OR (length(channel_participation_id)=16 AND channel_participation_id != zeroblob(16))),
    author_kind INTEGER NOT NULL CHECK(author_kind IN(0,1,2)),
    author_participation_id BLOB CHECK(author_participation_id IS NULL OR (length(author_participation_id)=16 AND author_participation_id != zeroblob(16))),
    author_profile_id BLOB CHECK(author_profile_id IS NULL OR (length(author_profile_id)=16 AND author_profile_id != zeroblob(16))),
    author_participant_id BLOB CHECK(author_participant_id IS NULL OR (length(author_participant_id)=16 AND author_participant_id != zeroblob(16))),
    author_session_id BLOB CHECK(author_session_id IS NULL OR (length(author_session_id)=16 AND author_session_id != zeroblob(16))),
    author_display_name TEXT NOT NULL CHECK(length(CAST(author_display_name AS BLOB)) BETWEEN 1 AND 128),
    origin_kind INTEGER NOT NULL CHECK(origin_kind IN(0,1)),
    body TEXT NOT NULL CHECK(length(CAST(body AS BLOB)) BETWEEN 1 AND 16384),
    sequence INTEGER NOT NULL CHECK(sequence>=1),
    submitted_at_ms INTEGER NOT NULL CHECK(submitted_at_ms>=0),
    activity_id BLOB CHECK(activity_id IS NULL OR length(activity_id)=16),
    goal_id BLOB CHECK(goal_id IS NULL OR length(goal_id)=16),
    dag_revision INTEGER CHECK(dag_revision IS NULL OR dag_revision>=1),
    node_id BLOB CHECK(node_id IS NULL OR length(node_id)=16),
    provider_conversation_id TEXT CHECK(provider_conversation_id IS NULL OR length(CAST(provider_conversation_id AS BLOB)) BETWEEN 1 AND 512),
    provider_turn_id TEXT CHECK(provider_turn_id IS NULL OR length(CAST(provider_turn_id AS BLOB)) BETWEEN 1 AND 512),
    provider_source_kind INTEGER CHECK(provider_source_kind IS NULL OR provider_source_kind IN(0,1)),
    provider_source_profile_id BLOB CHECK(provider_source_profile_id IS NULL OR (length(provider_source_profile_id)=16 AND provider_source_profile_id != zeroblob(16))),
    provider_source_participation_id BLOB CHECK(provider_source_participation_id IS NULL OR (length(provider_source_participation_id)=16 AND provider_source_participation_id != zeroblob(16))),
    provider_source_label TEXT CHECK(provider_source_label IS NULL OR length(CAST(provider_source_label AS BLOB)) BETWEEN 1 AND 256),
    UNIQUE(workspace_id, sequence),
    UNIQUE(workspace_id, provider_conversation_id, provider_turn_id),
    CHECK((channel_kind=0 AND channel_participation_id IS NULL) OR (channel_kind=1 AND channel_participation_id IS NOT NULL)),
    CHECK((author_kind=0 AND author_participation_id IS NULL AND author_profile_id IS NULL AND author_participant_id IS NULL AND author_session_id IS NULL) OR (author_kind=1 AND author_participation_id IS NOT NULL AND author_profile_id IS NOT NULL AND author_participant_id IS NOT NULL AND author_session_id IS NOT NULL) OR (author_kind=2 AND author_participation_id IS NULL AND author_profile_id IS NULL AND author_participant_id IS NULL AND author_session_id IS NULL)),
    CHECK((origin_kind=0 AND provider_conversation_id IS NULL AND provider_turn_id IS NULL AND provider_source_kind IS NULL AND provider_source_profile_id IS NULL AND provider_source_participation_id IS NULL AND provider_source_label IS NULL) OR (origin_kind=1 AND provider_conversation_id IS NOT NULL AND provider_turn_id IS NOT NULL AND provider_source_kind IS NOT NULL)),
    CHECK((provider_source_kind=0 AND provider_source_profile_id IS NOT NULL AND provider_source_participation_id IS NOT NULL) OR (provider_source_kind=1 AND provider_source_profile_id IS NULL AND provider_source_participation_id IS NULL) OR provider_source_kind IS NULL),
    CHECK((goal_id IS NOT NULL) OR (dag_revision IS NULL AND node_id IS NULL)),
    CHECK(dag_revision IS NOT NULL OR node_id IS NULL)
);
";

const WORKSPACE_MESSAGES_SQL_V35: &str = r"
CREATE TABLE ops_workspace_messages (
    message_id BLOB PRIMARY KEY NOT NULL CHECK(length(message_id)=16 AND message_id != zeroblob(16)),
    workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16),
    channel_kind INTEGER NOT NULL CHECK(channel_kind IN(0,1)),
    channel_participation_id BLOB CHECK(channel_participation_id IS NULL OR (length(channel_participation_id)=16 AND channel_participation_id != zeroblob(16))),
    author_kind INTEGER NOT NULL CHECK(author_kind IN(0,1,2,3)),
    author_participation_id BLOB CHECK(author_participation_id IS NULL OR (length(author_participation_id)=16 AND author_participation_id != zeroblob(16))),
    author_profile_id BLOB CHECK(author_profile_id IS NULL OR (length(author_profile_id)=16 AND author_profile_id != zeroblob(16))),
    author_participant_id BLOB CHECK(author_participant_id IS NULL OR (length(author_participant_id)=16 AND author_participant_id != zeroblob(16))),
    author_session_id BLOB CHECK(author_session_id IS NULL OR (length(author_session_id)=16 AND author_session_id != zeroblob(16))),
    author_display_name TEXT NOT NULL CHECK(length(CAST(author_display_name AS BLOB)) BETWEEN 1 AND 128),
    origin_kind INTEGER NOT NULL CHECK(origin_kind IN(0,1)),
    body TEXT NOT NULL CHECK(length(CAST(body AS BLOB)) BETWEEN 1 AND 16384),
    sequence INTEGER NOT NULL CHECK(sequence>=1),
    submitted_at_ms INTEGER NOT NULL CHECK(submitted_at_ms>=0),
    activity_id BLOB CHECK(activity_id IS NULL OR length(activity_id)=16),
    goal_id BLOB CHECK(goal_id IS NULL OR length(goal_id)=16),
    dag_revision INTEGER CHECK(dag_revision IS NULL OR dag_revision>=1),
    node_id BLOB CHECK(node_id IS NULL OR length(node_id)=16),
    provider_conversation_id TEXT CHECK(provider_conversation_id IS NULL OR length(CAST(provider_conversation_id AS BLOB)) BETWEEN 1 AND 512),
    provider_turn_id TEXT CHECK(provider_turn_id IS NULL OR length(CAST(provider_turn_id AS BLOB)) BETWEEN 1 AND 512),
    provider_source_kind INTEGER CHECK(provider_source_kind IS NULL OR provider_source_kind IN(0,1)),
    provider_source_profile_id BLOB CHECK(provider_source_profile_id IS NULL OR (length(provider_source_profile_id)=16 AND provider_source_profile_id != zeroblob(16))),
    provider_source_participation_id BLOB CHECK(provider_source_participation_id IS NULL OR (length(provider_source_participation_id)=16 AND provider_source_participation_id != zeroblob(16))),
    provider_source_label TEXT CHECK(provider_source_label IS NULL OR length(CAST(provider_source_label AS BLOB)) BETWEEN 1 AND 256),
    UNIQUE(workspace_id, sequence),
    UNIQUE(workspace_id, provider_conversation_id, provider_turn_id),
    CHECK((channel_kind=0 AND channel_participation_id IS NULL) OR (channel_kind=1 AND channel_participation_id IS NOT NULL)),
    CHECK((author_kind=0 AND author_participation_id IS NULL AND author_profile_id IS NULL AND author_participant_id IS NULL AND author_session_id IS NULL) OR (author_kind=1 AND author_participation_id IS NOT NULL AND author_profile_id IS NOT NULL AND author_participant_id IS NOT NULL AND author_session_id IS NOT NULL) OR (author_kind IN(2,3) AND author_participation_id IS NULL AND author_profile_id IS NULL AND author_participant_id IS NULL AND author_session_id IS NULL)),
    CHECK((origin_kind=0 AND provider_conversation_id IS NULL AND provider_turn_id IS NULL AND provider_source_kind IS NULL AND provider_source_profile_id IS NULL AND provider_source_participation_id IS NULL AND provider_source_label IS NULL) OR (origin_kind=1 AND provider_conversation_id IS NOT NULL AND provider_turn_id IS NOT NULL AND provider_source_kind IS NOT NULL)),
    CHECK((provider_source_kind=0 AND provider_source_profile_id IS NOT NULL AND provider_source_participation_id IS NOT NULL) OR (provider_source_kind=1 AND provider_source_profile_id IS NULL AND provider_source_participation_id IS NULL) OR provider_source_kind IS NULL),
    CHECK((goal_id IS NOT NULL) OR (dag_revision IS NULL AND node_id IS NULL)),
    CHECK(dag_revision IS NOT NULL OR node_id IS NULL)
);
";

const WORKSPACE_MESSAGES_SQL: &str = r"
CREATE TABLE ops_workspace_messages (
    message_id BLOB PRIMARY KEY NOT NULL CHECK(length(message_id)=16 AND message_id != zeroblob(16)),
    workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16),
    channel_kind INTEGER NOT NULL CHECK(channel_kind IN(0,1)),
    channel_participation_id BLOB CHECK(channel_participation_id IS NULL OR (length(channel_participation_id)=16 AND channel_participation_id != zeroblob(16))),
    author_kind INTEGER NOT NULL CHECK(author_kind IN(0,1,2,3)),
    author_participation_id BLOB CHECK(author_participation_id IS NULL OR (length(author_participation_id)=16 AND author_participation_id != zeroblob(16))),
    author_profile_id BLOB CHECK(author_profile_id IS NULL OR (length(author_profile_id)=16 AND author_profile_id != zeroblob(16))),
    author_participant_id BLOB CHECK(author_participant_id IS NULL OR (length(author_participant_id)=16 AND author_participant_id != zeroblob(16))),
    author_session_id BLOB CHECK(author_session_id IS NULL OR (length(author_session_id)=16 AND author_session_id != zeroblob(16))),
    author_display_name TEXT NOT NULL CHECK(length(CAST(author_display_name AS BLOB)) BETWEEN 1 AND 128),
    origin_kind INTEGER NOT NULL CHECK(origin_kind IN(0,1)),
    body TEXT NOT NULL CHECK(length(CAST(body AS BLOB)) BETWEEN 1 AND 16384),
    sequence INTEGER NOT NULL CHECK(sequence>=1),
    submitted_at_ms INTEGER NOT NULL CHECK(submitted_at_ms>=0),
    activity_id BLOB CHECK(activity_id IS NULL OR length(activity_id)=16),
    goal_id BLOB CHECK(goal_id IS NULL OR length(goal_id)=16),
    dag_revision INTEGER CHECK(dag_revision IS NULL OR dag_revision>=1),
    node_id BLOB CHECK(node_id IS NULL OR length(node_id)=16),
    provider_conversation_id TEXT CHECK(provider_conversation_id IS NULL OR length(CAST(provider_conversation_id AS BLOB)) BETWEEN 1 AND 512),
    provider_turn_id TEXT CHECK(provider_turn_id IS NULL OR length(CAST(provider_turn_id AS BLOB)) BETWEEN 1 AND 512),
    provider_source_kind INTEGER CHECK(provider_source_kind IS NULL OR provider_source_kind IN(0,1)),
    provider_source_profile_id BLOB CHECK(provider_source_profile_id IS NULL OR (length(provider_source_profile_id)=16 AND provider_source_profile_id != zeroblob(16))),
    provider_source_participation_id BLOB CHECK(provider_source_participation_id IS NULL OR (length(provider_source_participation_id)=16 AND provider_source_participation_id != zeroblob(16))),
    provider_source_label TEXT CHECK(provider_source_label IS NULL OR length(CAST(provider_source_label AS BLOB)) BETWEEN 1 AND 256),
    goal_created_at_ms INTEGER CHECK(goal_created_at_ms IS NULL OR (goal_id IS NOT NULL AND goal_created_at_ms>=0)),
    dag_base_goal_revision INTEGER CHECK(dag_base_goal_revision IS NULL OR (dag_revision IS NOT NULL AND dag_base_goal_revision>=1)),
    UNIQUE(workspace_id, sequence),
    UNIQUE(workspace_id, provider_conversation_id, provider_turn_id),
    CHECK((channel_kind=0 AND channel_participation_id IS NULL) OR (channel_kind=1 AND channel_participation_id IS NOT NULL)),
    CHECK((author_kind=0 AND author_participation_id IS NULL AND author_profile_id IS NULL AND author_participant_id IS NULL AND author_session_id IS NULL) OR (author_kind=1 AND author_participation_id IS NOT NULL AND author_profile_id IS NOT NULL AND author_participant_id IS NOT NULL AND author_session_id IS NOT NULL) OR (author_kind IN(2,3) AND author_participation_id IS NULL AND author_profile_id IS NULL AND author_participant_id IS NULL AND author_session_id IS NULL)),
    CHECK((origin_kind=0 AND provider_conversation_id IS NULL AND provider_turn_id IS NULL AND provider_source_kind IS NULL AND provider_source_profile_id IS NULL AND provider_source_participation_id IS NULL AND provider_source_label IS NULL) OR (origin_kind=1 AND provider_conversation_id IS NOT NULL AND provider_turn_id IS NOT NULL AND provider_source_kind IS NOT NULL)),
    CHECK((provider_source_kind=0 AND provider_source_profile_id IS NOT NULL AND provider_source_participation_id IS NOT NULL) OR (provider_source_kind=1 AND provider_source_profile_id IS NULL AND provider_source_participation_id IS NULL) OR provider_source_kind IS NULL),
    CHECK((goal_id IS NOT NULL) OR (dag_revision IS NULL AND node_id IS NULL)),
    CHECK(dag_revision IS NOT NULL OR node_id IS NULL)
);
";

const PROVIDER_OUTBOX_SQL_V35: &str = r"
CREATE TABLE ops_provider_message_outbox (
    message_id BLOB PRIMARY KEY NOT NULL REFERENCES ops_workspace_messages(message_id) CHECK(length(message_id)=16 AND message_id != zeroblob(16)),
    workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16),
    recipient_profile_id BLOB NOT NULL CHECK(length(recipient_profile_id)=16 AND recipient_profile_id != zeroblob(16)),
    recipient_participation_id BLOB NOT NULL CHECK(length(recipient_participation_id)=16 AND recipient_participation_id != zeroblob(16)),
    provider_family INTEGER NOT NULL CHECK(provider_family=1),
    runtime_session_id BLOB NOT NULL CHECK(length(runtime_session_id)=16 AND runtime_session_id != zeroblob(16)),
    provider_conversation_id TEXT NOT NULL CHECK(length(CAST(provider_conversation_id AS BLOB)) BETWEEN 1 AND 512),
    state INTEGER NOT NULL CHECK(state BETWEEN 1 AND 5),
    queued_at_ms INTEGER NOT NULL CHECK(queued_at_ms>=0),
    last_dispatch_at_ms INTEGER NOT NULL CHECK(last_dispatch_at_ms>=0),
    updated_at_ms INTEGER NOT NULL CHECK(updated_at_ms>=queued_at_ms),
    revision INTEGER NOT NULL CHECK(revision>=1),
    provider_turn_id TEXT CHECK(provider_turn_id IS NULL OR length(CAST(provider_turn_id AS BLOB)) BETWEEN 1 AND 512),
    provider_body TEXT CHECK(provider_body IS NULL OR length(CAST(provider_body AS BLOB)) BETWEEN 1 AND 16384),
    CHECK((state=3 AND provider_turn_id IS NOT NULL AND provider_body IS NOT NULL) OR (state!=3 AND provider_turn_id IS NULL AND provider_body IS NULL)),
    FOREIGN KEY(workspace_id, recipient_profile_id) REFERENCES ops_workspace_participations(workspace_id, agent_profile_id),
    FOREIGN KEY(workspace_id, recipient_participation_id) REFERENCES ops_workspace_participations(workspace_id, participation_id),
    FOREIGN KEY(provider_family,runtime_session_id,provider_conversation_id) REFERENCES ops_provider_message_routes(provider_family,runtime_session_id,provider_conversation_id)
);
";

const PROVIDER_OUTBOX_SQL: &str = r"
CREATE TABLE ops_provider_message_outbox (
    message_id BLOB PRIMARY KEY NOT NULL REFERENCES ops_workspace_messages(message_id) CHECK(length(message_id)=16 AND message_id != zeroblob(16)),
    workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16),
    recipient_profile_id BLOB NOT NULL CHECK(length(recipient_profile_id)=16 AND recipient_profile_id != zeroblob(16)),
    recipient_participation_id BLOB NOT NULL CHECK(length(recipient_participation_id)=16 AND recipient_participation_id != zeroblob(16)),
    provider_family INTEGER NOT NULL CHECK(provider_family=1),
    runtime_session_id BLOB NOT NULL CHECK(length(runtime_session_id)=16 AND runtime_session_id != zeroblob(16)),
    provider_conversation_id TEXT NOT NULL CHECK(length(CAST(provider_conversation_id AS BLOB)) BETWEEN 1 AND 512),
    state INTEGER NOT NULL CHECK(state BETWEEN 1 AND 5),
    queued_at_ms INTEGER NOT NULL CHECK(queued_at_ms>=0),
    last_dispatch_at_ms INTEGER NOT NULL CHECK(last_dispatch_at_ms>=0),
    updated_at_ms INTEGER NOT NULL CHECK(updated_at_ms>=queued_at_ms),
    revision INTEGER NOT NULL CHECK(revision>=1),
    provider_turn_id TEXT CHECK(provider_turn_id IS NULL OR length(CAST(provider_turn_id AS BLOB)) BETWEEN 1 AND 512),
    provider_body TEXT CHECK(provider_body IS NULL OR length(CAST(provider_body AS BLOB)) BETWEEN 1 AND 16384),
    node_route_witness BLOB CHECK(node_route_witness IS NULL OR (typeof(node_route_witness)='blob' AND length(node_route_witness) BETWEEN 1 AND 2048)),
    CHECK((state=3 AND provider_turn_id IS NOT NULL AND provider_body IS NOT NULL) OR (state!=3 AND provider_turn_id IS NULL AND provider_body IS NULL)),
    FOREIGN KEY(workspace_id, recipient_profile_id) REFERENCES ops_workspace_participations(workspace_id, agent_profile_id),
    FOREIGN KEY(workspace_id, recipient_participation_id) REFERENCES ops_workspace_participations(workspace_id, participation_id),
    FOREIGN KEY(provider_family,runtime_session_id,provider_conversation_id) REFERENCES ops_provider_message_routes(provider_family,runtime_session_id,provider_conversation_id)
);
";

const OUTBOX_TARGET_INDEX: &str = "CREATE INDEX ops_provider_message_outbox_queue_idx ON ops_provider_message_outbox(state,last_dispatch_at_ms,queued_at_ms,message_id)";

/// The public conversation channel.  Team/review channels are intentionally
/// outside this bounded packet and can reuse the private channel primitives.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageChannel {
    Workspace,
    Direct(WorkspaceParticipationId),
}

impl MessageChannel {
    fn kind(self) -> i64 {
        match self {
            Self::Workspace => 0,
            Self::Direct(_) => 1,
        }
    }

    fn participation_id(self) -> Option<WorkspaceParticipationId> {
        match self {
            Self::Workspace => None,
            Self::Direct(value) => Some(value),
        }
    }
}

/// Opaque Forge context attached to a public message.  Ingress validates that
/// each identifier belongs to the exact Workspace before calling persistence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct MessageContext {
    activity_id: Option<ActivityId>,
    goal_id: Option<GoalId>,
    goal_created_at: Option<TimestampMillis>,
    dag_revision: Option<DagRevisionNumber>,
    dag_base_goal_revision: Option<GoalRevision>,
    node_id: Option<DagNodeId>,
}

impl MessageContext {
    /// Construct a context while enforcing Forge's Goal-generation, DAG-base,
    /// DAG and node shape. Persistence does not assert that identifiers exist.
    ///
    /// # Errors
    /// Rejects nil identifiers and invalid Goal-generation/DAG-base/node combinations.
    pub fn new(
        activity_id: Option<ActivityId>,
        goal_id: Option<GoalId>,
        goal_created_at: Option<TimestampMillis>,
        dag_revision: Option<DagRevisionNumber>,
        dag_base_goal_revision: Option<GoalRevision>,
        node_id: Option<DagNodeId>,
    ) -> Result<Self, OperationsError> {
        if activity_id.is_some_and(|value| value.into_uuid().is_nil())
            || goal_id.is_some_and(|value| value.into_uuid().is_nil())
            || node_id.is_some_and(|value| value.into_uuid().is_nil())
        {
            return Err(OperationsError::Conflict("message context identity"));
        }
        if goal_id.is_none()
            && (goal_created_at.is_some() || dag_revision.is_some() || node_id.is_some())
        {
            return Err(OperationsError::Conflict(
                "message context generation or DAG requires Goal",
            ));
        }
        if dag_revision.is_none() && dag_base_goal_revision.is_some() {
            return Err(OperationsError::Conflict(
                "message context base Goal revision requires DAG",
            ));
        }
        if dag_revision.is_none() && node_id.is_some() {
            return Err(OperationsError::Conflict(
                "message context node requires DAG",
            ));
        }
        Ok(Self {
            activity_id,
            goal_id,
            goal_created_at,
            dag_revision,
            dag_base_goal_revision,
            node_id,
        })
    }

    #[must_use]
    pub const fn activity_id(self) -> Option<ActivityId> {
        self.activity_id
    }

    #[must_use]
    pub const fn goal_id(self) -> Option<GoalId> {
        self.goal_id
    }

    #[must_use]
    pub const fn goal_created_at(self) -> Option<TimestampMillis> {
        self.goal_created_at
    }

    #[must_use]
    pub const fn dag_revision(self) -> Option<DagRevisionNumber> {
        self.dag_revision
    }

    #[must_use]
    pub const fn dag_base_goal_revision(self) -> Option<GoalRevision> {
        self.dag_base_goal_revision
    }

    #[must_use]
    pub const fn node_id(self) -> Option<DagNodeId> {
        self.node_id
    }

    fn validate(self) -> Result<Self, OperationsError> {
        Self::new(
            self.activity_id,
            self.goal_id,
            self.goal_created_at,
            self.dag_revision,
            self.dag_base_goal_revision,
            self.node_id,
        )
    }
}

/// History selection: a channel's aggregate conversation, or an exact
/// Goal-generation/DAG-base node thread across former and current recipients. Neither grants work.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageReadScope {
    Channel(MessageChannel),
    Node(MessageContext),
}

impl MessageReadScope {
    fn validate(self) -> Result<Self, OperationsError> {
        if let Self::Node(context) = self {
            let context = context.validate()?;
            if context.activity_id().is_none()
                || context.goal_id().is_none()
                || context.goal_created_at().is_none()
                || context.dag_revision().is_none()
                || context.dag_base_goal_revision().is_none()
                || context.node_id().is_none()
            {
                return Err(OperationsError::Conflict("node message read scope"));
            }
        }
        Ok(self)
    }

    const fn channel(self) -> Option<MessageChannel> {
        match self {
            Self::Channel(channel) => Some(channel),
            Self::Node(_) => None,
        }
    }

    const fn context(self) -> Option<MessageContext> {
        match self {
            Self::Channel(_) => None,
            Self::Node(context) => Some(context),
        }
    }
}

/// Trusted native command for one internal public message append.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendWorkspaceMessageCommand {
    pub message_id: MessageId,
    pub workspace_id: WorkspaceId,
    pub channel: MessageChannel,
    pub actor: OperationalActor,
    pub body: String,
    pub submitted_at: TimestampMillis,
    pub context: MessageContext,
}

/// Trusted native command for one immutable provider outbox target.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnqueueProviderMessageCommand {
    pub message_id: MessageId,
    pub target: ProviderMessageTarget,
    pub queued_at: TimestampMillis,
    pub node_route_witness: Option<NodeMessageRouteWitness>,
}

/// Trusted native command for one incoming provider public turn.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordIncomingProviderMessageCommand {
    pub message_id: MessageId,
    pub workspace_id: WorkspaceId,
    pub channel: MessageChannel,
    pub target: ProviderMessageTarget,
    pub source: ProviderMessageSource,
    pub provider_turn_id: String,
    pub body: String,
    pub submitted_at: TimestampMillis,
    pub context: MessageContext,
}

/// Immutable source attribution for an incoming provider public turn.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProviderMessageSource {
    Known {
        profile_id: AgentProfileId,
        participation_id: WorkspaceParticipationId,
        display_name: String,
    },
    Unknown {
        label: String,
    },
}

impl ProviderMessageSource {
    /// Construct known attribution with a bounded display snapshot.
    ///
    /// # Errors
    /// Rejects an empty or oversized display snapshot.
    pub fn known(
        profile_id: AgentProfileId,
        participation_id: WorkspaceParticipationId,
        display_name: String,
    ) -> Result<Self, OperationsError> {
        validate_display_name(&display_name)?;
        Ok(Self::Known {
            profile_id,
            participation_id,
            display_name,
        })
    }

    /// Construct explicit unknown attribution.  Unknown is a durable state,
    /// never an implicit operator or participant identity.
    ///
    /// # Errors
    /// Rejects an empty or oversized source label.
    pub fn unknown(label: String) -> Result<Self, OperationsError> {
        validate_source_label(&label)?;
        Ok(Self::Unknown { label })
    }

    fn validate(&self) -> Result<(), OperationsError> {
        match self {
            Self::Known { display_name, .. } => validate_display_name(display_name),
            Self::Unknown { label } => validate_source_label(label),
        }
    }

    fn kind(&self) -> i64 {
        match self {
            Self::Known { .. } => 0,
            Self::Unknown { .. } => 1,
        }
    }

    fn profile_id(&self) -> Option<AgentProfileId> {
        match self {
            Self::Known { profile_id, .. } => Some(*profile_id),
            Self::Unknown { .. } => None,
        }
    }

    fn participation_id(&self) -> Option<WorkspaceParticipationId> {
        match self {
            Self::Known {
                participation_id, ..
            } => Some(*participation_id),
            Self::Unknown { .. } => None,
        }
    }

    fn display_name(&self) -> &str {
        match self {
            Self::Known { display_name, .. } => display_name,
            Self::Unknown { label } => label,
        }
    }
}

/// Immutable provider target captured before external I/O.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderMessageTarget {
    workspace_id: WorkspaceId,
    profile_id: AgentProfileId,
    participation_id: WorkspaceParticipationId,
    provider_family: AgentProfileProviderFamily,
    runtime_session_id: AgentProfileRuntimeSessionId,
    provider_conversation_id: String,
}

impl ProviderMessageTarget {
    /// Construct and validate an exact Workspace/Profile/Participation/
    /// RuntimeSession/provider-conversation target.
    ///
    /// # Errors
    /// Rejects a nil Workspace or an empty/oversized provider conversation ID.
    pub fn new(
        workspace_id: WorkspaceId,
        profile_id: AgentProfileId,
        participation_id: WorkspaceParticipationId,
        runtime_session_id: AgentProfileRuntimeSessionId,
        provider_conversation_id: String,
    ) -> Result<Self, OperationsError> {
        Self::new_with_provider_family(
            workspace_id,
            profile_id,
            participation_id,
            AgentProfileProviderFamily::ChatGpt,
            runtime_session_id,
            provider_conversation_id,
        )
    }

    /// Construct a target with an explicit provider family route key.
    ///
    /// # Errors
    /// Rejects a nil Workspace or an empty/oversized provider conversation ID.
    pub fn new_with_provider_family(
        workspace_id: WorkspaceId,
        profile_id: AgentProfileId,
        participation_id: WorkspaceParticipationId,
        provider_family: AgentProfileProviderFamily,
        runtime_session_id: AgentProfileRuntimeSessionId,
        provider_conversation_id: String,
    ) -> Result<Self, OperationsError> {
        validate_workspace_id(workspace_id)?;
        validate_provider_id(&provider_conversation_id, "provider conversation identity")?;
        Ok(Self {
            workspace_id,
            profile_id,
            participation_id,
            provider_family,
            runtime_session_id,
            provider_conversation_id,
        })
    }

    #[must_use]
    pub const fn workspace_id(&self) -> WorkspaceId {
        self.workspace_id
    }

    #[must_use]
    pub const fn profile_id(&self) -> AgentProfileId {
        self.profile_id
    }

    #[must_use]
    pub const fn participation_id(&self) -> WorkspaceParticipationId {
        self.participation_id
    }

    #[must_use]
    pub const fn provider_family(&self) -> AgentProfileProviderFamily {
        self.provider_family
    }

    #[must_use]
    pub const fn runtime_session_id(&self) -> AgentProfileRuntimeSessionId {
        self.runtime_session_id
    }

    #[must_use]
    pub fn provider_conversation_id(&self) -> &str {
        &self.provider_conversation_id
    }
}

/// Identity and public snapshot of a message author.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MessageAuthor {
    Operator,
    System,
    Participant {
        participation_id: WorkspaceParticipationId,
        profile_id: Uuid,
        participant_id: Uuid,
        session_id: AgentSessionId,
    },
    Provider {
        source: ProviderMessageSource,
    },
}

/// Whether a message was submitted internally or observed from a provider.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MessageOrigin {
    Internal,
    Provider {
        provider_conversation_id: String,
        provider_turn_id: String,
        source: ProviderMessageSource,
    },
}

/// Durable public message record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceMessage {
    message_id: MessageId,
    workspace_id: WorkspaceId,
    channel: MessageChannel,
    author: MessageAuthor,
    author_display_name: String,
    origin: MessageOrigin,
    body: String,
    sequence: u64,
    submitted_at: TimestampMillis,
    context: MessageContext,
}

impl WorkspaceMessage {
    #[must_use]
    pub const fn message_id(&self) -> Uuid {
        self.message_id
    }

    #[must_use]
    pub const fn workspace_id(&self) -> WorkspaceId {
        self.workspace_id
    }

    #[must_use]
    pub const fn channel(&self) -> MessageChannel {
        self.channel
    }

    #[must_use]
    pub fn author(&self) -> &MessageAuthor {
        &self.author
    }

    #[must_use]
    pub fn author_display_name(&self) -> &str {
        &self.author_display_name
    }

    #[must_use]
    pub fn origin(&self) -> &MessageOrigin {
        &self.origin
    }

    #[must_use]
    pub fn body(&self) -> &str {
        &self.body
    }

    #[must_use]
    pub const fn sequence(&self) -> u64 {
        self.sequence
    }

    #[must_use]
    pub const fn submitted_at(&self) -> TimestampMillis {
        self.submitted_at
    }

    #[must_use]
    pub const fn context(&self) -> MessageContext {
        self.context
    }
}

/// Format one stored internal message for the provider adapter.  The stored
/// Workspace message remains unchanged; only the provider-facing body carries
/// the exact opaque Forge context when present.
///
/// # Errors
/// Rejects the final UTF-8 or JSON-encoded provider body when it exceeds the
/// bounded bridge limits.
pub fn provider_message_text(message: &WorkspaceMessage) -> Result<String, OperationsError> {
    format_provider_message_text(message.workspace_id, message.body(), message.context)
}

/// Result of an idempotent message append.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MessageAppendOutcome {
    message: WorkspaceMessage,
    created: bool,
}

impl MessageAppendOutcome {
    #[must_use]
    pub fn message(&self) -> &WorkspaceMessage {
        &self.message
    }

    #[must_use]
    pub const fn created(&self) -> bool {
        self.created
    }
}

/// Stable bounded page of messages.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MessagePage {
    messages: Vec<WorkspaceMessage>,
    next_cursor: Option<u64>,
    has_more: bool,
}

impl MessagePage {
    #[must_use]
    pub fn messages(&self) -> &[WorkspaceMessage] {
        &self.messages
    }

    #[must_use]
    pub const fn next_cursor(&self) -> Option<u64> {
        self.next_cursor
    }

    #[must_use]
    pub const fn has_more(&self) -> bool {
        self.has_more
    }
}

/// Provider outbox lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderMessageState {
    Queued,
    EffectPending,
    Delivered,
    Failed,
    Unknown,
}

/// Durable provider receipt required to advance an outbox row to Delivered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderMessageDeliveryReceipt {
    target: ProviderMessageTarget,
    provider_turn_id: String,
    body: String,
}

impl ProviderMessageDeliveryReceipt {
    /// Construct a receipt from trusted provider readback.
    ///
    /// # Errors
    /// Rejects empty/oversized provider turn IDs and bodies outside the public
    /// message bound.
    pub fn new(
        target: ProviderMessageTarget,
        provider_turn_id: String,
        body: String,
    ) -> Result<Self, OperationsError> {
        validate_provider_id(&provider_turn_id, "provider turn identity")?;
        validate_body(&body)?;
        Ok(Self {
            target,
            provider_turn_id,
            body,
        })
    }

    #[must_use]
    pub fn target(&self) -> &ProviderMessageTarget {
        &self.target
    }

    #[must_use]
    pub fn provider_turn_id(&self) -> &str {
        &self.provider_turn_id
    }

    #[must_use]
    pub fn body(&self) -> &str {
        &self.body
    }
}

/// Durable provider outbox row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderMessageOutbox {
    message_id: MessageId,
    target: ProviderMessageTarget,
    state: ProviderMessageState,
    queued_at: TimestampMillis,
    last_dispatch_at: TimestampMillis,
    updated_at: TimestampMillis,
    revision: u64,
    receipt: Option<ProviderMessageDeliveryReceipt>,
    node_route_witness: Option<NodeMessageRouteWitness>,
}

impl ProviderMessageOutbox {
    #[must_use]
    pub const fn message_id(&self) -> Uuid {
        self.message_id
    }

    #[must_use]
    pub fn target(&self) -> &ProviderMessageTarget {
        &self.target
    }

    #[must_use]
    pub const fn state(&self) -> ProviderMessageState {
        self.state
    }

    #[must_use]
    pub const fn queued_at(&self) -> TimestampMillis {
        self.queued_at
    }

    #[must_use]
    pub const fn updated_at(&self) -> TimestampMillis {
        self.updated_at
    }

    #[must_use]
    pub const fn last_dispatch_at(&self) -> TimestampMillis {
        self.last_dispatch_at
    }

    #[must_use]
    pub const fn revision(&self) -> u64 {
        self.revision
    }

    #[must_use]
    pub fn receipt(&self) -> Option<&ProviderMessageDeliveryReceipt> {
        self.receipt.as_ref()
    }

    #[must_use]
    pub fn node_route_witness(&self) -> Option<&NodeMessageRouteWitness> {
        self.node_route_witness.as_ref()
    }
}

/// Result of idempotently queueing an internal message for provider delivery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderMessageQueueOutcome {
    outbox: ProviderMessageOutbox,
    created: bool,
}

impl ProviderMessageQueueOutcome {
    #[must_use]
    pub fn outbox(&self) -> &ProviderMessageOutbox {
        &self.outbox
    }

    #[must_use]
    pub const fn created(&self) -> bool {
        self.created
    }
}

/// Provider bridge action.  `Execute` is released by one exact CAS only;
/// `Inspect` is the sole action after a pending or unknown outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderMessageDispatchAction {
    Execute,
    Inspect,
}

/// Opaque dispatch fence returned to the provider adapter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderMessageDispatch {
    message_id: MessageId,
    revision: u64,
    action: ProviderMessageDispatchAction,
    target: ProviderMessageTarget,
}

impl ProviderMessageDispatch {
    #[must_use]
    pub const fn message_id(&self) -> Uuid {
        self.message_id
    }

    #[must_use]
    pub const fn revision(&self) -> u64 {
        self.revision
    }

    #[must_use]
    pub const fn action(&self) -> ProviderMessageDispatchAction {
        self.action
    }

    #[must_use]
    pub fn target(&self) -> &ProviderMessageTarget {
        &self.target
    }
}

/// Dispatch and durable readback together.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderMessageDispatchOutcome {
    outbox: ProviderMessageOutbox,
    dispatch: ProviderMessageDispatch,
}

impl ProviderMessageDispatchOutcome {
    #[must_use]
    pub fn outbox(&self) -> &ProviderMessageOutbox {
        &self.outbox
    }

    #[must_use]
    pub fn dispatch(&self) -> &ProviderMessageDispatch {
        &self.dispatch
    }
}

/// Result of an incoming provider turn deduplication attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IncomingProviderMessageOutcome {
    message: WorkspaceMessage,
    created: bool,
}

impl IncomingProviderMessageOutcome {
    #[must_use]
    pub fn message(&self) -> &WorkspaceMessage {
        &self.message
    }

    #[must_use]
    pub const fn created(&self) -> bool {
        self.created
    }
}

impl OperationsCatalog {
    /// Append an internal public Workspace message under a stable caller ID.
    ///
    /// Actor resolution and exact Workspace/import checks happen inside the
    /// immediate write transaction.  Exact replay returns the original row;
    /// changed actor, scope, body, context, or timestamp is rejected.
    ///
    /// # Errors
    /// Rejects unverified Workspaces, forged actors, malformed messages and
    /// changed replays, or returns durable SQLite failures.
    pub fn append_workspace_message(
        &mut self,
        command: AppendWorkspaceMessageCommand,
    ) -> Result<MessageAppendOutcome, OperationsError> {
        let message_id = command.message_id;
        let workspace_id = command.workspace_id;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let outcome =
            append_workspace_message_in_transaction(&transaction, self.destination, command)?;
        transaction
            .commit()
            .map_err(|source| operation_commit_unconfirmed(workspace_id, message_id, source))?;
        Ok(outcome)
    }

    /// Append an internal message and durably enqueue its immutable provider
    /// target and optional observed node route in one immediate SQLite transaction.
    /// Replay compares the original witness before mutable route checks. A witness
    /// records the trusted ingress observation; it does not itself grant authority.
    ///
    /// # Errors
    /// Rejects malformed messages, unverified Workspaces, invalid recipients,
    /// changed replays, route conflicts, bounded provider text, or SQLite
    /// failures. Any failure rolls back both the append and queue.
    pub fn append_and_enqueue_provider_message(
        &mut self,
        command: AppendWorkspaceMessageCommand,
        target: ProviderMessageTarget,
        node_route_witness: Option<NodeMessageRouteWitness>,
    ) -> Result<ProviderMessageQueueOutcome, OperationsError> {
        let message_id = command.message_id;
        let workspace_id = command.workspace_id;
        let queued_at = command.submitted_at;
        let context = command.context.validate()?;
        format_provider_message_text(workspace_id, &command.body, context)?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        require_verified_workspace(&transaction, workspace_id, self.destination)?;
        if let Some(witness) = &node_route_witness
            && !witness.matches_actor(command.actor)
        {
            return Err(OperationsError::Conflict("node message sender replay"));
        }
        if let Some(existing_outbox) = load_outbox(&transaction, message_id)? {
            ensure_target(&existing_outbox.target, &target)?;
            if existing_outbox.node_route_witness != node_route_witness {
                return Err(OperationsError::Conflict("provider node route replay"));
            }
            let existing_message = load_message(&transaction, message_id)?
                .ok_or(OperationsError::Corrupt("provider message source missing"))?;
            ensure_replay_scope(&existing_message, workspace_id, command.channel)?;
            let same_actor = match (command.actor, &existing_message.author) {
                (OperationalActor::LocalOperator, MessageAuthor::Operator) => true,
                (
                    OperationalActor::Participant { session_id, .. },
                    MessageAuthor::Participant {
                        session_id: original,
                        ..
                    },
                ) => session_id == *original,
                _ => false,
            };
            if !same_actor
                || !matches!(existing_message.origin, MessageOrigin::Internal)
                || existing_message.body != command.body
                || existing_message.context != command.context
                || existing_message.submitted_at != command.submitted_at
                || node_route_witness
                    .as_ref()
                    .is_some_and(|witness| !witness.matches_author(&existing_message.author))
            {
                return Err(OperationsError::Conflict(
                    "workspace message replay changed",
                ));
            }
            transaction.commit()?;
            return Ok(ProviderMessageQueueOutcome {
                outbox: existing_outbox,
                created: false,
            });
        }
        append_workspace_message_in_transaction(&transaction, self.destination, command)?;
        let outcome = enqueue_provider_message_in_transaction(
            &transaction,
            self.destination,
            EnqueueProviderMessageCommand {
                message_id,
                target,
                queued_at,
                node_route_witness,
            },
        )?;
        transaction
            .commit()
            .map_err(|source| operation_commit_unconfirmed(workspace_id, message_id, source))?;
        Ok(outcome)
    }

    /// Read a stable sequence page for a channel or an exact node thread.
    /// Node scope includes messages delivered to prior and current recipients.
    ///
    /// # Errors
    /// Rejects an unverified Workspace, invalid channel or page bound, or
    /// returns malformed stored rows and SQLite failures.
    pub fn read_workspace_messages(
        &self,
        workspace_id: WorkspaceId,
        scope: MessageReadScope,
        after_sequence: Option<u64>,
        limit: usize,
    ) -> Result<MessagePage, OperationsError> {
        validate_page_limit(limit)?;
        let scope = scope.validate()?;
        let context = scope.context();
        let channel = scope.channel();
        let transaction = self.connection.unchecked_transaction()?;
        require_verified_workspace(&transaction, workspace_id, self.destination)?;
        if let Some(channel) = channel {
            validate_channel(&transaction, workspace_id, channel)?;
        }
        let requested_after = match after_sequence {
            Some(value) => signed_counter(value)?,
            None => 0,
        };
        let sql_limit =
            i64::try_from(limit + 1).map_err(|_| OperationsError::Bounds("message page limit"))?;
        let channel_participation = channel
            .and_then(MessageChannel::participation_id)
            .map(|value| value.into_uuid().into_bytes().to_vec());
        let context_activity = context
            .and_then(MessageContext::activity_id)
            .map(|id| id.into_uuid().into_bytes().to_vec());
        let context_goal = context
            .and_then(MessageContext::goal_id)
            .map(|id| id.into_uuid().into_bytes().to_vec());
        let context_dag_revision = context
            .and_then(MessageContext::dag_revision)
            .map(DagRevisionNumber::get);
        let context_node = context
            .and_then(MessageContext::node_id)
            .map(|id| id.into_uuid().into_bytes().to_vec());
        let context_goal_created_at = context
            .and_then(MessageContext::goal_created_at)
            .map(TimestampMillis::get);
        let context_dag_base_goal_revision = context
            .and_then(MessageContext::dag_base_goal_revision)
            .map(GoalRevision::get);
        let mut messages = {
            let mut statement = transaction.prepare(
                "SELECT substr(message_id,1,17),substr(workspace_id,1,17),channel_kind,substr(channel_participation_id,1,17),author_kind,substr(author_participation_id,1,17),substr(author_profile_id,1,17),substr(author_participant_id,1,17),substr(author_session_id,1,17),substr(CAST(author_display_name AS BLOB),1,129),origin_kind,substr(CAST(body AS BLOB),1,16385),sequence,submitted_at_ms,substr(activity_id,1,17),substr(goal_id,1,17),dag_revision,substr(node_id,1,17),substr(CAST(provider_conversation_id AS BLOB),1,513),substr(CAST(provider_turn_id AS BLOB),1,513),provider_source_kind,substr(provider_source_profile_id,1,17),substr(provider_source_participation_id,1,17),substr(CAST(provider_source_label AS BLOB),1,257),goal_created_at_ms,dag_base_goal_revision FROM ops_workspace_messages WHERE workspace_id=?1 AND ((?2 IS NOT NULL AND channel_kind=?2 AND channel_participation_id IS ?3) OR (?2 IS NULL AND activity_id IS ?4 AND goal_id IS ?5 AND dag_revision IS ?6 AND node_id IS ?7 AND goal_created_at_ms IS ?8 AND dag_base_goal_revision IS ?9)) AND sequence>?10 ORDER BY sequence ASC LIMIT ?11",
            )?;
            let rows = statement.query_map(
                params![
                    workspace_id.into_uuid().as_bytes(),
                    channel.map(MessageChannel::kind),
                    channel_participation,
                    context_activity,
                    context_goal,
                    context_dag_revision,
                    context_node,
                    context_goal_created_at,
                    context_dag_base_goal_revision,
                    requested_after,
                    sql_limit,
                ],
                decode_message_row,
            )?;
            let mut values = Vec::with_capacity(limit);
            for row in rows {
                values.push(row?);
            }
            values
        };
        let has_more = messages.len() > limit;
        if has_more {
            messages.pop();
        }
        let next_cursor = has_more
            .then(|| messages.last().map(WorkspaceMessage::sequence))
            .flatten();
        catalog_commit_read(transaction, self.destination)?;
        Ok(MessagePage {
            messages,
            next_cursor,
            has_more,
        })
    }

    /// Read the latest bounded messages for a channel or exact node thread.
    /// Scope and an optional exclusive older cursor are applied before limiting.
    /// Returned messages are ascending by durable sequence. When `has_more` is true, `next_cursor` is the oldest sequence retained in this page;
    /// it is the exclusive upper-bound cursor for an older-page reader.
    /// `before_sequence` must be positive and fit the persisted signed sequence range.
    ///
    /// This read does not change the existing `after_sequence` semantics of
    /// [`Self::read_workspace_messages`].
    ///
    /// # Errors
    /// Rejects an unverified Workspace, invalid channel or page bound, or
    /// returns malformed stored rows and SQLite failures.
    pub fn workspace_messages_latest(
        &self,
        workspace_id: WorkspaceId,
        scope: MessageReadScope,
        before_sequence: Option<u64>,
        limit: usize,
    ) -> Result<MessagePage, OperationsError> {
        validate_page_limit(limit)?;
        let scope = scope.validate()?;
        let before_sequence = before_sequence
            .map(|cursor| {
                if cursor == 0 {
                    return Err(OperationsError::Conflict("message latest cursor"));
                }
                signed_counter(cursor)
            })
            .transpose()?;
        let context = scope.context();
        let channel = scope.channel();
        let transaction = self.connection.unchecked_transaction()?;
        require_verified_workspace(&transaction, workspace_id, self.destination)?;
        if let Some(channel) = channel {
            validate_channel(&transaction, workspace_id, channel)?;
        }
        let sql_limit =
            i64::try_from(limit + 1).map_err(|_| OperationsError::Bounds("message page limit"))?;
        let channel_participation = channel
            .and_then(MessageChannel::participation_id)
            .map(|value| value.into_uuid().into_bytes().to_vec());
        let context_activity = context
            .and_then(MessageContext::activity_id)
            .map(|id| id.into_uuid().into_bytes().to_vec());
        let context_goal = context
            .and_then(MessageContext::goal_id)
            .map(|id| id.into_uuid().into_bytes().to_vec());
        let context_dag_revision = context
            .and_then(MessageContext::dag_revision)
            .map(DagRevisionNumber::get);
        let context_node = context
            .and_then(MessageContext::node_id)
            .map(|id| id.into_uuid().into_bytes().to_vec());
        let context_goal_created_at = context
            .and_then(MessageContext::goal_created_at)
            .map(TimestampMillis::get);
        let context_dag_base_goal_revision = context
            .and_then(MessageContext::dag_base_goal_revision)
            .map(GoalRevision::get);
        let mut messages = {
            let mut statement = transaction.prepare(
                "SELECT substr(message_id,1,17),substr(workspace_id,1,17),channel_kind,substr(channel_participation_id,1,17),author_kind,substr(author_participation_id,1,17),substr(author_profile_id,1,17),substr(author_participant_id,1,17),substr(author_session_id,1,17),substr(CAST(author_display_name AS BLOB),1,129),origin_kind,substr(CAST(body AS BLOB),1,16385),sequence,submitted_at_ms,substr(activity_id,1,17),substr(goal_id,1,17),dag_revision,substr(node_id,1,17),substr(CAST(provider_conversation_id AS BLOB),1,513),substr(CAST(provider_turn_id AS BLOB),1,513),provider_source_kind,substr(provider_source_profile_id,1,17),substr(provider_source_participation_id,1,17),substr(CAST(provider_source_label AS BLOB),1,257),goal_created_at_ms,dag_base_goal_revision FROM ops_workspace_messages WHERE workspace_id=?1 AND ((?2 IS NOT NULL AND channel_kind=?2 AND channel_participation_id IS ?3) OR (?2 IS NULL AND activity_id IS ?4 AND goal_id IS ?5 AND dag_revision IS ?6 AND node_id IS ?7 AND goal_created_at_ms IS ?8 AND dag_base_goal_revision IS ?9)) AND (?10 IS NULL OR sequence<?10) ORDER BY sequence DESC LIMIT ?11",
            )?;
            let rows = statement.query_map(
                params![
                    workspace_id.into_uuid().as_bytes(),
                    channel.map(MessageChannel::kind),
                    channel_participation,
                    context_activity,
                    context_goal,
                    context_dag_revision,
                    context_node,
                    context_goal_created_at,
                    context_dag_base_goal_revision,
                    before_sequence,
                    sql_limit,
                ],
                decode_message_row,
            )?;
            let mut values = Vec::with_capacity(limit + 1);
            for row in rows {
                values.push(row?);
            }
            values
        };
        let has_more = messages.len() > limit;
        if has_more {
            messages.pop();
        }
        messages.reverse();
        let next_cursor = has_more
            .then(|| messages.first().map(WorkspaceMessage::sequence))
            .flatten();
        catalog_commit_read(transaction, self.destination)?;
        Ok(MessagePage {
            messages,
            next_cursor,
            has_more,
        })
    }

    /// Read one exact message by its caller-stable identity.
    ///
    /// # Errors
    /// Rejects a nil identity, an unverified stored Workspace, malformed rows,
    /// or SQLite failures.
    pub fn workspace_message(
        &self,
        message_id: MessageId,
    ) -> Result<Option<WorkspaceMessage>, OperationsError> {
        validate_message_id(message_id)?;
        let transaction = self.connection.unchecked_transaction()?;
        let message = load_message(&transaction, message_id)?;
        if let Some(message) = &message {
            require_verified_workspace(&transaction, message.workspace_id, self.destination)?;
        }
        catalog_commit_read(transaction, self.destination)?;
        Ok(message)
    }

    /// Queue an already persisted internal message for one immutable provider
    /// target.  Queueing returns `Queued`; it is never a send acknowledgement.
    ///
    /// # Errors
    /// Rejects an unverified Workspace, wrong recipient, provider route reuse,
    /// non-internal source, changed target or SQLite failures.
    pub fn enqueue_provider_message(
        &mut self,
        command: EnqueueProviderMessageCommand,
    ) -> Result<ProviderMessageQueueOutcome, OperationsError> {
        let message_id = command.message_id;
        let workspace_id = command.target.workspace_id;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let outcome =
            enqueue_provider_message_in_transaction(&transaction, self.destination, command)?;
        transaction
            .commit()
            .map_err(|source| operation_commit_unconfirmed(workspace_id, message_id, source))?;
        Ok(outcome)
    }

    /// Read an outbox row by stable message identity.
    ///
    /// # Errors
    /// Rejects a nil identity, an unverified stored Workspace, malformed rows,
    /// or SQLite failures.
    pub fn provider_message_outbox(
        &self,
        message_id: MessageId,
    ) -> Result<Option<ProviderMessageOutbox>, OperationsError> {
        validate_message_id(message_id)?;
        let transaction = self.connection.unchecked_transaction()?;
        let outbox = load_outbox(&transaction, message_id)?;
        if let Some(outbox) = &outbox {
            require_verified_workspace(&transaction, outbox.target.workspace_id, self.destination)?;
        }
        catalog_commit_read(transaction, self.destination)?;
        Ok(outbox)
    }

    /// Release one queued provider Execute permit or inspect an existing
    /// pending/unknown operation.  The CAS makes Execute at-most-once per row.
    ///
    /// # Errors
    /// Rejects stale clocks, unverified routes, malformed rows or SQLite
    /// failures; unknown commits remain inspect-only.
    pub fn next_provider_message_dispatch(
        &mut self,
        observed_at: TimestampMillis,
    ) -> Result<Option<ProviderMessageDispatchOutcome>, OperationsError> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let Some(message_id) = eligible_outbox_message(&transaction, None)? else {
            transaction.commit()?;
            return Ok(None);
        };
        let existing = load_outbox(&transaction, message_id)?
            .ok_or(OperationsError::Corrupt("provider queued readback"))?;
        require_verified_workspace(&transaction, existing.target.workspace_id, self.destination)?;
        if existing.state == ProviderMessageState::Queued
            && governance::expire_queued_invitation(&transaction, message_id, observed_at)?
        {
            let outbox = load_outbox(&transaction, message_id)?
                .ok_or(OperationsError::Corrupt("expired invitation readback"))?;
            let dispatch = dispatch_for(&outbox, ProviderMessageDispatchAction::Inspect);
            transaction.commit().map_err(|source| {
                operation_commit_unconfirmed(outbox.target.workspace_id, message_id, source)
            })?;
            return Ok(Some(ProviderMessageDispatchOutcome { outbox, dispatch }));
        }
        validate_recipient(&transaction, &existing.target)?;
        let action = if existing.state == ProviderMessageState::Queued {
            if observed_at < existing.updated_at {
                return Err(OperationsError::Conflict("provider dispatch clock"));
            }
            let revision = next_revision(existing.revision)?;
            let changed = transaction.execute(
                "UPDATE ops_provider_message_outbox SET state=2,updated_at_ms=?1,last_dispatch_at_ms=MAX(last_dispatch_at_ms,?1),revision=?2 WHERE message_id=?3 AND revision=?4 AND state=1",
                params![
                    observed_at.get(),
                    signed_revision(revision)?,
                    message_id.as_bytes(),
                    signed_revision(existing.revision)?,
                ],
            )?;
            if changed != 1 {
                return Err(OperationsError::Conflict("provider dispatch CAS"));
            }
            ProviderMessageDispatchAction::Execute
        } else {
            transaction.execute(
                "UPDATE ops_provider_message_outbox SET last_dispatch_at_ms=MAX(last_dispatch_at_ms,?1) WHERE message_id=?2 AND state IN(2,5)",
                params![observed_at.get(), message_id.as_bytes()],
            )?;
            ProviderMessageDispatchAction::Inspect
        };
        let outbox = load_outbox(&transaction, message_id)?
            .ok_or(OperationsError::Corrupt("provider dispatch update"))?;
        let dispatch = dispatch_for(&outbox, action);
        transaction.commit().map_err(|source| {
            operation_commit_unconfirmed(outbox.target.workspace_id, message_id, source)
        })?;
        Ok(Some(ProviderMessageDispatchOutcome { outbox, dispatch }))
    }

    /// Return the dispatch decision for one exact message identity.
    ///
    /// # Errors
    /// Rejects a nil identity, stale clocks, unverified routes, malformed rows
    /// or SQLite failures.
    pub fn provider_message_dispatch(
        &mut self,
        message_id: MessageId,
        observed_at: TimestampMillis,
    ) -> Result<Option<ProviderMessageDispatchOutcome>, OperationsError> {
        validate_message_id(message_id)?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let Some(existing) = load_outbox(&transaction, message_id)? else {
            transaction.commit()?;
            return Ok(None);
        };
        if existing.state == ProviderMessageState::Queued
            && eligible_outbox_message(&transaction, Some(message_id))? != Some(message_id)
        {
            transaction.commit()?;
            return Ok(None);
        }
        if existing.state == ProviderMessageState::Queued
            && governance::expire_queued_invitation(&transaction, message_id, observed_at)?
        {
            let outbox = load_outbox(&transaction, message_id)?
                .ok_or(OperationsError::Corrupt("expired invitation readback"))?;
            let dispatch = dispatch_for(&outbox, ProviderMessageDispatchAction::Inspect);
            transaction.commit().map_err(|source| {
                operation_commit_unconfirmed(outbox.target.workspace_id, message_id, source)
            })?;
            return Ok(Some(ProviderMessageDispatchOutcome { outbox, dispatch }));
        }
        let action = match existing.state {
            ProviderMessageState::Queued => {
                require_verified_workspace(
                    &transaction,
                    existing.target.workspace_id,
                    self.destination,
                )?;
                validate_recipient(&transaction, &existing.target)?;
                if observed_at < existing.updated_at {
                    return Err(OperationsError::Conflict("provider dispatch clock"));
                }
                let revision = next_revision(existing.revision)?;
                let changed = transaction.execute(
                    "UPDATE ops_provider_message_outbox SET state=2,updated_at_ms=?1,last_dispatch_at_ms=MAX(last_dispatch_at_ms,?1),revision=?2 WHERE message_id=?3 AND revision=?4 AND state=1",
                    params![
                        observed_at.get(),
                        signed_revision(revision)?,
                        message_id.as_bytes(),
                        signed_revision(existing.revision)?,
                    ],
                )?;
                if changed != 1 {
                    return Err(OperationsError::Conflict("provider dispatch CAS"));
                }
                ProviderMessageDispatchAction::Execute
            }
            ProviderMessageState::EffectPending | ProviderMessageState::Unknown => {
                require_verified_workspace(
                    &transaction,
                    existing.target.workspace_id,
                    self.destination,
                )?;
                validate_recipient(&transaction, &existing.target)?;
                transaction.execute(
                    "UPDATE ops_provider_message_outbox SET last_dispatch_at_ms=MAX(last_dispatch_at_ms,?1) WHERE message_id=?2 AND state IN(2,5)",
                    params![observed_at.get(), message_id.as_bytes()],
                )?;
                ProviderMessageDispatchAction::Inspect
            }
            ProviderMessageState::Delivered | ProviderMessageState::Failed => {
                transaction.commit()?;
                return Ok(None);
            }
        };
        let outbox = load_outbox(&transaction, message_id)?
            .ok_or(OperationsError::Corrupt("provider dispatch readback"))?;
        let dispatch = dispatch_for(&outbox, action);
        transaction.commit().map_err(|source| {
            operation_commit_unconfirmed(outbox.target.workspace_id, message_id, source)
        })?;
        Ok(Some(ProviderMessageDispatchOutcome { outbox, dispatch }))
    }

    /// Persist an uncertain provider result.  Subsequent dispatch is Inspect.
    ///
    /// # Errors
    /// Rejects a changed target, stale revision/time, invalid lifecycle or
    /// SQLite failures.
    pub fn mark_provider_message_effect_unknown(
        &mut self,
        message_id: MessageId,
        expected_revision: u64,
        target: &ProviderMessageTarget,
        observed_at: TimestampMillis,
    ) -> Result<ProviderMessageOutbox, OperationsError> {
        self.transition_provider_message(
            message_id,
            expected_revision,
            target,
            observed_at,
            &[ProviderMessageState::EffectPending],
            ProviderMessageState::Unknown,
        )
    }

    /// Persist exact provider public user-turn identity/body and mark the
    /// outbox delivered.  Target and revision are rechecked in the CAS.
    ///
    /// # Errors
    /// Rejects changed target, body, turn identity, stale revision/time,
    /// invalid lifecycle, or SQLite failures.
    pub fn confirm_provider_message_delivery(
        &mut self,
        message_id: MessageId,
        expected_revision: u64,
        receipt: &ProviderMessageDeliveryReceipt,
        observed_at: TimestampMillis,
    ) -> Result<ProviderMessageOutbox, OperationsError> {
        validate_message_id(message_id)?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let existing = load_outbox(&transaction, message_id)?.ok_or(OperationsError::Conflict(
            "provider receipt message missing",
        ))?;
        ensure_target(&existing.target, receipt.target())?;
        require_verified_workspace(&transaction, existing.target.workspace_id, self.destination)?;
        validate_recipient(&transaction, &existing.target)?;
        let message = load_message(&transaction, message_id)?
            .ok_or(OperationsError::Corrupt("provider receipt source message"))?;
        let provider_body = provider_message_text(&message)?;
        if provider_body != receipt.body {
            return Err(OperationsError::Conflict("provider receipt body"));
        }
        if existing.state == ProviderMessageState::Delivered {
            if existing.receipt.as_ref() == Some(receipt) {
                transaction.commit()?;
                return Ok(existing);
            }
            return Err(OperationsError::Conflict("provider receipt changed"));
        }
        if !matches!(
            existing.state,
            ProviderMessageState::EffectPending | ProviderMessageState::Unknown
        ) {
            return Err(OperationsError::Conflict("provider receipt state"));
        }
        validate_revision_and_time(&existing, expected_revision, observed_at)?;
        let revision = next_revision(existing.revision)?;
        let changed = transaction.execute(
            "UPDATE ops_provider_message_outbox SET state=3,updated_at_ms=?1,revision=?2,provider_turn_id=?3,provider_body=?4 WHERE message_id=?5 AND revision=?6 AND state IN(2,5)",
            params![
                observed_at.get(),
                signed_revision(revision)?,
                receipt.provider_turn_id(),
                receipt.body(),
                message_id.as_bytes(),
                signed_revision(expected_revision)?,
            ],
        )?;
        if changed != 1 {
            return Err(OperationsError::Conflict("provider receipt CAS"));
        }
        let outbox = load_outbox(&transaction, message_id)?
            .ok_or(OperationsError::Corrupt("provider receipt readback"))?;
        transaction.commit().map_err(|source| {
            operation_commit_unconfirmed(receipt.target().workspace_id, message_id, source)
        })?;
        Ok(outbox)
    }

    /// Record a definite provider failure after the adapter has inspected the
    /// exact target.  Failed rows remain durable and are never auto-requeued.
    ///
    /// # Errors
    /// Rejects a changed target, stale revision/time, invalid lifecycle or
    /// SQLite failures.
    pub fn fail_provider_message_delivery(
        &mut self,
        message_id: MessageId,
        expected_revision: u64,
        target: &ProviderMessageTarget,
        observed_at: TimestampMillis,
    ) -> Result<ProviderMessageOutbox, OperationsError> {
        self.transition_provider_message(
            message_id,
            expected_revision,
            target,
            observed_at,
            &[
                ProviderMessageState::EffectPending,
                ProviderMessageState::Unknown,
            ],
            ProviderMessageState::Failed,
        )
    }

    /// Deduplicate one provider public turn by exact conversation + turn
    /// identity while preserving source attribution and Workspace sequence.
    ///
    /// # Errors
    /// Rejects unverified Workspaces, wrong source/recipient, route conflicts,
    /// changed replays, malformed input or SQLite failures.
    #[allow(clippy::too_many_lines)]
    pub fn record_incoming_provider_message(
        &mut self,
        command: RecordIncomingProviderMessageCommand,
    ) -> Result<IncomingProviderMessageOutcome, OperationsError> {
        let RecordIncomingProviderMessageCommand {
            message_id,
            workspace_id,
            channel,
            target,
            source,
            provider_turn_id,
            body,
            submitted_at,
            context,
        } = command;
        validate_message_id(message_id)?;
        validate_provider_id(&provider_turn_id, "provider turn identity")?;
        validate_body(&body)?;
        source.validate()?;
        let context = context.validate()?;
        if target.workspace_id != workspace_id {
            return Err(OperationsError::Conflict("incoming provider Workspace"));
        }
        if target.provider_conversation_id.is_empty() {
            return Err(OperationsError::Bounds("provider conversation identity"));
        }
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        require_verified_workspace(&transaction, workspace_id, self.destination)?;
        validate_channel(&transaction, workspace_id, channel)?;
        validate_recipient(&transaction, &target)?;
        validate_provider_source(&transaction, workspace_id, &source)?;
        ensure_incoming_source_target(&target, &source)?;
        claim_provider_route(&transaction, &target, submitted_at)?;
        if let Some((existing, confirmed_body)) =
            load_delivered_provider_turn(&transaction, &target, &provider_turn_id)?
        {
            if confirmed_body != body {
                return Err(OperationsError::Conflict("provider turn body changed"));
            }
            ensure_replay_scope(&existing, workspace_id, channel)?;
            transaction
                .commit()
                .map_err(|error| operation_commit_unconfirmed(workspace_id, message_id, error))?;
            return Ok(IncomingProviderMessageOutcome {
                message: existing,
                created: false,
            });
        }
        if let Some(existing) = load_message(&transaction, message_id)? {
            ensure_replay_scope(&existing, workspace_id, channel)?;
            ensure_replay_provider(
                &existing,
                &target.provider_conversation_id,
                &provider_turn_id,
                &source,
                &body,
                submitted_at,
                context,
            )?;
            transaction
                .commit()
                .map_err(|error| operation_commit_unconfirmed(workspace_id, message_id, error))?;
            return Ok(IncomingProviderMessageOutcome {
                message: existing,
                created: false,
            });
        }
        if let Some(existing) = load_provider_turn(
            &transaction,
            workspace_id,
            &target.provider_conversation_id,
            &provider_turn_id,
        )? {
            if existing.message_id != message_id {
                return Err(OperationsError::Conflict(
                    "incoming provider message identity changed",
                ));
            }
            ensure_replay_scope(&existing, workspace_id, channel)?;
            ensure_replay_provider(
                &existing,
                &target.provider_conversation_id,
                &provider_turn_id,
                &source,
                &body,
                submitted_at,
                context,
            )?;
            transaction
                .commit()
                .map_err(|error| operation_commit_unconfirmed(workspace_id, message_id, error))?;
            return Ok(IncomingProviderMessageOutcome {
                message: existing,
                created: false,
            });
        }
        let sequence = next_sequence(&transaction, workspace_id)?;
        insert_provider_message(
            &transaction,
            message_id,
            workspace_id,
            channel,
            &source,
            &target.provider_conversation_id,
            &provider_turn_id,
            &body,
            sequence,
            submitted_at,
            context,
        )?;
        let message = load_message(&transaction, message_id)?
            .ok_or(OperationsError::Corrupt("incoming provider insert"))?;
        transaction
            .commit()
            .map_err(|error| operation_commit_unconfirmed(workspace_id, message_id, error))?;
        Ok(IncomingProviderMessageOutcome {
            message,
            created: true,
        })
    }

    fn transition_provider_message(
        &mut self,
        message_id: MessageId,
        expected_revision: u64,
        target: &ProviderMessageTarget,
        observed_at: TimestampMillis,
        expected_states: &[ProviderMessageState],
        next_state: ProviderMessageState,
    ) -> Result<ProviderMessageOutbox, OperationsError> {
        validate_message_id(message_id)?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let existing = load_outbox(&transaction, message_id)?.ok_or(OperationsError::Conflict(
            "provider transition message missing",
        ))?;
        ensure_target(&existing.target, target)?;
        require_verified_workspace(&transaction, existing.target.workspace_id, self.destination)?;
        validate_recipient(&transaction, &existing.target)?;
        if existing.state == next_state {
            transaction.commit()?;
            return Ok(existing);
        }
        if !expected_states.contains(&existing.state) {
            return Err(OperationsError::Conflict("provider transition state"));
        }
        validate_revision_and_time(&existing, expected_revision, observed_at)?;
        let revision = next_revision(existing.revision)?;
        let changed = transaction.execute(
            "UPDATE ops_provider_message_outbox SET state=?1,updated_at_ms=?2,revision=?3 WHERE message_id=?4 AND revision=?5 AND state IN(2,5)",
            params![
                encode_state(next_state),
                observed_at.get(),
                signed_revision(revision)?,
                message_id.as_bytes(),
                signed_revision(expected_revision)?,
            ],
        )?;
        if changed != 1 {
            return Err(OperationsError::Conflict("provider transition CAS"));
        }
        let outbox = load_outbox(&transaction, message_id)?
            .ok_or(OperationsError::Corrupt("provider transition readback"))?;
        transaction.commit().map_err(|source| {
            operation_commit_unconfirmed(target.workspace_id, message_id, source)
        })?;
        Ok(outbox)
    }
}

fn append_workspace_message_in_transaction(
    transaction: &Transaction<'_>,
    destination: Uuid,
    command: AppendWorkspaceMessageCommand,
) -> Result<MessageAppendOutcome, OperationsError> {
    let AppendWorkspaceMessageCommand {
        message_id,
        workspace_id,
        channel,
        actor,
        body,
        submitted_at,
        context,
    } = command;
    validate_message_id(message_id)?;
    validate_body(&body)?;
    let context = context.validate()?;
    require_verified_workspace(transaction, workspace_id, destination)?;
    validate_channel(transaction, workspace_id, channel)?;
    let resolved = resolve_actor(transaction, workspace_id, actor)?;
    if let Some(existing) = load_message(transaction, message_id)? {
        ensure_replay_scope(&existing, workspace_id, channel)?;
        ensure_replay_internal(&existing, &resolved, &body, submitted_at, context)?;
        return Ok(MessageAppendOutcome {
            message: existing,
            created: false,
        });
    }

    let sequence = next_sequence(transaction, workspace_id)?;
    insert_internal_message(
        transaction,
        message_id,
        workspace_id,
        channel,
        &resolved,
        &body,
        sequence,
        submitted_at,
        context,
    )?;
    let message = load_message(transaction, message_id)?
        .ok_or(OperationsError::Corrupt("workspace message insert"))?;
    Ok(MessageAppendOutcome {
        message,
        created: true,
    })
}

fn enqueue_provider_message_in_transaction(
    transaction: &Transaction<'_>,
    destination: Uuid,
    command: EnqueueProviderMessageCommand,
) -> Result<ProviderMessageQueueOutcome, OperationsError> {
    let EnqueueProviderMessageCommand {
        message_id,
        target,
        queued_at,
        node_route_witness,
    } = command;
    validate_message_id(message_id)?;
    require_verified_workspace(transaction, target.workspace_id, destination)?;
    let message = load_message(transaction, message_id)?
        .ok_or(OperationsError::Conflict("provider message source missing"))?;
    if message.workspace_id != target.workspace_id {
        return Err(OperationsError::Conflict("provider message Workspace"));
    }
    if !matches!(message.origin, MessageOrigin::Internal) {
        return Err(OperationsError::Conflict("provider message source origin"));
    }
    if let Some(existing) = load_outbox(transaction, message_id)? {
        ensure_target(&existing.target, &target)?;
        if existing.node_route_witness != node_route_witness {
            return Err(OperationsError::Conflict("provider node route replay"));
        }
        return Ok(ProviderMessageQueueOutcome {
            outbox: existing,
            created: false,
        });
    }
    crate::profile_deletion::require_profile_admission_open(transaction, target.profile_id)?;
    if message.context.node_id().is_some() != node_route_witness.is_some() {
        return Err(OperationsError::Conflict(
            "fresh node message requires exact route witness",
        ));
    }
    let encoded_node_route = node_route_witness
        .as_ref()
        .map(NodeMessageRouteWitness::encode)
        .transpose()?;
    if let Some(witness) = &node_route_witness {
        MessageReadScope::Node(message.context).validate()?;
        let sender_shape = matches!(
            (&message.author, witness.sender_leadership_revision()),
            (MessageAuthor::Operator, None) | (MessageAuthor::Participant { .. }, Some(_))
        );
        if !sender_shape {
            return Err(OperationsError::Conflict("node message sender witness"));
        }
        node_route::validate_node_admission(transaction, &message, &target, witness)?;
    }
    validate_recipient(transaction, &target)?;
    claim_provider_route(transaction, &target, queued_at)?;
    transaction.execute(
        "INSERT INTO ops_provider_message_outbox(message_id,workspace_id,recipient_profile_id,recipient_participation_id,provider_family,runtime_session_id,provider_conversation_id,state,queued_at_ms,last_dispatch_at_ms,updated_at_ms,revision,provider_turn_id,provider_body,node_route_witness) VALUES(?1,?2,?3,?4,?5,?6,?7,1,?8,0,?8,1,NULL,NULL,?9)",
        params![
            message_id.as_bytes(),
            target.workspace_id.into_uuid().as_bytes(),
            target.profile_id.into_uuid().as_bytes(),
            target.participation_id.into_uuid().as_bytes(),
            encode_provider_family(target.provider_family),
            target.runtime_session_id.into_uuid().as_bytes(),
            target.provider_conversation_id(),
            queued_at.get(),
            encoded_node_route,
        ],
    )?;
    let outbox = load_outbox(transaction, message_id)?
        .ok_or(OperationsError::Corrupt("provider message queue insert"))?;
    Ok(ProviderMessageQueueOutcome {
        outbox,
        created: true,
    })
}

/// Schema hooks are intentionally exposed for Main's catalog integration.
pub(crate) fn schema_objects() -> Vec<(&'static str, &'static str, &'static str)> {
    let mut objects = vec![
        ("table", "ops_provider_message_routes", PROVIDER_ROUTES_SQL),
        ("table", "ops_workspace_messages", WORKSPACE_MESSAGES_SQL),
        ("table", "ops_provider_message_outbox", PROVIDER_OUTBOX_SQL),
        (
            "index",
            "ops_provider_message_outbox_queue_idx",
            OUTBOX_TARGET_INDEX,
        ),
    ];
    objects.extend(governance::schema_objects());
    objects
}

/// Exact message, outbox and governance schema at catalog versions 32 through 35.
pub(crate) fn schema_objects_v35() -> Vec<(&'static str, &'static str, &'static str)> {
    schema_objects()
        .into_iter()
        .map(|(kind, name, sql)| {
            (
                kind,
                name,
                match name {
                    "ops_workspace_messages" => WORKSPACE_MESSAGES_SQL_V35,
                    "ops_provider_message_outbox" => PROVIDER_OUTBOX_SQL_V35,
                    _ => sql,
                },
            )
        })
        .collect()
}

/// Preserve legacy messages and add exact Goal-generation/DAG-base provenance.
pub(crate) fn migrate_v35_to_v36(transaction: &Transaction<'_>) -> Result<(), OperationsError> {
    transaction.execute_batch(
        "ALTER TABLE ops_provider_message_outbox ADD COLUMN node_route_witness BLOB CHECK(node_route_witness IS NULL OR (typeof(node_route_witness)='blob' AND length(node_route_witness) BETWEEN 1 AND 2048));\
         ALTER TABLE ops_workspace_messages ADD COLUMN goal_created_at_ms INTEGER CHECK(goal_created_at_ms IS NULL OR (goal_id IS NOT NULL AND goal_created_at_ms>=0));\
         ALTER TABLE ops_workspace_messages ADD COLUMN dag_base_goal_revision INTEGER CHECK(dag_base_goal_revision IS NULL OR (dag_revision IS NOT NULL AND dag_base_goal_revision>=1))",
    )?;
    Ok(())
}

/// Exact message/outbox schema as it existed at catalog version 31.
/// Historical validators must use this hook rather than the current schema.
pub(crate) fn schema_objects_v31() -> Vec<(&'static str, &'static str, &'static str)> {
    vec![
        ("table", "ops_provider_message_routes", PROVIDER_ROUTES_SQL),
        (
            "table",
            "ops_workspace_messages",
            WORKSPACE_MESSAGES_SQL_V31,
        ),
        (
            "table",
            "ops_provider_message_outbox",
            PROVIDER_OUTBOX_SQL_V35,
        ),
        (
            "index",
            "ops_provider_message_outbox_queue_idx",
            OUTBOX_TARGET_INDEX,
        ),
    ]
}

/// Create the message/outbox tables in an already initialized Operations
/// catalog.  Catalog version and migration ownership remain with Main.
pub(crate) fn create_schema(connection: &Connection) -> Result<(), OperationsError> {
    for (_, _, sql) in schema_objects() {
        connection.execute_batch(sql)?;
    }
    Ok(())
}

/// Upgrade the message table to the catalog-32 System-author shape and add
/// governance invitations.  Catalog migration owns foreign-key enforcement
/// state around this helper; this function only performs the transactional
/// table rebuild and preserves every existing message/outbox row.
pub(crate) fn migrate_v31_to_v32(transaction: &Transaction<'_>) -> Result<(), OperationsError> {
    let has_system_author: bool = transaction.query_row(
        "SELECT EXISTS(SELECT 1 FROM pragma_table_info('ops_workspace_messages') WHERE name='message_id') AND instr(COALESCE((SELECT sql FROM sqlite_schema WHERE type='table' AND name='ops_workspace_messages'),''),'author_kind IN(0,1,2,3)') > 0",
        [],
        |row| row.get(0),
    )?;
    if !has_system_author {
        transaction.execute_batch(&WORKSPACE_MESSAGES_SQL_V35.replace(
            "CREATE TABLE ops_workspace_messages ",
            "CREATE TABLE ops_workspace_messages_v32 ",
        ))?;
        transaction.execute(
            "INSERT INTO ops_workspace_messages_v32(message_id,workspace_id,channel_kind,channel_participation_id,author_kind,author_participation_id,author_profile_id,author_participant_id,author_session_id,author_display_name,origin_kind,body,sequence,submitted_at_ms,activity_id,goal_id,dag_revision,node_id,provider_conversation_id,provider_turn_id,provider_source_kind,provider_source_profile_id,provider_source_participation_id,provider_source_label) SELECT message_id,workspace_id,channel_kind,channel_participation_id,author_kind,author_participation_id,author_profile_id,author_participant_id,author_session_id,author_display_name,origin_kind,body,sequence,submitted_at_ms,activity_id,goal_id,dag_revision,node_id,provider_conversation_id,provider_turn_id,provider_source_kind,provider_source_profile_id,provider_source_participation_id,provider_source_label FROM ops_workspace_messages",
            [],
        )?;
        transaction.execute_batch(
            "DROP TABLE ops_workspace_messages; ALTER TABLE ops_workspace_messages_v32 RENAME TO ops_workspace_messages;",
        )?;
    }
    if !governance::table_exists(transaction, "ops_governance_invitations")? {
        governance::create_schema(transaction)?;
    }
    Ok(())
}

fn operation_commit_unconfirmed(
    workspace_id: WorkspaceId,
    request_id: Uuid,
    source: rusqlite::Error,
) -> OperationsError {
    OperationsError::OperationCommitUnconfirmed {
        workspace_id,
        request_id,
        source,
    }
}

fn catalog_commit_read(
    transaction: Transaction<'_>,
    destination: Uuid,
) -> Result<(), OperationsError> {
    crate::catalog::commit_read(transaction, destination)
}

pub(crate) fn require_verified_workspace(
    connection: &Connection,
    workspace_id: WorkspaceId,
    destination: Uuid,
) -> Result<(), OperationsError> {
    validate_workspace_id(workspace_id)?;
    let verified: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM ops_imports WHERE workspace_id=?1 AND destination_id=?2 AND phase=2)",
        params![workspace_id.into_uuid().as_bytes(), destination.as_bytes()],
        |row| row.get(0),
    )?;
    if verified {
        Ok(())
    } else {
        Err(OperationsError::Conflict(
            "Workspace import is not Verified",
        ))
    }
}

fn validate_channel(
    connection: &Connection,
    workspace_id: WorkspaceId,
    channel: MessageChannel,
) -> Result<(), OperationsError> {
    if let Some(participation_id) = channel.participation_id() {
        let present: bool = connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM ops_workspace_participations WHERE workspace_id=?1 AND participation_id=?2)",
            params![
                workspace_id.into_uuid().as_bytes(),
                participation_id.into_uuid().as_bytes(),
            ],
            |row| row.get(0),
        )?;
        if !present {
            return Err(OperationsError::Conflict(
                "message Direct recipient Workspace",
            ));
        }
    }
    Ok(())
}

fn validate_recipient(
    connection: &Connection,
    target: &ProviderMessageTarget,
) -> Result<(), OperationsError> {
    let present: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM ops_workspace_participations WHERE workspace_id=?1 AND agent_profile_id=?2 AND participation_id=?3)",
        params![
            target.workspace_id.into_uuid().as_bytes(),
            target.profile_id.into_uuid().as_bytes(),
            target.participation_id.into_uuid().as_bytes(),
        ],
        |row| row.get(0),
    )?;
    if present {
        Ok(())
    } else {
        Err(OperationsError::Conflict(
            "provider recipient Workspace/Participation",
        ))
    }
}

fn claim_provider_route(
    connection: &Connection,
    target: &ProviderMessageTarget,
    observed_at: TimestampMillis,
) -> Result<(), OperationsError> {
    let mut statement = connection.prepare(
        "SELECT substr(workspace_id,1,17),substr(recipient_profile_id,1,17),substr(recipient_participation_id,1,17),substr(runtime_session_id,1,17),provider_conversation_id FROM ops_provider_message_routes WHERE provider_family=?1 AND (runtime_session_id=?2 OR provider_conversation_id=?3)",
    )?;
    let rows = statement.query_map(
        params![
            encode_provider_family(target.provider_family),
            target.runtime_session_id.into_uuid().as_bytes(),
            target.provider_conversation_id(),
        ],
        |row| {
            Ok((
                row.get::<_, Vec<u8>>(0)?,
                row.get::<_, Vec<u8>>(1)?,
                row.get::<_, Vec<u8>>(2)?,
                row.get::<_, Vec<u8>>(3)?,
                row.get::<_, String>(4)?,
            ))
        },
    )?;
    for row in rows {
        let (workspace, profile, participation, runtime_session, conversation) = row?;
        let workspace = WorkspaceId::from_uuid(decode_uuid(workspace, "provider route Workspace")?);
        let profile = AgentProfileId::from_uuid(decode_uuid(profile, "provider route profile")?)
            .map_err(|_| OperationsError::Corrupt("provider route profile"))?;
        let participation = WorkspaceParticipationId::from_uuid(decode_uuid(
            participation,
            "provider route participation",
        )?)?;
        let runtime_session = AgentProfileRuntimeSessionId::from_uuid(decode_uuid(
            runtime_session,
            "provider route runtime",
        )?)
        .map_err(|_| OperationsError::Corrupt("provider route runtime"))?;
        let same_runtime = runtime_session == target.runtime_session_id;
        let same_conversation = conversation == target.provider_conversation_id();
        if same_runtime && same_conversation {
            if workspace != target.workspace_id {
                return Err(OperationsError::Conflict("provider route Workspace"));
            }
            if profile != target.profile_id || participation != target.participation_id {
                return Err(OperationsError::Conflict("provider route recipient"));
            }
            return Ok(());
        }
        if same_runtime {
            return Err(OperationsError::Conflict("provider route runtime alias"));
        }
        if same_conversation {
            return Err(OperationsError::Conflict(
                "provider route conversation alias",
            ));
        }
    }
    drop(statement);
    connection.execute(
        "INSERT INTO ops_provider_message_routes(provider_family,runtime_session_id,provider_conversation_id,workspace_id,recipient_profile_id,recipient_participation_id,created_at_ms) VALUES(?1,?2,?3,?4,?5,?6,?7)",
        params![
            encode_provider_family(target.provider_family),
            target.runtime_session_id.into_uuid().as_bytes(),
            target.provider_conversation_id(),
            target.workspace_id.into_uuid().as_bytes(),
            target.profile_id.into_uuid().as_bytes(),
            target.participation_id.into_uuid().as_bytes(),
            observed_at.get(),
        ],
    )?;
    Ok(())
}

fn validate_provider_source(
    connection: &Connection,
    workspace_id: WorkspaceId,
    source: &ProviderMessageSource,
) -> Result<(), OperationsError> {
    if let ProviderMessageSource::Known {
        profile_id,
        participation_id,
        ..
    } = source
    {
        let present: bool = connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM ops_workspace_participations WHERE workspace_id=?1 AND agent_profile_id=?2 AND participation_id=?3)",
            params![
                workspace_id.into_uuid().as_bytes(),
                profile_id.into_uuid().as_bytes(),
                participation_id.into_uuid().as_bytes(),
            ],
            |row| row.get(0),
        )?;
        if !present {
            return Err(OperationsError::Conflict(
                "provider source Workspace/Participation",
            ));
        }
    }
    Ok(())
}

fn ensure_incoming_source_target(
    target: &ProviderMessageTarget,
    source: &ProviderMessageSource,
) -> Result<(), OperationsError> {
    if let ProviderMessageSource::Known {
        profile_id,
        participation_id,
        ..
    } = source
        && (*profile_id != target.profile_id || *participation_id != target.participation_id)
    {
        return Err(OperationsError::Conflict("incoming provider source target"));
    }
    Ok(())
}

fn ensure_target(
    existing: &ProviderMessageTarget,
    requested: &ProviderMessageTarget,
) -> Result<(), OperationsError> {
    if existing == requested {
        Ok(())
    } else {
        Err(OperationsError::Conflict("provider message target changed"))
    }
}

fn validate_message_id(value: Uuid) -> Result<(), OperationsError> {
    if value.is_nil() {
        Err(OperationsError::Conflict("workspace message identity"))
    } else {
        Ok(())
    }
}

fn validate_workspace_id(value: WorkspaceId) -> Result<(), OperationsError> {
    if value.into_uuid().is_nil() {
        Err(OperationsError::Conflict("Workspace identity"))
    } else {
        Ok(())
    }
}

fn validate_body(value: &str) -> Result<(), OperationsError> {
    let bytes = value.len();
    if bytes == 0 || bytes > MAX_MESSAGE_BODY_BYTES {
        return Err(OperationsError::Bounds("message body"));
    }
    Ok(())
}

/// Validate the raw chat text before route selection or effect admission.
/// Formatted provider payloads retain their separate owner validation.
///
/// # Errors
/// Rejects empty text and the existing raw-text or encoded JSON byte bounds.
pub fn validate_chat_text(value: &str) -> Result<(), OperationsError> {
    if value.trim().is_empty() {
        return Err(OperationsError::Bounds("chat text"));
    }
    validate_body(value)?;
    if serde_json::to_vec(value)?.len() > MAX_PROVIDER_MESSAGE_JSON_BYTES {
        return Err(OperationsError::Bounds("chat JSON text"));
    }
    Ok(())
}

fn format_provider_message_text(
    workspace_id: WorkspaceId,
    body: &str,
    context: MessageContext,
) -> Result<String, OperationsError> {
    let formatted = if context.goal_id().is_none()
        && context.goal_created_at().is_none()
        && context.dag_revision().is_none()
        && context.dag_base_goal_revision().is_none()
        && context.node_id().is_none()
    {
        body.to_owned()
    } else {
        let mut prefix = format!("[Heterarchy context]\nWorkspace: {workspace_id}");
        if let Some(goal_id) = context.goal_id() {
            let _ = write!(prefix, "\nGoal: {goal_id}");
        }
        if let Some(goal_created_at) = context.goal_created_at() {
            let _ = write!(prefix, "\nGoal created at (ms): {}", goal_created_at.get());
        }
        if let Some(dag_revision) = context.dag_revision() {
            let _ = write!(prefix, "\nDAG revision: {}", dag_revision.get());
        }
        if let Some(base_goal_revision) = context.dag_base_goal_revision() {
            let _ = write!(
                prefix,
                "\nDAG base Goal revision: {}",
                base_goal_revision.get()
            );
        }
        if let Some(node_id) = context.node_id() {
            let _ = write!(prefix, "\nNode: {node_id}");
        }
        prefix.push_str("\n\n");
        prefix.push_str(body);
        prefix
    };
    validate_body(&formatted)?;
    if serde_json::to_vec(&formatted)?.len() > MAX_PROVIDER_MESSAGE_JSON_BYTES {
        return Err(OperationsError::Bounds("provider message JSON body"));
    }
    Ok(formatted)
}

fn validate_display_name(value: &str) -> Result<(), OperationsError> {
    if value.is_empty() || value.len() > MAX_DISPLAY_NAME_BYTES {
        Err(OperationsError::Bounds("message display name"))
    } else {
        Ok(())
    }
}

fn validate_source_label(value: &str) -> Result<(), OperationsError> {
    if value.is_empty() || value.len() > MAX_PROVIDER_SOURCE_LABEL_BYTES {
        Err(OperationsError::Bounds("provider source label"))
    } else {
        Ok(())
    }
}

fn validate_provider_id(value: &str, field: &'static str) -> Result<(), OperationsError> {
    if value.is_empty() || value.len() > MAX_PROVIDER_ID_BYTES {
        Err(OperationsError::Bounds(field))
    } else {
        Ok(())
    }
}

fn validate_page_limit(limit: usize) -> Result<(), OperationsError> {
    if (1..=MAX_MESSAGE_PAGE_SIZE).contains(&limit) {
        Ok(())
    } else {
        Err(OperationsError::Bounds("message page limit"))
    }
}

fn signed_counter(value: u64) -> Result<i64, OperationsError> {
    i64::try_from(value).map_err(|_| OperationsError::Bounds("message sequence cursor"))
}

fn signed_revision(value: u64) -> Result<i64, OperationsError> {
    i64::try_from(value).map_err(|_| OperationsError::Bounds("provider message revision"))
}

fn next_revision(value: u64) -> Result<u64, OperationsError> {
    value
        .checked_add(1)
        .ok_or(OperationsError::Bounds("provider message revision"))
}

fn next_sequence(
    connection: &Connection,
    workspace_id: WorkspaceId,
) -> Result<u64, OperationsError> {
    let current: i64 = connection.query_row(
        "SELECT COALESCE(MAX(sequence),0) FROM ops_workspace_messages WHERE workspace_id=?1",
        params![workspace_id.into_uuid().as_bytes()],
        |row| row.get(0),
    )?;
    let current =
        u64::try_from(current).map_err(|_| OperationsError::Corrupt("message sequence"))?;
    current
        .checked_add(1)
        .ok_or(OperationsError::Bounds("message sequence"))
}

fn ensure_replay_scope(
    existing: &WorkspaceMessage,
    workspace_id: WorkspaceId,
    channel: MessageChannel,
) -> Result<(), OperationsError> {
    if existing.workspace_id == workspace_id && existing.channel == channel {
        Ok(())
    } else {
        Err(OperationsError::Conflict("workspace message replay scope"))
    }
}

fn ensure_replay_internal(
    existing: &WorkspaceMessage,
    actor: &ResolvedOperationalActor,
    body: &str,
    submitted_at: TimestampMillis,
    context: MessageContext,
) -> Result<(), OperationsError> {
    if !matches!(existing.origin, MessageOrigin::Internal)
        || existing.body != body
        || existing.submitted_at != submitted_at
        || existing.context != context
        || !author_matches_resolved(&existing.author, actor)
    {
        return Err(OperationsError::Conflict(
            "workspace message replay changed",
        ));
    }
    Ok(())
}

fn ensure_replay_provider(
    existing: &WorkspaceMessage,
    conversation_id: &str,
    turn_id: &str,
    source: &ProviderMessageSource,
    body: &str,
    submitted_at: TimestampMillis,
    context: MessageContext,
) -> Result<(), OperationsError> {
    let matches_origin = matches!(
        &existing.origin,
        MessageOrigin::Provider {
            provider_conversation_id,
            provider_turn_id,
            source: existing_source,
        } if provider_conversation_id == conversation_id
            && provider_turn_id == turn_id
            && existing_source == source
    );
    if !matches_origin
        || existing.body != body
        || existing.submitted_at != submitted_at
        || existing.context != context
    {
        return Err(OperationsError::Conflict(
            "incoming provider replay changed",
        ));
    }
    Ok(())
}

fn author_matches_resolved(author: &MessageAuthor, resolved: &ResolvedOperationalActor) -> bool {
    match (author, resolved.participation_id) {
        (MessageAuthor::Operator, None) => true,
        (
            MessageAuthor::Participant {
                participation_id,
                profile_id,
                participant_id,
                session_id,
            },
            Some(_resolved_participation),
        ) => {
            Some(*participation_id) == resolved.participation_id
                && Some(*profile_id) == resolved.profile_id
                && Some(*participant_id) == resolved.participant_id
                && Some(*session_id) == resolved.session_id
        }
        _ => false,
    }
}

#[allow(clippy::too_many_arguments)]
fn insert_internal_message(
    transaction: &Transaction<'_>,
    message_id: MessageId,
    workspace_id: WorkspaceId,
    channel: MessageChannel,
    actor: &ResolvedOperationalActor,
    body: &str,
    sequence: u64,
    submitted_at: TimestampMillis,
    context: MessageContext,
) -> Result<(), OperationsError> {
    let (author_kind, participation_id, profile_id, participant_id, session_id) =
        if let Some(participation_id) = actor.participation_id {
            (
                1,
                Some(participation_id.into_uuid().into_bytes().to_vec()),
                actor.profile_id.map(|value| value.into_bytes().to_vec()),
                actor
                    .participant_id
                    .map(|value| value.into_bytes().to_vec()),
                actor
                    .session_id
                    .map(|value| value.into_uuid().into_bytes().to_vec()),
            )
        } else {
            (0, None, None, None, None)
        };
    transaction.execute(
        "INSERT INTO ops_workspace_messages(message_id,workspace_id,channel_kind,channel_participation_id,author_kind,author_participation_id,author_profile_id,author_participant_id,author_session_id,author_display_name,origin_kind,body,sequence,submitted_at_ms,activity_id,goal_id,dag_revision,node_id,provider_conversation_id,provider_turn_id,provider_source_kind,provider_source_profile_id,provider_source_participation_id,provider_source_label,goal_created_at_ms,dag_base_goal_revision) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,0,?11,?12,?13,?14,?15,?16,?17,NULL,NULL,NULL,NULL,NULL,NULL,?18,?19)",
        params![
            message_id.as_bytes(),
            workspace_id.into_uuid().as_bytes(),
            channel.kind(),
            channel.participation_id().map(|value| value.into_uuid().into_bytes().to_vec()),
            author_kind,
            participation_id,
            profile_id,
            participant_id,
            session_id,
            actor.display_name,
            body,
            signed_counter(sequence)?,
            submitted_at.get(),
            context.activity_id.map(|value| value.into_uuid().into_bytes().to_vec()),
            context.goal_id.map(|value| value.into_uuid().into_bytes().to_vec()),
            context.dag_revision.map(DagRevisionNumber::get),
            context.node_id.map(|value| value.into_uuid().into_bytes().to_vec()),
            context.goal_created_at.map(TimestampMillis::get),
            context.dag_base_goal_revision.map(GoalRevision::get),
        ],
    )?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn insert_system_message(
    transaction: &Transaction<'_>,
    message_id: MessageId,
    workspace_id: WorkspaceId,
    channel: MessageChannel,
    body: &str,
    sequence: u64,
    submitted_at: TimestampMillis,
) -> Result<(), OperationsError> {
    transaction.execute(
        "INSERT INTO ops_workspace_messages(message_id,workspace_id,channel_kind,channel_participation_id,author_kind,author_participation_id,author_profile_id,author_participant_id,author_session_id,author_display_name,origin_kind,body,sequence,submitted_at_ms,activity_id,goal_id,dag_revision,node_id,provider_conversation_id,provider_turn_id,provider_source_kind,provider_source_profile_id,provider_source_participation_id,provider_source_label,goal_created_at_ms,dag_base_goal_revision) VALUES(?1,?2,?3,?4,3,NULL,NULL,NULL,NULL,'System',0,?5,?6,?7,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL)",
        params![
            message_id.as_bytes(),
            workspace_id.into_uuid().as_bytes(),
            channel.kind(),
            channel
                .participation_id()
                .map(|value| value.into_uuid().into_bytes().to_vec()),
            body,
            signed_counter(sequence)?,
            submitted_at.get(),
        ],
    )?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn insert_provider_message(
    transaction: &Transaction<'_>,
    message_id: MessageId,
    workspace_id: WorkspaceId,
    channel: MessageChannel,
    source: &ProviderMessageSource,
    conversation_id: &str,
    turn_id: &str,
    body: &str,
    sequence: u64,
    submitted_at: TimestampMillis,
    context: MessageContext,
) -> Result<(), OperationsError> {
    transaction.execute(
        "INSERT INTO ops_workspace_messages(message_id,workspace_id,channel_kind,channel_participation_id,author_kind,author_participation_id,author_profile_id,author_participant_id,author_session_id,author_display_name,origin_kind,body,sequence,submitted_at_ms,activity_id,goal_id,dag_revision,node_id,provider_conversation_id,provider_turn_id,provider_source_kind,provider_source_profile_id,provider_source_participation_id,provider_source_label,goal_created_at_ms,dag_base_goal_revision) VALUES(?1,?2,?3,?4,2,NULL,NULL,NULL,NULL,?5,1,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20)",
        params![
            message_id.as_bytes(),
            workspace_id.into_uuid().as_bytes(),
            channel.kind(),
            channel.participation_id().map(|value| value.into_uuid().into_bytes().to_vec()),
            source.display_name(),
            body,
            signed_counter(sequence)?,
            submitted_at.get(),
            context.activity_id.map(|value| value.into_uuid().into_bytes().to_vec()),
            context.goal_id.map(|value| value.into_uuid().into_bytes().to_vec()),
            context.dag_revision.map(DagRevisionNumber::get),
            context.node_id.map(|value| value.into_uuid().into_bytes().to_vec()),
            conversation_id,
            turn_id,
            source.kind(),
            source.profile_id().map(|value| value.into_uuid().into_bytes().to_vec()),
            source.participation_id().map(|value| value.into_uuid().into_bytes().to_vec()),
            Some(source.display_name()),
            context.goal_created_at.map(TimestampMillis::get),
            context.dag_base_goal_revision.map(GoalRevision::get),
        ],
    )?;
    Ok(())
}

fn load_message(
    connection: &Connection,
    message_id: MessageId,
) -> Result<Option<WorkspaceMessage>, OperationsError> {
    Ok(connection
        .query_row(
            "SELECT substr(message_id,1,17),substr(workspace_id,1,17),channel_kind,substr(channel_participation_id,1,17),author_kind,substr(author_participation_id,1,17),substr(author_profile_id,1,17),substr(author_participant_id,1,17),substr(author_session_id,1,17),substr(CAST(author_display_name AS BLOB),1,129),origin_kind,substr(CAST(body AS BLOB),1,16385),sequence,submitted_at_ms,substr(activity_id,1,17),substr(goal_id,1,17),dag_revision,substr(node_id,1,17),substr(CAST(provider_conversation_id AS BLOB),1,513),substr(CAST(provider_turn_id AS BLOB),1,513),provider_source_kind,substr(provider_source_profile_id,1,17),substr(provider_source_participation_id,1,17),substr(CAST(provider_source_label AS BLOB),1,257),goal_created_at_ms,dag_base_goal_revision FROM ops_workspace_messages WHERE message_id=?1",
            params![message_id.as_bytes()],
            decode_message_row,
        )
        .optional()?)
}

fn load_provider_turn(
    connection: &Connection,
    workspace_id: WorkspaceId,
    conversation_id: &str,
    turn_id: &str,
) -> Result<Option<WorkspaceMessage>, OperationsError> {
    Ok(connection
        .query_row(
            "SELECT substr(message_id,1,17),substr(workspace_id,1,17),channel_kind,substr(channel_participation_id,1,17),author_kind,substr(author_participation_id,1,17),substr(author_profile_id,1,17),substr(author_participant_id,1,17),substr(author_session_id,1,17),substr(CAST(author_display_name AS BLOB),1,129),origin_kind,substr(CAST(body AS BLOB),1,16385),sequence,submitted_at_ms,substr(activity_id,1,17),substr(goal_id,1,17),dag_revision,substr(node_id,1,17),substr(CAST(provider_conversation_id AS BLOB),1,513),substr(CAST(provider_turn_id AS BLOB),1,513),provider_source_kind,substr(provider_source_profile_id,1,17),substr(provider_source_participation_id,1,17),substr(CAST(provider_source_label AS BLOB),1,257),goal_created_at_ms,dag_base_goal_revision FROM ops_workspace_messages WHERE workspace_id=?1 AND provider_conversation_id=?2 AND provider_turn_id=?3",
            params![workspace_id.into_uuid().as_bytes(), conversation_id, turn_id],
            decode_message_row,
        )
        .optional()?)
}

fn load_delivered_provider_turn(
    connection: &Connection,
    target: &ProviderMessageTarget,
    turn_id: &str,
) -> Result<Option<(WorkspaceMessage, String)>, OperationsError> {
    let raw: Option<(Vec<u8>, Vec<u8>)> = connection
        .query_row(
            "SELECT substr(message_id,1,17),substr(CAST(provider_body AS BLOB),1,16385) FROM ops_provider_message_outbox WHERE workspace_id=?1 AND recipient_profile_id=?2 AND recipient_participation_id=?3 AND provider_family=?4 AND runtime_session_id=?5 AND provider_conversation_id=?6 AND provider_turn_id=?7 AND state=3",
            params![
                target.workspace_id.into_uuid().as_bytes(),
                target.profile_id.into_uuid().as_bytes(),
                target.participation_id.into_uuid().as_bytes(),
                encode_provider_family(target.provider_family),
                target.runtime_session_id.into_uuid().as_bytes(),
                target.provider_conversation_id(),
                turn_id,
            ],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    raw.map(|(message_id, body)| {
        let message_id = decode_uuid(message_id, "provider delivered message")?;
        let body = decode_string(body, "provider delivered body")?;
        let message = load_message(connection, message_id)?.ok_or(OperationsError::Corrupt(
            "provider delivered source message",
        ))?;
        Ok((message, body))
    })
    .transpose()
}

#[allow(clippy::too_many_lines)]
fn decode_message_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<WorkspaceMessage> {
    let message_id = decode_uuid(row.get(0)?, "message id")?;
    let workspace_id = WorkspaceId::from_uuid(decode_uuid(row.get(1)?, "workspace id")?);
    let channel_kind: i64 = row.get(2)?;
    let channel_participation = row
        .get::<_, Option<Vec<u8>>>(3)?
        .map(|value| {
            WorkspaceParticipationId::from_uuid(decode_uuid(value, "channel participation")?)
                .map_err(to_sqlite_error)
        })
        .transpose()?;
    let channel = match (channel_kind, channel_participation) {
        (0, None) => MessageChannel::Workspace,
        (1, Some(participation)) => MessageChannel::Direct(participation),
        _ => return Err(to_sqlite_error(OperationsError::Corrupt("message channel"))),
    };
    let author_kind: i64 = row.get(4)?;
    let author_participation = row
        .get::<_, Option<Vec<u8>>>(5)?
        .map(|value| {
            WorkspaceParticipationId::from_uuid(decode_uuid(value, "author participation")?)
                .map_err(to_sqlite_error)
        })
        .transpose()?;
    let author_profile = row
        .get::<_, Option<Vec<u8>>>(6)?
        .map(|value| decode_uuid(value, "author profile"))
        .transpose()?;
    let author_participant = row
        .get::<_, Option<Vec<u8>>>(7)?
        .map(|value| decode_uuid(value, "author participant"))
        .transpose()?;
    let author_session = row
        .get::<_, Option<Vec<u8>>>(8)?
        .map(|value| decode_uuid(value, "author session").map(AgentSessionId::from_uuid))
        .transpose()?;
    let author_display_name = decode_string(row.get(9)?, "author display name")?;
    validate_display_name(&author_display_name).map_err(to_sqlite_error)?;
    let origin_kind: i64 = row.get(10)?;
    let body = decode_string(row.get(11)?, "message body")?;
    validate_body(&body).map_err(to_sqlite_error)?;
    let sequence = decode_counter(row.get(12)?, "message sequence")?;
    let submitted_at = TimestampMillis::new(row.get(13)?)
        .map_err(|_| to_sqlite_error(OperationsError::Corrupt("message submitted time")))?;
    let activity_id = row
        .get::<_, Option<Vec<u8>>>(14)?
        .map(|value| decode_uuid(value, "message activity").map(ActivityId::from_uuid))
        .transpose()?;
    let goal_id = row
        .get::<_, Option<Vec<u8>>>(15)?
        .map(|value| decode_uuid(value, "message goal").map(GoalId::from_uuid))
        .transpose()?;
    let goal_created_at = row
        .get::<_, Option<i64>>(24)?
        .map(|value| {
            TimestampMillis::new(value)
                .map_err(|_| to_sqlite_error(OperationsError::Corrupt("message Goal generation")))
        })
        .transpose()?;
    let dag_revision = row
        .get::<_, Option<i64>>(16)?
        .map(|value| {
            DagRevisionNumber::new(value)
                .map_err(|_| to_sqlite_error(OperationsError::Corrupt("message DAG revision")))
        })
        .transpose()?;
    let node_id = row
        .get::<_, Option<Vec<u8>>>(17)?
        .map(|value| decode_uuid(value, "message node").map(DagNodeId::from_uuid))
        .transpose()?;
    let dag_base_goal_revision = row
        .get::<_, Option<i64>>(25)?
        .map(|value| {
            GoalRevision::new(value).map_err(|_| {
                to_sqlite_error(OperationsError::Corrupt("message DAG base Goal revision"))
            })
        })
        .transpose()?;
    let context = MessageContext::new(
        activity_id,
        goal_id,
        goal_created_at,
        dag_revision,
        dag_base_goal_revision,
        node_id,
    )
    .map_err(to_sqlite_error)?;
    let provider_conversation_id = row
        .get::<_, Option<Vec<u8>>>(18)?
        .map(|value| decode_string(value, "provider conversation"))
        .transpose()?;
    let provider_turn_id = row
        .get::<_, Option<Vec<u8>>>(19)?
        .map(|value| decode_string(value, "provider turn"))
        .transpose()?;
    let provider_source_kind = row.get::<_, Option<i64>>(20)?;
    let provider_source_profile = row
        .get::<_, Option<Vec<u8>>>(21)?
        .map(|value| {
            AgentProfileId::from_uuid(decode_uuid(value, "provider source profile")?)
                .map_err(|_| to_sqlite_error(OperationsError::Corrupt("provider source profile")))
        })
        .transpose()?;
    let provider_source_participation = row
        .get::<_, Option<Vec<u8>>>(22)?
        .map(|value| {
            WorkspaceParticipationId::from_uuid(decode_uuid(
                value,
                "provider source participation",
            )?)
            .map_err(to_sqlite_error)
        })
        .transpose()?;
    let provider_source_label = row
        .get::<_, Option<Vec<u8>>>(23)?
        .map(|value| decode_string(value, "provider source label"))
        .transpose()?;
    let source = decode_source(
        provider_source_kind,
        provider_source_profile,
        provider_source_participation,
        provider_source_label,
    )?;
    if let Some(source) = &source
        && source.display_name() != author_display_name
    {
        return Err(to_sqlite_error(OperationsError::Corrupt(
            "provider author display snapshot",
        )));
    }
    let author = match author_kind {
        0 if author_participation.is_none()
            && author_profile.is_none()
            && author_participant.is_none()
            && author_session.is_none() =>
        {
            MessageAuthor::Operator
        }
        3 if author_participation.is_none()
            && author_profile.is_none()
            && author_participant.is_none()
            && author_session.is_none()
            && author_display_name == "System" =>
        {
            MessageAuthor::System
        }
        1 => MessageAuthor::Participant {
            participation_id: author_participation
                .ok_or_else(|| to_sqlite_error(OperationsError::Corrupt("author participation")))?,
            profile_id: author_profile
                .ok_or_else(|| to_sqlite_error(OperationsError::Corrupt("author profile")))?,
            participant_id: author_participant
                .ok_or_else(|| to_sqlite_error(OperationsError::Corrupt("author participant")))?,
            session_id: author_session
                .ok_or_else(|| to_sqlite_error(OperationsError::Corrupt("author session")))?,
        },
        2 if author_participation.is_none()
            && author_profile.is_none()
            && author_participant.is_none()
            && author_session.is_none() =>
        {
            MessageAuthor::Provider {
                source: source.clone().ok_or_else(|| {
                    to_sqlite_error(OperationsError::Corrupt("provider author source"))
                })?,
            }
        }
        _ => return Err(to_sqlite_error(OperationsError::Corrupt("message author"))),
    };
    let origin = match origin_kind {
        0 if provider_conversation_id.is_none()
            && provider_turn_id.is_none()
            && source.is_none() =>
        {
            MessageOrigin::Internal
        }
        1 => MessageOrigin::Provider {
            provider_conversation_id: provider_conversation_id.ok_or_else(|| {
                to_sqlite_error(OperationsError::Corrupt("provider conversation"))
            })?,
            provider_turn_id: provider_turn_id
                .ok_or_else(|| to_sqlite_error(OperationsError::Corrupt("provider turn")))?,
            source: source
                .ok_or_else(|| to_sqlite_error(OperationsError::Corrupt("provider source")))?,
        },
        _ => return Err(to_sqlite_error(OperationsError::Corrupt("message origin"))),
    };
    Ok(WorkspaceMessage {
        message_id,
        workspace_id,
        channel,
        author,
        author_display_name,
        origin,
        body,
        sequence,
        submitted_at,
        context,
    })
}

fn decode_source(
    kind: Option<i64>,
    profile_id: Option<AgentProfileId>,
    participation_id: Option<WorkspaceParticipationId>,
    label: Option<String>,
) -> rusqlite::Result<Option<ProviderMessageSource>> {
    match (kind, profile_id, participation_id, label) {
        (None, None, None, None) => Ok(None),
        (Some(0), Some(profile_id), Some(participation_id), Some(label)) => {
            Ok(Some(ProviderMessageSource::Known {
                profile_id,
                participation_id,
                display_name: label,
            }))
        }
        (Some(1), None, None, Some(label)) => Ok(Some(ProviderMessageSource::Unknown { label })),
        _ => Err(to_sqlite_error(OperationsError::Corrupt(
            "provider source shape",
        ))),
    }
}

fn decode_outbox_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<ProviderMessageOutbox> {
    let message_id = decode_uuid(row.get(0)?, "provider message id")?;
    let workspace_id = WorkspaceId::from_uuid(decode_uuid(row.get(1)?, "provider Workspace")?);
    let profile_id = AgentProfileId::from_uuid(decode_uuid(row.get(2)?, "recipient profile")?)
        .map_err(|_| to_sqlite_error(OperationsError::Corrupt("recipient profile")))?;
    let participation_id =
        WorkspaceParticipationId::from_uuid(decode_uuid(row.get(3)?, "recipient participation")?)
            .map_err(to_sqlite_error)?;
    let provider_family = decode_provider_family(row.get(4)?)?;
    let runtime_session_id =
        AgentProfileRuntimeSessionId::from_uuid(decode_uuid(row.get(5)?, "runtime session")?)
            .map_err(|_| to_sqlite_error(OperationsError::Corrupt("runtime session")))?;
    let provider_conversation_id = decode_string(row.get(6)?, "provider conversation")?;
    validate_provider_id(&provider_conversation_id, "provider conversation identity")
        .map_err(to_sqlite_error)?;
    let state = decode_state(row.get(7)?)?;
    let queued_at = TimestampMillis::new(row.get(8)?)
        .map_err(|_| to_sqlite_error(OperationsError::Corrupt("provider queued time")))?;
    let last_dispatch_at = TimestampMillis::new(row.get(9)?)
        .map_err(|_| to_sqlite_error(OperationsError::Corrupt("provider dispatch time")))?;
    let updated_at = TimestampMillis::new(row.get(10)?)
        .map_err(|_| to_sqlite_error(OperationsError::Corrupt("provider updated time")))?;
    if updated_at < queued_at {
        return Err(to_sqlite_error(OperationsError::Corrupt(
            "provider timestamp order",
        )));
    }
    let revision = decode_counter(row.get(11)?, "provider revision")?;
    let provider_turn_id = row
        .get::<_, Option<Vec<u8>>>(12)?
        .map(|value| decode_string(value, "provider turn"))
        .transpose()?;
    let provider_body = row
        .get::<_, Option<Vec<u8>>>(13)?
        .map(|value| decode_string(value, "provider body"))
        .transpose()?;
    if let Some(body) = &provider_body {
        validate_body(body).map_err(to_sqlite_error)?;
    }
    let receipt = match (provider_turn_id, provider_body) {
        (None, None) => None,
        (Some(provider_turn_id), Some(body)) => Some(ProviderMessageDeliveryReceipt {
            target: ProviderMessageTarget {
                workspace_id,
                profile_id,
                participation_id,
                provider_family,
                runtime_session_id,
                provider_conversation_id: provider_conversation_id.clone(),
            },
            provider_turn_id,
            body,
        }),
        _ => {
            return Err(to_sqlite_error(OperationsError::Corrupt(
                "provider receipt shape",
            )));
        }
    };
    let node_route_witness = row
        .get::<_, Option<Vec<u8>>>(14)?
        .map(|bytes| NodeMessageRouteWitness::decode(&bytes).map_err(to_sqlite_error))
        .transpose()?;
    Ok(ProviderMessageOutbox {
        message_id,
        target: ProviderMessageTarget {
            workspace_id,
            profile_id,
            participation_id,
            provider_family,
            runtime_session_id,
            provider_conversation_id,
        },
        state,
        queued_at,
        last_dispatch_at,
        updated_at,
        revision,
        receipt,
        node_route_witness,
    })
}

fn load_outbox(
    connection: &Connection,
    message_id: MessageId,
) -> Result<Option<ProviderMessageOutbox>, OperationsError> {
    Ok(connection
        .query_row(
            "SELECT substr(message_id,1,17),substr(workspace_id,1,17),substr(recipient_profile_id,1,17),substr(recipient_participation_id,1,17),provider_family,substr(runtime_session_id,1,17),substr(CAST(provider_conversation_id AS BLOB),1,513),state,queued_at_ms,last_dispatch_at_ms,updated_at_ms,revision,substr(CAST(provider_turn_id AS BLOB),1,513),substr(CAST(provider_body AS BLOB),1,16385),substr(node_route_witness,1,2049) FROM ops_provider_message_outbox WHERE message_id=?1",
            params![message_id.as_bytes()],
            decode_outbox_row,
        )
        .optional()?)
}

fn eligible_outbox_message(
    connection: &Connection,
    requested_message_id: Option<MessageId>,
) -> Result<Option<Uuid>, OperationsError> {
    let requested = requested_message_id.map(|id| id.as_bytes().to_vec());
    connection
        .query_row(
            "SELECT substr(candidate.message_id,1,17)
               FROM ops_provider_message_outbox AS candidate
              WHERE candidate.state IN(1,2,5)
                AND (?1 IS NULL OR candidate.message_id=?1)
                AND (
                    candidate.state!=1
                    OR NOT EXISTS(
                        SELECT 1 FROM ops_agent_profile_deletion_barriers AS barrier
                         WHERE barrier.profile_id=candidate.recipient_profile_id
                    )
                )
                AND (
                    candidate.state IN(2,5)
                    OR (
                        NOT EXISTS(
                            SELECT 1 FROM ops_provider_message_outbox AS blocked
                             WHERE blocked.runtime_session_id=candidate.runtime_session_id
                               AND blocked.state IN(2,5)
                        )
                        AND NOT EXISTS(
                            SELECT 1 FROM ops_provider_message_outbox AS earlier
                             WHERE earlier.runtime_session_id=candidate.runtime_session_id
                               AND earlier.state=1
                               AND (
                                    earlier.queued_at_ms < candidate.queued_at_ms
                                    OR (earlier.queued_at_ms = candidate.queued_at_ms
                                        AND earlier.message_id < candidate.message_id)
                               )
                        )
                    )
                )
              ORDER BY candidate.last_dispatch_at_ms ASC,
                       candidate.queued_at_ms ASC,
                       candidate.message_id ASC
              LIMIT 1",
            params![requested],
            |row| decode_uuid(row.get(0)?, "provider eligible message"),
        )
        .optional()
        .map_err(OperationsError::from)
}

fn dispatch_for(
    outbox: &ProviderMessageOutbox,
    action: ProviderMessageDispatchAction,
) -> ProviderMessageDispatch {
    ProviderMessageDispatch {
        message_id: outbox.message_id,
        revision: outbox.revision,
        action,
        target: outbox.target.clone(),
    }
}

fn validate_revision_and_time(
    existing: &ProviderMessageOutbox,
    expected_revision: u64,
    observed_at: TimestampMillis,
) -> Result<(), OperationsError> {
    if expected_revision == 0 || existing.revision != expected_revision {
        return Err(OperationsError::Conflict("provider message revision fence"));
    }
    if observed_at < existing.updated_at {
        return Err(OperationsError::Conflict(
            "provider message timestamp fence",
        ));
    }
    Ok(())
}

fn decode_state(value: i64) -> rusqlite::Result<ProviderMessageState> {
    match value {
        1 => Ok(ProviderMessageState::Queued),
        2 => Ok(ProviderMessageState::EffectPending),
        3 => Ok(ProviderMessageState::Delivered),
        4 => Ok(ProviderMessageState::Failed),
        5 => Ok(ProviderMessageState::Unknown),
        _ => Err(to_sqlite_error(OperationsError::Corrupt(
            "provider message state",
        ))),
    }
}

fn encode_provider_family(value: AgentProfileProviderFamily) -> i64 {
    match value {
        AgentProfileProviderFamily::ChatGpt => 1,
    }
}

fn decode_provider_family(value: i64) -> rusqlite::Result<AgentProfileProviderFamily> {
    match value {
        1 => Ok(AgentProfileProviderFamily::ChatGpt),
        _ => Err(to_sqlite_error(OperationsError::Corrupt("provider family"))),
    }
}

fn encode_state(value: ProviderMessageState) -> i64 {
    match value {
        ProviderMessageState::Queued => 1,
        ProviderMessageState::EffectPending => 2,
        ProviderMessageState::Delivered => 3,
        ProviderMessageState::Failed => 4,
        ProviderMessageState::Unknown => 5,
    }
}

fn decode_uuid(value: Vec<u8>, field: &'static str) -> rusqlite::Result<Uuid> {
    let bytes: [u8; 16] = value
        .try_into()
        .map_err(|_| to_sqlite_error(OperationsError::Corrupt(field)))?;
    let value = Uuid::from_bytes(bytes);
    if value.is_nil() {
        return Err(to_sqlite_error(OperationsError::Corrupt(field)));
    }
    Ok(value)
}

fn decode_string(value: Vec<u8>, field: &'static str) -> rusqlite::Result<String> {
    String::from_utf8(value).map_err(|_| to_sqlite_error(OperationsError::Corrupt(field)))
}

fn decode_counter(value: i64, field: &'static str) -> rusqlite::Result<u64> {
    u64::try_from(value)
        .ok()
        .filter(|value| *value > 0)
        .ok_or_else(|| to_sqlite_error(OperationsError::Corrupt(field)))
}

fn to_sqlite_error(error: OperationsError) -> rusqlite::Error {
    rusqlite::Error::ToSqlConversionFailure(Box::new(error))
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::fs;

    use forge_domain::{TimestampMillis, WorkspaceId};
    use heterarchy_orchestration_sessions::{
        AgentProfileCatalog, AgentProfileCreationOrigin, AgentProfileDeletionCommand,
        AgentProfileDeletionIntent, AgentProfileId, AgentProfileProviderFamily,
        AgentProfileRuntimeSessionId, CreateAgentProfile,
    };
    use rusqlite::params;

    use super::*;

    struct Fixture {
        catalog: OperationsCatalog,
        path: std::path::PathBuf,
        destination: Uuid,
        workspace: WorkspaceId,
        profile_id: AgentProfileId,
        participation_id: WorkspaceParticipationId,
        runtime_session_id: AgentProfileRuntimeSessionId,
    }

    impl Fixture {
        fn target(&self) -> Result<ProviderMessageTarget, OperationsError> {
            ProviderMessageTarget::new(
                self.workspace,
                self.profile_id,
                self.participation_id,
                self.runtime_session_id,
                "conversation-1".to_owned(),
            )
        }

        fn cleanup(self) -> Result<(), Box<dyn Error>> {
            drop(self.catalog);
            fs::remove_file(self.path)?;
            Ok(())
        }
    }

    fn fixture() -> Result<Fixture, Box<dyn Error>> {
        let destination = Uuid::now_v7();
        let workspace = WorkspaceId::from_uuid(Uuid::now_v7());
        let profile_id = AgentProfileId::from_uuid(Uuid::now_v7())?;
        let participation_id = WorkspaceParticipationId::from_uuid(Uuid::now_v7())?;
        let runtime_session_id = AgentProfileRuntimeSessionId::from_uuid(Uuid::now_v7())?;
        let path = std::env::temp_dir().join(format!("operational-messages-{}", Uuid::now_v7()));
        let catalog = OperationsCatalog::open(&path, destination)?;
        catalog.connection.execute(
            "INSERT INTO ops_imports(migration_id,workspace_id,destination_id,source_revision,phase,next_cursor,page_count,record_count,import_digest,verified_pages,verify_digest) VALUES(?1,?2,?3,1,2,NULL,5,0,?4,5,?4)",
            params![
                Uuid::now_v7().as_bytes(),
                workspace.into_uuid().as_bytes(),
                destination.as_bytes(),
                [0_u8; 32].as_slice(),
            ],
        )?;
        catalog.connection.execute(
            "INSERT INTO ops_workspace_participations(workspace_id,agent_profile_id,participation_id,created_at_ms,revision) VALUES(?1,?2,?3,0,1)",
            params![
                workspace.into_uuid().as_bytes(),
                profile_id.into_uuid().as_bytes(),
                participation_id.into_uuid().as_bytes(),
            ],
        )?;
        Ok(Fixture {
            catalog,
            path,
            destination,
            workspace,
            profile_id,
            participation_id,
            runtime_session_id,
        })
    }

    fn time(value: i64) -> Result<TimestampMillis, OperationsError> {
        TimestampMillis::new(value).map_err(|_| OperationsError::Bounds("test timestamp"))
    }

    fn begin_profile_deletion_intent(
        profile_id: AgentProfileId,
        path: &std::path::Path,
        request_id: Uuid,
        requested_at: i64,
    ) -> Result<(AgentProfileCatalog, AgentProfileDeletionIntent), Box<dyn Error>> {
        let mut profiles = AgentProfileCatalog::open(path)?;
        let created = CreateAgentProfile::new(
            profile_id,
            Uuid::now_v7(),
            "profile-delete-fixture".to_owned(),
            "Profile deletion fixture".to_owned(),
            AgentProfileProviderFamily::ChatGpt,
            None,
            None,
            Vec::new(),
            AgentProfileCreationOrigin::UserManual,
            time(requested_at - 1)?,
        )?;
        let profile = profiles.create_profile(&created)?;
        let command = AgentProfileDeletionCommand::new(
            request_id,
            profile_id,
            profile.profile().revision(),
            time(requested_at)?,
        )?;
        let intent = profiles.begin_profile_deletion(&command)?;
        Ok((profiles, intent))
    }

    fn seed_node_recipient(
        fixture: &mut Fixture,
        context: MessageContext,
    ) -> Result<(), Box<dyn Error>> {
        let operation = Uuid::from_u128(0xC101);
        let participant = Uuid::from_u128(0xC110);
        let session = Uuid::from_u128(0xC104);
        let agent = Uuid::from_u128(0xC103);
        let activity = context.activity_id().ok_or("node Activity missing")?;
        let request = Uuid::from_u128(0xC111);
        let connection = &fixture.catalog.connection;
        connection.execute(
            "INSERT INTO ops_logical_members(workspace_id,participant_id,session_id,principal_kind,principal_id,display_name,declared_role,transport_kind,membership_revision,created_at_ms) VALUES(?1,?2,?3,0,NULL,'Node target','worker',1,1,1)",
            params![fixture.workspace.into_uuid().as_bytes(),participant.as_bytes(),session.as_bytes()],
        )?;
        connection.execute(
            "INSERT INTO ops_current_effect_bindings(workspace_id,participant_id,activity_id,session_id,agent_id,source_kind,source_request_id) VALUES(?1,?2,?3,?4,?5,0,?6)",
            params![fixture.workspace.into_uuid().as_bytes(),participant.as_bytes(),activity.into_uuid().as_bytes(),session.as_bytes(),agent.as_bytes(),operation.as_bytes()],
        )?;
        connection.execute(
            "INSERT INTO ops_workspace_participation_mappings(request_id,workspace_id,participation_id,agent_profile_id,profile_revision,profile_mapping_request_id,session_id,session_revision,participant_id,participant_membership_revision,mapped_at_ms) VALUES(?1,?2,?3,?4,1,?5,?6,1,?7,1,1)",
            params![Uuid::from_u128(0xC112).as_bytes(),fixture.workspace.into_uuid().as_bytes(),fixture.participation_id.into_uuid().as_bytes(),fixture.profile_id.into_uuid().as_bytes(),operation.as_bytes(),session.as_bytes(),participant.as_bytes()],
        )?;
        connection.execute(
            "INSERT INTO ops_agent_requests(request_id,workspace_id,activity_id,requester_agent_id,requester_session_id,coordinator_revision,objective,capability,requested_count,effect_scope,independence_requirement,reason,state,created_at_ms,revision) VALUES(?1,?2,?3,?4,?5,1,'node','worker',1,0,0,'node conversation',1,1,1)",
            params![request.as_bytes(),fixture.workspace.into_uuid().as_bytes(),activity.into_uuid().as_bytes(),agent.as_bytes(),session.as_bytes()],
        )?;
        connection.execute(
            "INSERT INTO ops_agent_request_resolutions(request_id,slot_ordinal,resolution_kind,claim_id,profile_id,profile_revision,runtime_session_id,runtime_session_revision,policy_revision,reserved_at_ms) VALUES(?1,0,2,?2,?3,1,?4,1,1,1)",
            params![request.as_bytes(),Uuid::from_u128(0xC113).as_bytes(),fixture.profile_id.into_uuid().as_bytes(),fixture.runtime_session_id.into_uuid().as_bytes()],
        )?;
        connection.execute(
            "INSERT INTO ops_agent_provision_operations(operation_id,request_id,slot_ordinal,effect_kind,provider_family,profile_id,runtime_session_id,state,provider_conversation_id,prepared_at_ms,updated_at_ms,revision) VALUES(?1,?2,0,1,1,?3,?4,5,'conversation-1',1,1,1)",
            params![operation.as_bytes(),request.as_bytes(),fixture.profile_id.into_uuid().as_bytes(),fixture.runtime_session_id.into_uuid().as_bytes()],
        )?;
        let scope = crate::NodeTeamBindingScope::new(
            fixture.workspace,
            activity,
            context.goal_id().ok_or("node Goal missing")?,
            context.goal_created_at().ok_or("node creation missing")?,
            context.dag_revision().ok_or("node DAG missing")?,
            context
                .dag_base_goal_revision()
                .ok_or("node DAG base missing")?,
            context.node_id().ok_or("node ID missing")?,
        )?;
        let _receipt =
            fixture
                .catalog
                .record_assignment_affinity(&crate::AssignmentAffinityCommand {
                    workspace_id: fixture.workspace,
                    assignment_id: forge_domain::AgentAssignmentId::from_uuid(Uuid::from_u128(
                        0xC102,
                    )),
                    activity_id: activity,
                    target_agent_id: forge_domain::AgentInstanceId::from_uuid(agent),
                    target_session_id: forge_domain::AgentSessionId::from_uuid(session),
                    requester: OperationalActor::LocalOperator,
                    team_id: None,
                    assignment_payload_fingerprint: [0xC1; 32],
                    original_offer_payload: b"fixture-offer".to_vec(),
                    observed_at: time(100)?,
                    node_scope: scope,
                    expected_binding_revision: None,
                })?;
        Ok(())
    }

    fn confirm_node_workspace_leader(
        fixture: &mut Fixture,
        context: MessageContext,
    ) -> Result<i64, Box<dyn Error>> {
        let activity_id = context.activity_id().ok_or("node Activity missing")?;
        let assignment_id = forge_domain::AgentAssignmentId::from_uuid(Uuid::from_u128(0xC102));
        let agent_id = forge_domain::AgentInstanceId::from_uuid(Uuid::from_u128(0xC103));
        let session_id = forge_domain::AgentSessionId::from_uuid(Uuid::from_u128(0xC104));
        let migration_id: Vec<u8> = fixture.catalog.connection.query_row(
            "SELECT migration_id FROM ops_imports WHERE workspace_id=?1 AND phase=2 LIMIT 1",
            params![fixture.workspace.into_uuid().as_bytes()],
            |row| row.get(0),
        )?;
        fixture.catalog.connection.execute(
            "INSERT INTO ops_effect_fact_consumers(migration_id,workspace_id,destination_id,last_sequence,applied_count) VALUES(?1,?2,?3,0,0)",
            params![
                migration_id.as_slice(),
                fixture.workspace.into_uuid().as_bytes(),
                fixture.destination.as_bytes(),
            ],
        )?;
        fixture.catalog.connection.execute(
            "INSERT INTO ops_assignment_offer_projection(migration_id,source_sequence,fact_digest,assignment_id,workspace_id,activity_id,goal_id,node_id,target_agent_id,target_session_id,credit_budget,offered_at_ms) VALUES(?1,1,?2,?3,?4,?5,?6,?7,?8,?9,NULL,100)",
            params![
                migration_id.as_slice(),
                [0xC1_u8; 32].as_slice(),
                assignment_id.into_uuid().as_bytes(),
                fixture.workspace.into_uuid().as_bytes(),
                activity_id.into_uuid().as_bytes(),
                context.goal_id().ok_or("node Goal missing")?.into_uuid().as_bytes(),
                context.node_id().ok_or("node ID missing")?.into_uuid().as_bytes(),
                agent_id.into_uuid().as_bytes(),
                session_id.into_uuid().as_bytes(),
            ],
        )?;
        {
            let transaction = fixture.catalog.connection.transaction()?;
            crate::leadership::bootstrap_from_offer(
                &transaction,
                crate::leadership::OfferBootstrapObservation {
                    workspace_id: fixture.workspace,
                    activity_id,
                    assignment_id,
                    target_agent_id: agent_id,
                    offered_at_ms: 100,
                },
            )?;
            transaction.commit()?;
        }
        let scope = crate::LeadershipScope::workspace(fixture.workspace);
        let bootstrapped = fixture
            .catalog
            .leadership_scope_view(scope, 100)?
            .ok_or("Workspace leadership bootstrap missing")?;
        assert_eq!(bootstrapped.state(), crate::LeadershipState::Bootstrapping);
        let confirmed =
            fixture
                .catalog
                .confirm_bootstrap_evidence(crate::ConfirmBootstrapEvidenceCommand {
                    scope,
                    assignment_id,
                    expected_revision: bootstrapped.leadership_revision(),
                    now_ms: 101,
                })?;
        assert_eq!(confirmed.state(), crate::LeadershipState::Stabilizing);
        let authority = fixture
            .catalog
            .current_leadership_authority(scope)?
            .ok_or("confirmed Workspace leader authority missing")?;
        assert_eq!(authority.participant_id(), Uuid::from_u128(0xC110));
        assert_eq!(
            authority.leadership_revision(),
            confirmed.leadership_revision()
        );
        Ok(authority.leadership_revision())
    }

    fn append(
        catalog: &mut OperationsCatalog,
        workspace: WorkspaceId,
        message_id: MessageId,
        body: &str,
        submitted_at: i64,
    ) -> Result<MessageAppendOutcome, OperationsError> {
        append_with_context(
            catalog,
            workspace,
            message_id,
            body,
            MessageChannel::Workspace,
            MessageContext::default(),
            submitted_at,
        )
    }

    fn append_with_context(
        catalog: &mut OperationsCatalog,
        workspace: WorkspaceId,
        message_id: MessageId,
        body: &str,
        channel: MessageChannel,
        context: MessageContext,
        submitted_at: i64,
    ) -> Result<MessageAppendOutcome, OperationsError> {
        catalog.append_workspace_message(AppendWorkspaceMessageCommand {
            message_id,
            workspace_id: workspace,
            channel,
            actor: OperationalActor::LocalOperator,
            body: body.to_owned(),
            submitted_at: time(submitted_at)?,
            context,
        })
    }

    fn append_and_enqueue(
        catalog: &mut OperationsCatalog,
        workspace: WorkspaceId,
        message_id: MessageId,
        body: &str,
        context: MessageContext,
        target: ProviderMessageTarget,
        submitted_at: i64,
    ) -> Result<ProviderMessageQueueOutcome, OperationsError> {
        catalog.append_and_enqueue_provider_message(
            AppendWorkspaceMessageCommand {
                message_id,
                workspace_id: workspace,
                channel: MessageChannel::Workspace,
                actor: OperationalActor::LocalOperator,
                body: body.to_owned(),
                submitted_at: time(submitted_at)?,
                context,
            },
            target,
            None,
        )
    }

    fn insert_recipient(
        fixture: &Fixture,
    ) -> Result<
        (
            AgentProfileId,
            WorkspaceParticipationId,
            AgentProfileRuntimeSessionId,
        ),
        Box<dyn Error>,
    > {
        let profile_id = AgentProfileId::from_uuid(Uuid::now_v7())?;
        let participation_id = WorkspaceParticipationId::from_uuid(Uuid::now_v7())?;
        let runtime_session_id = AgentProfileRuntimeSessionId::from_uuid(Uuid::now_v7())?;
        fixture.catalog.connection.execute(
            "INSERT INTO ops_workspace_participations(workspace_id,agent_profile_id,participation_id,created_at_ms,revision) VALUES(?1,?2,?3,0,1)",
            params![
                fixture.workspace.into_uuid().as_bytes(),
                profile_id.into_uuid().as_bytes(),
                participation_id.into_uuid().as_bytes(),
            ],
        )?;
        Ok((profile_id, participation_id, runtime_session_id))
    }

    #[allow(clippy::too_many_arguments)]
    fn incoming(
        catalog: &mut OperationsCatalog,
        message_id: MessageId,
        workspace: WorkspaceId,
        target: ProviderMessageTarget,
        source: ProviderMessageSource,
        turn_id: &str,
        body: &str,
        submitted_at: i64,
        context: MessageContext,
    ) -> Result<IncomingProviderMessageOutcome, OperationsError> {
        catalog.record_incoming_provider_message(RecordIncomingProviderMessageCommand {
            message_id,
            workspace_id: workspace,
            channel: MessageChannel::Workspace,
            target,
            source,
            provider_turn_id: turn_id.to_owned(),
            body: body.to_owned(),
            submitted_at: time(submitted_at)?,
            context,
        })
    }

    #[test]
    fn append_replay_and_reopen_round_trip() -> Result<(), Box<dyn Error>> {
        let mut fixture = fixture()?;
        let message_id = Uuid::now_v7();
        let first = append(
            &mut fixture.catalog,
            fixture.workspace,
            message_id,
            "hello",
            10,
        )?;
        assert!(first.created());
        let replay = append(
            &mut fixture.catalog,
            fixture.workspace,
            message_id,
            "hello",
            10,
        )?;
        assert!(!replay.created());
        assert_eq!(replay.message(), first.message());
        drop(fixture.catalog);
        let reopened = OperationsCatalog::open(&fixture.path, fixture.destination)?;
        let stored = reopened
            .workspace_message(message_id)?
            .ok_or("message missing")?;
        assert_eq!(stored.body(), "hello");
        assert_eq!(stored.sequence(), 1);
        drop(reopened);
        fs::remove_file(fixture.path)?;
        Ok(())
    }

    #[allow(
        clippy::too_many_lines,
        reason = "One durable fixture proves append rollback, exact node route replay, reopen and corrupt witness rejection."
    )]
    #[test]
    fn append_and_enqueue_is_atomic_and_replays_by_message_identity() -> Result<(), Box<dyn Error>>
    {
        let mut fixture = fixture()?;
        let target = fixture.target()?;
        let message_id = Uuid::now_v7();
        let first = append_and_enqueue(
            &mut fixture.catalog,
            fixture.workspace,
            message_id,
            "hello",
            MessageContext::default(),
            target.clone(),
            10,
        )?;
        assert!(first.created());
        let replay = append_and_enqueue(
            &mut fixture.catalog,
            fixture.workspace,
            message_id,
            "hello",
            MessageContext::default(),
            target,
            10,
        )?;
        assert!(!replay.created());
        assert_eq!(replay.outbox(), first.outbox());

        let wrong_target = ProviderMessageTarget::new(
            fixture.workspace,
            fixture.profile_id,
            WorkspaceParticipationId::from_uuid(Uuid::now_v7())?,
            fixture.runtime_session_id,
            "conversation-rollback".to_owned(),
        )?;
        let rejected = append_and_enqueue(
            &mut fixture.catalog,
            fixture.workspace,
            Uuid::now_v7(),
            "must rollback",
            MessageContext::default(),
            wrong_target,
            11,
        );
        assert!(rejected.is_err());
        let messages = fixture.catalog.read_workspace_messages(
            fixture.workspace,
            MessageReadScope::Channel(MessageChannel::Workspace),
            None,
            MAX_MESSAGE_PAGE_SIZE,
        )?;
        assert_eq!(messages.messages().len(), 1);
        // Node routing is captured before delivery and survives exact replay.
        let witness = NodeMessageRouteWitness::new(
            Uuid::from_u128(0xC101),
            1,
            2,
            OperationalActor::LocalOperator,
            None,
            NodeMessageRouteTarget::Agent {
                assignment_id: Uuid::from_u128(0xC102),
                assignment_revision: 3,
                agent_id: Uuid::from_u128(0xC103),
                session_id: Uuid::from_u128(0xC104),
            },
        )?;
        assert!(
            fixture
                .catalog
                .enqueue_provider_message(EnqueueProviderMessageCommand {
                    message_id,
                    target: first.outbox().target().clone(),
                    queued_at: time(12)?,
                    node_route_witness: Some(witness.clone()),
                })
                .is_err()
        );
        let command = AppendWorkspaceMessageCommand {
            message_id: Uuid::from_u128(0xC105),
            workspace_id: fixture.workspace,
            channel: MessageChannel::Direct(fixture.participation_id),
            actor: OperationalActor::LocalOperator,
            body: "steer exact node".to_owned(),
            submitted_at: time(112)?,
            context: MessageContext::new(
                Some(ActivityId::from_uuid(Uuid::from_u128(0xC106))),
                Some(GoalId::from_uuid(Uuid::from_u128(0xC107))),
                Some(time(99)?),
                Some(DagRevisionNumber::INITIAL),
                Some(GoalRevision::INITIAL),
                Some(DagNodeId::from_uuid(Uuid::from_u128(0xC108))),
            )?,
        };
        assert!(matches!(
            fixture.catalog.append_and_enqueue_provider_message(
                command.clone(),
                fixture.target()?,
                None,
            ),
            Err(OperationsError::Conflict(
                "fresh node message requires exact route witness"
            ))
        ));
        assert!(
            fixture
                .catalog
                .workspace_message(command.message_id)?
                .is_none()
        );
        seed_node_recipient(&mut fixture, command.context)?;
        let sender_actor = OperationalActor::Participant {
            activity_id: command.context.activity_id().ok_or("Activity missing")?,
            agent_id: forge_domain::AgentInstanceId::from_uuid(Uuid::from_u128(0xC103)),
            session_id: forge_domain::AgentSessionId::from_uuid(Uuid::from_u128(0xC104)),
        };
        let mut unauthorized = command.clone();
        unauthorized.message_id = Uuid::now_v7();
        unauthorized.actor = sender_actor;
        let unauthorized_witness = NodeMessageRouteWitness::new(
            witness.operation_id(),
            witness.participation_revision(),
            witness.runtime_revision(),
            sender_actor,
            Some(1),
            witness.target(),
        )?;
        assert!(matches!(
            fixture.catalog.append_and_enqueue_provider_message(
                unauthorized.clone(),
                fixture.target()?,
                Some(unauthorized_witness),
            ),
            Err(OperationsError::Conflict("node message sender leadership"))
        ));
        assert!(
            fixture
                .catalog
                .workspace_message(unauthorized.message_id)?
                .is_none()
        );
        let sender_leadership_revision =
            confirm_node_workspace_leader(&mut fixture, command.context)?;
        let mut participant_command = command.clone();
        participant_command.actor = sender_actor;
        let participant_witness = NodeMessageRouteWitness::new(
            witness.operation_id(),
            witness.participation_revision(),
            witness.runtime_revision(),
            sender_actor,
            Some(sender_leadership_revision),
            witness.target(),
        )?;
        let node_first = fixture.catalog.append_and_enqueue_provider_message(
            participant_command.clone(),
            fixture.target()?,
            Some(participant_witness.clone()),
        )?;
        assert_eq!(
            node_first.outbox().node_route_witness(),
            Some(&participant_witness)
        );
        assert!(node_first.outbox().receipt().is_none());
        // The sender's Ready binding also sustains the Workspace leader proof.
        // Removing it must not change an exact replay's durable witness/outbox.
        fixture.catalog.connection.execute(
            "DELETE FROM ops_current_effect_bindings WHERE workspace_id=?1",
            params![fixture.workspace.into_uuid().as_bytes()],
        )?;
        let mut stale = participant_command.clone();
        stale.message_id = Uuid::now_v7();
        assert!(
            fixture
                .catalog
                .append_and_enqueue_provider_message(
                    stale.clone(),
                    fixture.target()?,
                    Some(participant_witness.clone()),
                )
                .is_err()
        );
        assert!(
            fixture
                .catalog
                .workspace_message(stale.message_id)?
                .is_none()
        );
        let node_replay = fixture.catalog.append_and_enqueue_provider_message(
            participant_command.clone(),
            fixture.target()?,
            Some(participant_witness.clone()),
        )?;
        assert!(!node_replay.created());
        assert_eq!(node_replay.outbox(), node_first.outbox());

        let mut changed_actor_command = participant_command.clone();
        changed_actor_command.actor = OperationalActor::Participant {
            activity_id: ActivityId::from_uuid(Uuid::from_u128(0xC120)),
            agent_id: forge_domain::AgentInstanceId::from_uuid(Uuid::from_u128(0xC121)),
            session_id: forge_domain::AgentSessionId::from_uuid(Uuid::from_u128(0xC104)),
        };
        assert!(matches!(
            fixture.catalog.append_and_enqueue_provider_message(
                changed_actor_command,
                fixture.target()?,
                Some(participant_witness.clone()),
            ),
            Err(OperationsError::Conflict("node message sender replay"))
        ));
        assert!(
            fixture
                .catalog
                .append_and_enqueue_provider_message(
                    participant_command.clone(),
                    fixture.target()?,
                    None,
                )
                .is_err()
        );
        let changed_witness = NodeMessageRouteWitness::new(
            participant_witness.operation_id(),
            participant_witness.participation_revision(),
            participant_witness.runtime_revision(),
            sender_actor,
            Some(sender_leadership_revision),
            NodeMessageRouteTarget::Team {
                team_id: Uuid::from_u128(0xC109),
                binding_revision: 1,
                leadership_revision: 4,
            },
        )?;
        assert!(
            fixture
                .catalog
                .append_and_enqueue_provider_message(
                    participant_command.clone(),
                    fixture.target()?,
                    Some(changed_witness),
                )
                .is_err()
        );
        let reopened = OperationsCatalog::open(&fixture.path, fixture.catalog.destination_id())?;
        assert_eq!(
            reopened
                .provider_message_outbox(command.message_id)?
                .ok_or("node outbox missing after reopen")?
                .node_route_witness(),
            Some(&participant_witness)
        );
        drop(reopened);
        fixture
            .catalog
            .connection
            .execute_batch("SAVEPOINT corrupt_node_route")?;
        fixture.catalog.connection.execute(
            "UPDATE ops_provider_message_outbox SET node_route_witness=X'00' WHERE message_id=?1",
            params![command.message_id.as_bytes()],
        )?;
        assert!(
            fixture
                .catalog
                .provider_message_outbox(command.message_id)
                .is_err()
        );
        fixture
            .catalog
            .connection
            .execute_batch("ROLLBACK TO corrupt_node_route; RELEASE corrupt_node_route")?;
        fixture.cleanup()?;
        Ok(())
    }

    #[test]
    fn provider_text_formatter_preserves_plain_body_and_adds_exact_context()
    -> Result<(), Box<dyn Error>> {
        let mut fixture = fixture()?;
        let plain_id = Uuid::now_v7();
        append(
            &mut fixture.catalog,
            fixture.workspace,
            plain_id,
            "plain",
            10,
        )?;
        let plain = fixture
            .catalog
            .workspace_message(plain_id)?
            .ok_or("plain message missing")?;
        assert_eq!(provider_message_text(&plain)?, "plain");
        assert!(MessageContext::new(None, None, Some(time(1)?), None, None, None).is_err());
        assert!(
            MessageContext::new(None, None, None, None, Some(GoalRevision::INITIAL), None).is_err()
        );

        let goal = GoalId::from_uuid(Uuid::from_u128(0xAA01));
        let node = DagNodeId::from_uuid(Uuid::from_u128(0xAA02));
        let activity = ActivityId::from_uuid(Uuid::from_u128(0xAA03));
        let goal_created_at = time(123)?;
        let context = MessageContext::new(
            Some(activity),
            Some(goal),
            Some(goal_created_at),
            Some(DagRevisionNumber::INITIAL),
            Some(GoalRevision::INITIAL),
            Some(node),
        )?;
        let contextual_id = Uuid::now_v7();
        append(
            &mut fixture.catalog,
            fixture.workspace,
            contextual_id,
            "body",
            11,
        )?;
        fixture.catalog.connection.execute(
            "UPDATE ops_workspace_messages SET activity_id=?1, goal_id=?2, dag_revision=?3, node_id=?4, goal_created_at_ms=?5, dag_base_goal_revision=?6 WHERE message_id=?7",
            params![
                context.activity_id().map(|id| id.into_uuid().into_bytes()),
                context.goal_id().map(|id| id.into_uuid().into_bytes()),
                context.dag_revision().map(DagRevisionNumber::get),
                context.node_id().map(|id| id.into_uuid().into_bytes()),
                context.goal_created_at().map(TimestampMillis::get),
                context.dag_base_goal_revision().map(GoalRevision::get),
                contextual_id.as_bytes(),
            ],
        )?;
        let contextual = fixture
            .catalog
            .workspace_message(contextual_id)?
            .ok_or("contextual message missing")?;
        assert_eq!(
            provider_message_text(&contextual)?,
            format!(
                "[Heterarchy context]\nWorkspace: {}\nGoal: {}\nGoal created at (ms): {}\nDAG revision: 1\nDAG base Goal revision: 1\nNode: {}\n\nbody",
                fixture.workspace,
                goal,
                goal_created_at.get(),
                node
            )
        );
        let without_generation = MessageContext::new(
            Some(activity),
            Some(goal),
            None,
            Some(DagRevisionNumber::INITIAL),
            None,
            Some(node),
        )?;
        let without_generation_id = Uuid::now_v7();
        append_with_context(
            &mut fixture.catalog,
            fixture.workspace,
            without_generation_id,
            "legacy context",
            MessageChannel::Workspace,
            without_generation,
            12,
        )?;
        let without_generation = fixture
            .catalog
            .workspace_message(without_generation_id)?
            .ok_or("generation-free context missing")?;
        assert_eq!(
            provider_message_text(&without_generation)?,
            format!(
                "[Heterarchy context]\nWorkspace: {}\nGoal: {}\nDAG revision: 1\nNode: {}\n\nlegacy context",
                fixture.workspace, goal, node
            )
        );
        fixture.cleanup()?;
        Ok(())
    }

    #[test]
    fn provider_receipt_must_match_formatted_context_body() -> Result<(), Box<dyn Error>> {
        let mut fixture = fixture()?;
        let target = fixture.target()?;
        let context = MessageContext::new(
            Some(ActivityId::from_uuid(Uuid::from_u128(0xAB03))),
            Some(GoalId::from_uuid(Uuid::from_u128(0xAB01))),
            Some(time(321)?),
            Some(DagRevisionNumber::INITIAL),
            Some(GoalRevision::INITIAL),
            Some(DagNodeId::from_uuid(Uuid::from_u128(0xAB02))),
        )?;
        let message_id = Uuid::now_v7();
        seed_node_recipient(&mut fixture, context)?;
        let queued = fixture.catalog.append_and_enqueue_provider_message(
            AppendWorkspaceMessageCommand {
                message_id,
                workspace_id: fixture.workspace,
                channel: MessageChannel::Direct(fixture.participation_id),
                actor: OperationalActor::LocalOperator,
                body: "body".to_owned(),
                submitted_at: time(10)?,
                context,
            },
            target.clone(),
            Some(NodeMessageRouteWitness::new(
                Uuid::from_u128(0xC101),
                1,
                2,
                OperationalActor::LocalOperator,
                None,
                NodeMessageRouteTarget::Agent {
                    assignment_id: Uuid::from_u128(0xC102),
                    assignment_revision: 3,
                    agent_id: Uuid::from_u128(0xC103),
                    session_id: Uuid::from_u128(0xC104),
                },
            )?),
        )?;
        let execute = fixture
            .catalog
            .next_provider_message_dispatch(time(11)?)?
            .ok_or("missing provider execute")?;
        assert_eq!(execute.outbox().revision(), queued.outbox().revision() + 1);
        let raw_receipt = ProviderMessageDeliveryReceipt::new(
            target.clone(),
            "turn-raw".to_owned(),
            "body".to_owned(),
        )?;
        assert!(
            fixture
                .catalog
                .confirm_provider_message_delivery(
                    message_id,
                    execute.outbox().revision(),
                    &raw_receipt,
                    time(12)?,
                )
                .is_err()
        );
        let message = fixture
            .catalog
            .workspace_message(message_id)?
            .ok_or("message missing")?;
        let formatted = provider_message_text(&message)?;
        let receipt =
            ProviderMessageDeliveryReceipt::new(target, "turn-formatted".to_owned(), formatted)?;
        let delivered = fixture.catalog.confirm_provider_message_delivery(
            message_id,
            execute.outbox().revision(),
            &receipt,
            time(13)?,
        )?;
        assert_eq!(delivered.state(), ProviderMessageState::Delivered);
        fixture.cleanup()?;
        Ok(())
    }

    #[test]
    fn formatted_provider_body_bound_rejects_before_atomic_enqueue() -> Result<(), Box<dyn Error>> {
        let mut fixture = fixture()?;
        let context = MessageContext::new(
            None,
            Some(GoalId::from_uuid(Uuid::from_u128(0xAC01))),
            None,
            Some(DagRevisionNumber::INITIAL),
            None,
            Some(DagNodeId::from_uuid(Uuid::from_u128(0xAC02))),
        )?;
        let target = fixture.target()?;
        let rejected = append_and_enqueue(
            &mut fixture.catalog,
            fixture.workspace,
            Uuid::now_v7(),
            &"x".repeat(MAX_MESSAGE_BODY_BYTES - 1),
            context,
            target,
            10,
        );
        assert!(matches!(
            rejected,
            Err(OperationsError::Bounds("message body"))
        ));
        let page = fixture.catalog.read_workspace_messages(
            fixture.workspace,
            MessageReadScope::Channel(MessageChannel::Workspace),
            None,
            MAX_MESSAGE_PAGE_SIZE,
        )?;
        assert!(page.messages().is_empty());
        fixture.cleanup()?;
        Ok(())
    }

    #[test]
    fn changed_replay_and_direct_scope_are_rejected() -> Result<(), Box<dyn Error>> {
        let mut fixture = fixture()?;
        let message_id = Uuid::now_v7();
        append(
            &mut fixture.catalog,
            fixture.workspace,
            message_id,
            "hello",
            10,
        )?;
        let changed = append(
            &mut fixture.catalog,
            fixture.workspace,
            message_id,
            "changed",
            10,
        );
        assert!(changed.is_err());
        let other_participation = WorkspaceParticipationId::from_uuid(Uuid::now_v7())?;
        fixture.catalog.connection.execute(
            "INSERT INTO ops_workspace_participations(workspace_id,agent_profile_id,participation_id,created_at_ms,revision) VALUES(?1,?2,?3,0,1)",
            params![
                fixture.workspace.into_uuid().as_bytes(),
                Uuid::now_v7().as_bytes(),
                other_participation.into_uuid().as_bytes(),
            ],
        )?;
        let changed_scope =
            fixture
                .catalog
                .append_workspace_message(AppendWorkspaceMessageCommand {
                    message_id,
                    workspace_id: fixture.workspace,
                    channel: MessageChannel::Direct(other_participation),
                    actor: OperationalActor::LocalOperator,
                    body: "hello".to_owned(),
                    submitted_at: time(10)?,
                    context: MessageContext::default(),
                });
        assert!(changed_scope.is_err());
        fixture.cleanup()?;
        Ok(())
    }

    #[test]
    fn sequence_cursor_has_more_is_stable_and_bounded() -> Result<(), Box<dyn Error>> {
        let mut fixture = fixture()?;
        for (index, body) in ["one", "two", "three"].into_iter().enumerate() {
            append(
                &mut fixture.catalog,
                fixture.workspace,
                Uuid::now_v7(),
                body,
                i64::try_from(index + 1)?,
            )?;
        }
        let first = fixture.catalog.read_workspace_messages(
            fixture.workspace,
            MessageReadScope::Channel(MessageChannel::Workspace),
            None,
            2,
        )?;
        assert_eq!(first.messages().len(), 2);
        assert!(first.has_more());
        assert_eq!(first.next_cursor(), Some(2));
        let second = fixture.catalog.read_workspace_messages(
            fixture.workspace,
            MessageReadScope::Channel(MessageChannel::Workspace),
            first.next_cursor(),
            MAX_MESSAGE_PAGE_SIZE,
        )?;
        assert_eq!(second.messages().len(), 1);
        assert!(!second.has_more());
        assert_eq!(second.messages()[0].body(), "three");
        assert!(
            fixture
                .catalog
                .read_workspace_messages(
                    fixture.workspace,
                    MessageReadScope::Channel(MessageChannel::Workspace),
                    None,
                    MAX_MESSAGE_PAGE_SIZE + 1,
                )
                .is_err()
        );
        fixture.cleanup()?;
        Ok(())
    }

    #[allow(
        clippy::too_many_lines,
        reason = "One interleaved history fixture proves scope isolation, recipient changes and both cursor directions."
    )]
    #[test]
    fn latest_messages_filter_exact_context_before_paging() -> Result<(), Box<dyn Error>> {
        let mut fixture = fixture()?;
        let activity = ActivityId::from_uuid(Uuid::from_u128(0xC001));
        let goal = GoalId::from_uuid(Uuid::from_u128(0xC002));
        let other_goal = GoalId::from_uuid(Uuid::from_u128(0xC003));
        let node = DagNodeId::from_uuid(Uuid::from_u128(0xC004));
        let goal_created_at = time(100)?;
        let base_goal_revision = GoalRevision::INITIAL;
        let other_base_goal_revision = GoalRevision::new(2)
            .map_err(|_| OperationsError::Conflict("test base Goal revision"))?;
        let context = MessageContext::new(
            Some(activity),
            Some(goal),
            Some(goal_created_at),
            Some(DagRevisionNumber::INITIAL),
            Some(base_goal_revision),
            Some(node),
        )?;
        let same_node_other_goal = MessageContext::new(
            Some(activity),
            Some(other_goal),
            Some(goal_created_at),
            Some(DagRevisionNumber::INITIAL),
            Some(base_goal_revision),
            Some(node),
        )?;
        let same_node_other_generation = MessageContext::new(
            Some(activity),
            Some(goal),
            Some(time(101)?),
            Some(DagRevisionNumber::INITIAL),
            Some(base_goal_revision),
            Some(node),
        )?;
        let same_node_other_base = MessageContext::new(
            Some(activity),
            Some(goal),
            Some(goal_created_at),
            Some(DagRevisionNumber::INITIAL),
            Some(other_base_goal_revision),
            Some(node),
        )?;
        let legacy_generation_missing = MessageContext::new(
            Some(activity),
            Some(goal),
            None,
            Some(DagRevisionNumber::INITIAL),
            Some(base_goal_revision),
            Some(node),
        )?;
        let missing_base_revision = MessageContext::new(
            Some(activity),
            Some(goal),
            Some(goal_created_at),
            Some(DagRevisionNumber::INITIAL),
            None,
            Some(node),
        )?;
        let later_dag = DagRevisionNumber::new(2)
            .map_err(|_| OperationsError::Conflict("test DAG revision"))?;
        let same_node_other_dag = MessageContext::new(
            Some(activity),
            Some(goal),
            Some(goal_created_at),
            Some(later_dag),
            Some(base_goal_revision),
            Some(node),
        )?;

        let (_, next_participation, _) = insert_recipient(&fixture)?;
        let previous_channel = MessageChannel::Direct(fixture.participation_id);
        let next_channel = MessageChannel::Direct(next_participation);
        for (id, body, context, submitted_at, channel) in [
            (0xC010, "target-one", context, 1, previous_channel),
            (
                0xC011,
                "other-goal",
                same_node_other_goal,
                2,
                MessageChannel::Workspace,
            ),
            (
                0xC012,
                "other-dag",
                same_node_other_dag,
                3,
                MessageChannel::Workspace,
            ),
            (0xC013, "target-two", context, 4, next_channel),
            (
                0xC016,
                "other-generation",
                same_node_other_generation,
                5,
                MessageChannel::Workspace,
            ),
            (
                0xC018,
                "other-base-goal-revision",
                same_node_other_base,
                6,
                MessageChannel::Workspace,
            ),
            (
                0xC017,
                "legacy-generation-null",
                legacy_generation_missing,
                7,
                MessageChannel::Workspace,
            ),
            (
                0xC014,
                "unscoped",
                MessageContext::default(),
                8,
                MessageChannel::Workspace,
            ),
            (0xC015, "target-three", context, 9, next_channel),
        ] {
            append_with_context(
                &mut fixture.catalog,
                fixture.workspace,
                Uuid::from_u128(id),
                body,
                channel,
                context,
                submitted_at,
            )?;
        }

        let first = fixture.catalog.read_workspace_messages(
            fixture.workspace,
            MessageReadScope::Node(context),
            None,
            2,
        )?;
        assert_eq!(
            first
                .messages()
                .iter()
                .map(WorkspaceMessage::body)
                .collect::<Vec<_>>(),
            vec!["target-one", "target-two"]
        );
        assert!(first.has_more());
        assert_eq!(first.next_cursor(), Some(4));
        let second = fixture.catalog.read_workspace_messages(
            fixture.workspace,
            MessageReadScope::Node(context),
            first.next_cursor(),
            2,
        )?;
        assert_eq!(second.messages().len(), 1);
        assert_eq!(second.messages()[0].body(), "target-three");
        assert!(!second.has_more());
        assert_eq!(second.next_cursor(), None);

        let latest = fixture.catalog.workspace_messages_latest(
            fixture.workspace,
            MessageReadScope::Node(context),
            None,
            2,
        )?;
        assert_eq!(
            latest
                .messages()
                .iter()
                .map(WorkspaceMessage::body)
                .collect::<Vec<_>>(),
            vec!["target-two", "target-three"]
        );
        assert!(latest.has_more());
        assert_eq!(latest.next_cursor(), Some(4));
        let older = fixture.catalog.workspace_messages_latest(
            fixture.workspace,
            MessageReadScope::Node(context),
            latest.next_cursor(),
            2,
        )?;
        assert_eq!(older.messages().len(), 1);
        assert_eq!(older.messages()[0].body(), "target-one");
        assert!(!older.has_more());
        assert_eq!(older.next_cursor(), None);
        assert!(
            fixture
                .catalog
                .workspace_messages_latest(
                    fixture.workspace,
                    MessageReadScope::Node(context),
                    Some(0),
                    2,
                )
                .is_err()
        );
        assert!(
            fixture
                .catalog
                .workspace_messages_latest(
                    fixture.workspace,
                    MessageReadScope::Node(context),
                    Some(u64::MAX),
                    2,
                )
                .is_err()
        );
        let largest_cursor = fixture.catalog.workspace_messages_latest(
            fixture.workspace,
            MessageReadScope::Node(context),
            Some(i64::MAX as u64),
            2,
        )?;
        assert_eq!(
            largest_cursor
                .messages()
                .iter()
                .map(WorkspaceMessage::body)
                .collect::<Vec<_>>(),
            vec!["target-two", "target-three"]
        );

        let aggregate = fixture.catalog.workspace_messages_latest(
            fixture.workspace,
            MessageReadScope::Channel(MessageChannel::Workspace),
            None,
            MAX_MESSAGE_PAGE_SIZE,
        )?;
        assert!(!aggregate.has_more());
        assert_eq!(aggregate.messages().len(), 6);
        assert!(
            aggregate
                .messages()
                .iter()
                .any(|message| message.body() == "legacy-generation-null")
        );
        let previous = fixture.catalog.workspace_messages_latest(
            fixture.workspace,
            MessageReadScope::Channel(previous_channel),
            None,
            8,
        )?;
        assert_eq!(previous.messages().len(), 1);
        assert_eq!(previous.messages()[0].body(), "target-one");
        assert!(
            fixture
                .catalog
                .workspace_messages_latest(
                    fixture.workspace,
                    MessageReadScope::Node(MessageContext::default()),
                    None,
                    8,
                )
                .is_err()
        );
        assert!(
            fixture
                .catalog
                .workspace_messages_latest(
                    fixture.workspace,
                    MessageReadScope::Node(legacy_generation_missing),
                    None,
                    8,
                )
                .is_err()
        );
        assert!(
            fixture
                .catalog
                .workspace_messages_latest(
                    fixture.workspace,
                    MessageReadScope::Node(missing_base_revision),
                    None,
                    8,
                )
                .is_err()
        );
        fixture.cleanup()?;
        Ok(())
    }

    #[test]
    fn dispatch_fairness_blocks_same_runtime_but_allows_other_runtime() -> Result<(), Box<dyn Error>>
    {
        let mut fixture = fixture()?;
        let (profile_b, participation_b, runtime_b) = insert_recipient(&fixture)?;
        let target_a = fixture.target()?;
        let target_b = ProviderMessageTarget::new(
            fixture.workspace,
            profile_b,
            participation_b,
            runtime_b,
            "conversation-b".to_owned(),
        )?;
        let a1 = Uuid::now_v7();
        let a2 = Uuid::now_v7();
        let b1 = Uuid::now_v7();
        let b2 = Uuid::now_v7();
        append_and_enqueue(
            &mut fixture.catalog,
            fixture.workspace,
            a1,
            "a1",
            MessageContext::default(),
            target_a.clone(),
            20,
        )?;
        append_and_enqueue(
            &mut fixture.catalog,
            fixture.workspace,
            a2,
            "a2",
            MessageContext::default(),
            target_a.clone(),
            21,
        )?;
        append_and_enqueue(
            &mut fixture.catalog,
            fixture.workspace,
            b1,
            "b1",
            MessageContext::default(),
            target_b.clone(),
            20,
        )?;
        append_and_enqueue(
            &mut fixture.catalog,
            fixture.workspace,
            b2,
            "b2",
            MessageContext::default(),
            target_b.clone(),
            21,
        )?;

        let first = fixture
            .catalog
            .provider_message_dispatch(a1, time(22)?)?
            .ok_or("missing first runtime A dispatch")?;
        assert_eq!(
            first.dispatch().action(),
            ProviderMessageDispatchAction::Execute
        );
        let unknown_a = fixture.catalog.mark_provider_message_effect_unknown(
            a1,
            first.outbox().revision(),
            &target_a,
            time(23)?,
        )?;

        let a2_blocked = fixture.catalog.provider_message_dispatch(a2, time(24)?)?;
        assert!(
            a2_blocked.is_none(),
            "same-runtime queued work bypassed Unknown"
        );

        let second = fixture
            .catalog
            .next_provider_message_dispatch(time(25)?)?
            .ok_or("different runtime did not progress")?;
        assert_eq!(second.outbox().message_id(), b1);
        assert_eq!(
            second.dispatch().action(),
            ProviderMessageDispatchAction::Execute
        );

        let b2_blocked = fixture.catalog.provider_message_dispatch(b2, time(26)?)?;
        assert!(
            b2_blocked.is_none(),
            "same-runtime queue order was bypassed"
        );

        let inspection = fixture
            .catalog
            .provider_message_dispatch(a1, time(27)?)?
            .ok_or("missing Unknown inspection")?;
        assert_eq!(
            inspection.dispatch().action(),
            ProviderMessageDispatchAction::Inspect
        );
        assert_eq!(inspection.dispatch().revision(), unknown_a.revision());
        assert_eq!(inspection.outbox().last_dispatch_at(), time(27)?);
        fixture.cleanup()?;
        Ok(())
    }

    #[test]
    fn profile_barrier_blocks_new_messages_but_keeps_replay_inspect_and_other_profiles_moving()
    -> Result<(), Box<dyn Error>> {
        let mut fixture = fixture()?;
        let profile_path =
            std::env::temp_dir().join(format!("operational-messages-profile-{}", Uuid::now_v7()));
        let a_message = Uuid::from_u128(0xC801);
        let target_a = fixture.target()?;
        let _queued_a = append_and_enqueue(
            &mut fixture.catalog,
            fixture.workspace,
            a_message,
            "profile A",
            MessageContext::default(),
            target_a.clone(),
            10,
        )?;
        let admitted_a = fixture
            .catalog
            .provider_message_dispatch(a_message, time(11)?)?
            .ok_or("Profile A Execute missing")?;
        let (profiles, intent) = begin_profile_deletion_intent(
            fixture.profile_id,
            &profile_path,
            Uuid::from_u128(0xC802),
            20,
        )?;
        let _ = fixture.catalog.begin_profile_deletion_barrier(&intent)?;

        let replay = fixture
            .catalog
            .enqueue_provider_message(EnqueueProviderMessageCommand {
                message_id: a_message,
                target: target_a.clone(),
                queued_at: time(10)?,
                node_route_witness: None,
            })?;
        assert!(!replay.created());
        assert_eq!(replay.outbox(), admitted_a.outbox());
        let inspect_a = fixture
            .catalog
            .provider_message_dispatch(a_message, time(21)?)?
            .ok_or("admitted Profile A effect was not inspectable")?;
        assert_eq!(
            inspect_a.dispatch().action(),
            ProviderMessageDispatchAction::Inspect
        );

        let blocked_message = Uuid::from_u128(0xC803);
        assert!(
            append_and_enqueue(
                &mut fixture.catalog,
                fixture.workspace,
                blocked_message,
                "must remain unappended",
                MessageContext::default(),
                target_a,
                22,
            )
            .is_err()
        );
        assert!(
            fixture
                .catalog
                .workspace_message(blocked_message)?
                .is_none()
        );

        let (profile_b, participation_b, runtime_b) = insert_recipient(&fixture)?;
        let target_b = ProviderMessageTarget::new(
            fixture.workspace,
            profile_b,
            participation_b,
            runtime_b,
            "conversation-b".to_owned(),
        )?;
        let b_message = Uuid::from_u128(0xC804);
        append_and_enqueue(
            &mut fixture.catalog,
            fixture.workspace,
            b_message,
            "Profile B",
            MessageContext::default(),
            target_b,
            23,
        )?;
        let next = fixture
            .catalog
            .next_provider_message_dispatch(time(24)?)?
            .ok_or("unrelated Profile B provider message did not continue")?;
        assert_eq!(next.outbox().message_id(), b_message);
        assert_eq!(
            next.dispatch().action(),
            ProviderMessageDispatchAction::Execute
        );
        let counts = fixture
            .catalog
            .profile_deletion_in_flight_counts(fixture.profile_id)?;
        assert_eq!(counts.pending_provider_messages(), 1);

        drop(profiles);
        fs::remove_file(profile_path)?;
        fixture.cleanup()?;
        Ok(())
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn provider_dispatch_is_execute_once_then_inspect_and_unknown_is_durable()
    -> Result<(), Box<dyn Error>> {
        let mut fixture = fixture()?;
        let message_id = Uuid::now_v7();
        append(
            &mut fixture.catalog,
            fixture.workspace,
            message_id,
            "hello",
            10,
        )?;
        let target = fixture.target()?;
        let queued = fixture
            .catalog
            .enqueue_provider_message(EnqueueProviderMessageCommand {
                node_route_witness: None,
                message_id,
                target: target.clone(),
                queued_at: time(11)?,
            })?;
        assert!(queued.created());
        assert_eq!(queued.outbox().state(), ProviderMessageState::Queued);
        let execute = fixture
            .catalog
            .next_provider_message_dispatch(time(12)?)?
            .ok_or("missing Execute")?;
        assert_eq!(
            execute.dispatch().action(),
            ProviderMessageDispatchAction::Execute
        );
        assert_eq!(
            execute.outbox().state(),
            ProviderMessageState::EffectPending
        );
        let changes = fixture.catalog.connection.total_changes();
        let observed = fixture.catalog.provider_message_in_flight_page(None, 1)?;
        assert_eq!(observed.outboxes, vec![execute.outbox().clone()]);
        assert!(observed.next_message_id.is_none());
        assert_eq!(fixture.catalog.connection.total_changes(), changes);
        let inspect = fixture
            .catalog
            .next_provider_message_dispatch(time(13)?)?
            .ok_or("missing Inspect")?;
        assert_eq!(
            inspect.dispatch().action(),
            ProviderMessageDispatchAction::Inspect
        );
        assert_eq!(inspect.dispatch().revision(), execute.dispatch().revision());
        let changed_target = ProviderMessageTarget::new(
            fixture.workspace,
            fixture.profile_id,
            fixture.participation_id,
            fixture.runtime_session_id,
            "conversation-2".to_owned(),
        )?;
        assert!(
            fixture
                .catalog
                .mark_provider_message_effect_unknown(
                    message_id,
                    execute.dispatch().revision(),
                    &changed_target,
                    time(14)?,
                )
                .is_err()
        );
        let unknown = fixture.catalog.mark_provider_message_effect_unknown(
            message_id,
            execute.dispatch().revision(),
            &target,
            time(15)?,
        )?;
        assert_eq!(unknown.state(), ProviderMessageState::Unknown);
        let inspect_after_unknown = fixture
            .catalog
            .next_provider_message_dispatch(time(16)?)?
            .ok_or("missing unknown Inspect")?;
        assert_eq!(
            inspect_after_unknown.dispatch().action(),
            ProviderMessageDispatchAction::Inspect
        );
        let receipt =
            ProviderMessageDeliveryReceipt::new(target, "turn-1".to_owned(), "hello".to_owned())?;
        let delivered = fixture.catalog.confirm_provider_message_delivery(
            message_id,
            unknown.revision(),
            &receipt,
            time(17)?,
        )?;
        assert_eq!(delivered.state(), ProviderMessageState::Delivered);
        let changed_receipt = ProviderMessageDeliveryReceipt::new(
            receipt.target().clone(),
            "turn-2".to_owned(),
            "hello".to_owned(),
        )?;
        assert!(
            fixture
                .catalog
                .confirm_provider_message_delivery(
                    message_id,
                    delivered.revision(),
                    &changed_receipt,
                    time(18)?,
                )
                .is_err()
        );
        let captured = incoming(
            &mut fixture.catalog,
            Uuid::now_v7(),
            fixture.workspace,
            receipt.target().clone(),
            ProviderMessageSource::known(
                fixture.profile_id,
                fixture.participation_id,
                "Agent".to_owned(),
            )?,
            "turn-1",
            "hello",
            19,
            MessageContext::default(),
        )?;
        assert!(!captured.created());
        assert_eq!(captured.message().message_id(), message_id);
        fixture.cleanup()?;
        Ok(())
    }

    fn assert_turn_observation_is_read_only(
        fixture: &Fixture,
        target: &ProviderMessageTarget,
        expected: &WorkspaceMessage,
    ) -> Result<(), Box<dyn Error>> {
        let changes = fixture.catalog.connection.total_changes();
        let observed = fixture
            .catalog
            .incoming_provider_turn(target, "turn-1")?
            .ok_or("recorded turn readback missing")?;
        assert_eq!(&observed.message, expected);
        assert_eq!(observed.observed_body, expected.body());
        assert!(
            fixture
                .catalog
                .incoming_provider_turn(target, "absent-turn")?
                .is_none()
        );
        assert_eq!(fixture.catalog.connection.total_changes(), changes);
        Ok(())
    }

    #[test]
    fn incoming_provider_turn_dedupes_and_preserves_goal_generation() -> Result<(), Box<dyn Error>>
    {
        let mut fixture = fixture()?;
        let target = fixture.target()?;
        let source = ProviderMessageSource::unknown("unattributed provider".to_owned())?;
        let message_id = Uuid::now_v7();
        let context = MessageContext::new(
            Some(ActivityId::from_uuid(Uuid::from_u128(0xCE01))),
            Some(GoalId::from_uuid(Uuid::from_u128(0xCE02))),
            Some(time(222)?),
            Some(DagRevisionNumber::INITIAL),
            Some(GoalRevision::INITIAL),
            Some(DagNodeId::from_uuid(Uuid::from_u128(0xCE03))),
        )?;
        let first = incoming(
            &mut fixture.catalog,
            message_id,
            fixture.workspace,
            target.clone(),
            source.clone(),
            "turn-1",
            "incoming",
            10,
            context,
        )?;
        assert!(first.created());
        assert_turn_observation_is_read_only(&fixture, &target, first.message())?;
        let duplicate = incoming(
            &mut fixture.catalog,
            message_id,
            fixture.workspace,
            target.clone(),
            source.clone(),
            "turn-1",
            "incoming",
            10,
            context,
        )?;
        assert!(!duplicate.created());
        assert_eq!(duplicate.message(), first.message());
        let changed_body = incoming(
            &mut fixture.catalog,
            message_id,
            fixture.workspace,
            target.clone(),
            source.clone(),
            "turn-1",
            "changed",
            10,
            context,
        );
        assert!(changed_body.is_err());
        let another_generation = MessageContext::new(
            context.activity_id(),
            context.goal_id(),
            Some(time(223)?),
            context.dag_revision(),
            context.dag_base_goal_revision(),
            context.node_id(),
        )?;
        let changed_generation = incoming(
            &mut fixture.catalog,
            message_id,
            fixture.workspace,
            target.clone(),
            source.clone(),
            "turn-1",
            "incoming",
            10,
            another_generation,
        );
        assert!(changed_generation.is_err());

        let changed_id = incoming(
            &mut fixture.catalog,
            Uuid::now_v7(),
            fixture.workspace,
            target,
            source,
            "turn-1",
            "incoming",
            10,
            context,
        );
        assert!(changed_id.is_err());
        assert_eq!(first.message().context(), context);
        let reopened = OperationsCatalog::open(&fixture.path, fixture.destination)?;
        let reread = reopened
            .workspace_message(message_id)?
            .ok_or("incoming provider message missing after reopen")?;
        assert_eq!(reread.context(), context);
        drop(reopened);
        fixture.cleanup()?;
        Ok(())
    }

    #[test]
    fn wrong_provider_recipient_is_rejected() -> Result<(), Box<dyn Error>> {
        let mut fixture = fixture()?;
        let message_id = Uuid::now_v7();
        append(
            &mut fixture.catalog,
            fixture.workspace,
            message_id,
            "hello",
            10,
        )?;
        let wrong_participation = WorkspaceParticipationId::from_uuid(Uuid::now_v7())?;
        let target = ProviderMessageTarget::new(
            fixture.workspace,
            fixture.profile_id,
            wrong_participation,
            fixture.runtime_session_id,
            "conversation-1".to_owned(),
        )?;
        assert!(
            fixture
                .catalog
                .enqueue_provider_message(EnqueueProviderMessageCommand {
                    node_route_witness: None,
                    message_id,
                    target,
                    queued_at: time(11)?,
                })
                .is_err()
        );
        fixture.cleanup()?;
        Ok(())
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn provider_route_rejects_cross_workspace_reuse() -> Result<(), Box<dyn Error>> {
        let mut fixture = fixture()?;
        let first_message = Uuid::now_v7();
        append(
            &mut fixture.catalog,
            fixture.workspace,
            first_message,
            "first",
            10,
        )?;
        let target = fixture.target()?;
        fixture
            .catalog
            .enqueue_provider_message(EnqueueProviderMessageCommand {
                node_route_witness: None,
                message_id: first_message,
                target: target.clone(),
                queued_at: time(11)?,
            })?;

        let second_workspace = WorkspaceId::from_uuid(Uuid::now_v7());
        let second_participation = WorkspaceParticipationId::from_uuid(Uuid::now_v7())?;
        fixture.catalog.connection.execute(
            "INSERT INTO ops_imports(migration_id,workspace_id,destination_id,source_revision,phase,next_cursor,page_count,record_count,import_digest,verified_pages,verify_digest) VALUES(?1,?2,?3,1,2,NULL,5,0,?4,5,?4)",
            params![
                Uuid::now_v7().as_bytes(),
                second_workspace.into_uuid().as_bytes(),
                fixture.destination.as_bytes(),
                [0_u8; 32].as_slice(),
            ],
        )?;
        fixture.catalog.connection.execute(
            "INSERT INTO ops_workspace_participations(workspace_id,agent_profile_id,participation_id,created_at_ms,revision) VALUES(?1,?2,?3,0,1)",
            params![
                second_workspace.into_uuid().as_bytes(),
                fixture.profile_id.into_uuid().as_bytes(),
                second_participation.into_uuid().as_bytes(),
            ],
        )?;
        let second_message = Uuid::now_v7();
        append(
            &mut fixture.catalog,
            second_workspace,
            second_message,
            "second",
            12,
        )?;
        let runtime_alias_target = ProviderMessageTarget::new(
            second_workspace,
            fixture.profile_id,
            second_participation,
            fixture.runtime_session_id,
            "conversation-2".to_owned(),
        )?;
        let runtime_alias =
            fixture
                .catalog
                .enqueue_provider_message(EnqueueProviderMessageCommand {
                    node_route_witness: None,
                    message_id: second_message,
                    target: runtime_alias_target,
                    queued_at: time(12)?,
                });
        assert!(matches!(
            runtime_alias,
            Err(OperationsError::Conflict("provider route runtime alias"))
        ));
        let second_runtime = AgentProfileRuntimeSessionId::from_uuid(Uuid::now_v7())?;
        let conversation_alias_target = ProviderMessageTarget::new(
            second_workspace,
            fixture.profile_id,
            second_participation,
            second_runtime,
            "conversation-1".to_owned(),
        )?;
        let conversation_alias =
            fixture
                .catalog
                .enqueue_provider_message(EnqueueProviderMessageCommand {
                    node_route_witness: None,
                    message_id: second_message,
                    target: conversation_alias_target,
                    queued_at: time(12)?,
                });
        assert!(matches!(
            conversation_alias,
            Err(OperationsError::Conflict(
                "provider route conversation alias"
            ))
        ));
        let cross_workspace_target = ProviderMessageTarget::new(
            second_workspace,
            fixture.profile_id,
            second_participation,
            fixture.runtime_session_id,
            "conversation-1".to_owned(),
        )?;
        let cross_workspace =
            fixture
                .catalog
                .enqueue_provider_message(EnqueueProviderMessageCommand {
                    node_route_witness: None,
                    message_id: second_message,
                    target: cross_workspace_target,
                    queued_at: time(13)?,
                });
        assert!(cross_workspace.is_err());

        let repeated_message = Uuid::now_v7();
        append(
            &mut fixture.catalog,
            fixture.workspace,
            repeated_message,
            "repeat",
            14,
        )?;
        let repeated = fixture
            .catalog
            .enqueue_provider_message(EnqueueProviderMessageCommand {
                node_route_witness: None,
                message_id: repeated_message,
                target,
                queued_at: time(15)?,
            })?;
        assert!(repeated.created());
        fixture.cleanup()?;
        Ok(())
    }
}
