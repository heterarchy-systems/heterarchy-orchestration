//! Transport-neutral Team / Membership / Participation application
//! operations.
//!
//! The durable authority (`heterarchy-orchestration-operations`) owns the
//! exact revision CAS, membership atomicity, initial-roster canonicalization
//! and the trusted-operator boundary for retirement. This layer adds the
//! one thing transports must not improvise: **participant identity is always
//! server-derived** from the caller's logical Session through the canonical
//! member binding (`member_agent_id`), exactly like the leadership lane.
//! A request body names `activity_id` + `session_id`; it never names an
//! `agent_id`. Retirement rechecks the authenticated principal's explicit
//! operator grant while Operations records the trusted local operator actor.

use forge_domain::{
    ActivityId, AgentSessionId, AuthenticatedPrincipal, TimestampMillis, WorkspaceId,
};
use heterarchy_orchestration_operations::{
    OperationalActor, OperationsError, TeamCreateCommand, TeamJoinCommand, TeamLeaveCommand,
    TeamMutationRecord, TeamQuerySnapshot, TeamReadSet, TeamRecord, TeamRetireCommand,
    TeamRosterMemberInput,
};
use uuid::Uuid;

use super::{leadership::member_agent_id, participation::ParticipationTarget};
use crate::{OrchestrationRuntime, RuntimeError, TeamSummary};

/// Server-derived participant actor for one caller session. The `agent_id`
/// is never accepted from a request body.
fn participant_actor(
    workspace: WorkspaceId,
    activity_id: Uuid,
    session_id: Uuid,
) -> Result<(OperationalActor, ParticipationTarget), RuntimeError> {
    if activity_id.is_nil() || session_id.is_nil() {
        return Err(RuntimeError::InvalidInput);
    }
    let _workspace_scope = require_workspace(workspace)?;
    let agent_id = member_agent_id(workspace.into_uuid(), activity_id, session_id);
    Ok((
        OperationalActor::Participant {
            activity_id: ActivityId::from_uuid(activity_id),
            agent_id,
            session_id: AgentSessionId::from_uuid(session_id),
        },
        ParticipationTarget {
            workspace_id: workspace.into_uuid(),
            activity_id,
            agent_id: agent_id.into_uuid(),
            session_id,
        },
    ))
}

fn require_workspace(value: WorkspaceId) -> Result<WorkspaceId, RuntimeError> {
    if value.into_uuid().is_nil() {
        return Err(RuntimeError::InvalidInput);
    }
    Ok(value)
}

fn require_identity(value: Uuid) -> Result<Uuid, RuntimeError> {
    if value.is_nil() {
        return Err(RuntimeError::InvalidInput);
    }
    Ok(value)
}

fn timestamp(value: i64) -> Result<TimestampMillis, RuntimeError> {
    TimestampMillis::new(value).map_err(|_| RuntimeError::InvalidInput)
}

/// Bounded projection of one durable membership row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TeamMembershipSummary {
    /// Workspace participation identity.
    pub participation_id: Uuid,
    /// Linked Profile, when the participant has one.
    pub profile_id: Option<Uuid>,
    /// Declared role inside the team.
    pub role: Option<String>,
    /// Membership state label (`joined` / `left`).
    pub state: &'static str,
    /// Exact membership revision.
    pub revision: u64,
    /// Join timestamp.
    pub joined_at_ms: i64,
    /// Leave timestamp, when the member left.
    pub left_at_ms: Option<i64>,
}

/// One bounded membership page.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TeamMembershipPage {
    /// Membership rows.
    pub items: Vec<TeamMembershipSummary>,
    /// True when more rows exist beyond the requested limit.
    pub truncated: bool,
}

/// Creation intent for one Team: the creator session and every initial
/// roster member are named by `activity_id` + `session_id` only; member
/// Agent identities are server-derived through the canonical member
/// binding.
#[derive(Debug, Clone)]
pub struct TeamCreateSpec {
    /// Exact replay identity of the creation request.
    pub request_id: Uuid,
    /// Exact Workspace scope.
    pub workspace_id: Uuid,
    /// New Team identity.
    pub team_id: Uuid,
    /// Creator's Activity.
    pub activity_id: Uuid,
    /// Creator's logical Session.
    pub session_id: Uuid,
    /// Sender leadership revision fence, when the creator is a leader.
    pub expected_sender_leadership_revision: Option<i64>,
    /// Team name (domain-bounded).
    pub name: String,
    /// Team purpose (domain-bounded).
    pub purpose: String,
    /// Initial roster, assigned atomically at formation.
    pub initial_members: Vec<TeamRosterMemberSpec>,
    /// Caller-supplied wall-clock milliseconds.
    pub observed_at_ms: i64,
}

/// One initial roster member named by session, never by agent id.
#[derive(Debug, Clone)]
pub struct TeamRosterMemberSpec {
    /// Member's Activity.
    pub activity_id: Uuid,
    /// Member's logical Session.
    pub session_id: Uuid,
    /// Declared role inside the team.
    pub role: String,
}

/// Join intent for one caller session.
#[derive(Debug, Clone)]
pub struct TeamJoinSpec {
    /// Exact replay identity of the join request.
    pub request_id: Uuid,
    /// Exact Workspace scope.
    pub workspace_id: Uuid,
    /// Target Team identity.
    pub team_id: Uuid,
    /// Exact Team revision the caller observed.
    pub expected_team_revision: u64,
    /// Caller's Activity.
    pub activity_id: Uuid,
    /// Caller's logical Session.
    pub session_id: Uuid,
    /// Declared role inside the team.
    pub role: String,
    /// Caller-supplied wall-clock milliseconds.
    pub observed_at_ms: i64,
}

/// Leave intent for one caller session.
#[derive(Debug, Clone, Copy)]
pub struct TeamLeaveSpec {
    /// Exact replay identity of the leave request.
    pub request_id: Uuid,
    /// Exact Workspace scope.
    pub workspace_id: Uuid,
    /// Target Team identity.
    pub team_id: Uuid,
    /// Exact Team revision the caller observed.
    pub expected_team_revision: u64,
    /// Caller's Activity.
    pub activity_id: Uuid,
    /// Caller's logical Session.
    pub session_id: Uuid,
    /// Caller-supplied wall-clock milliseconds.
    pub observed_at_ms: i64,
}

impl OrchestrationRuntime {
    /// Read complete Team records for a bounded Workspace page.
    ///
    /// This preserves the full Operations record for transport projection,
    /// including the creator identity omitted by the older summary API.
    ///
    /// # Errors
    /// Returns invalid Workspace/limit input or durable catalog failures.
    pub fn team_records(
        &self,
        workspace_id: Uuid,
        limit: u32,
    ) -> Result<TeamReadSet<TeamRecord>, RuntimeError> {
        let workspace = WorkspaceId::from_uuid(require_identity(workspace_id)?);
        self.catalog()?.teams(workspace, limit).map_err(Into::into)
    }

    /// Read the current Team, its bounded member page, and an optional exact
    /// mutation receipt from one Operations transaction.
    ///
    /// Profile display names are deliberately not part of this snapshot; a
    /// transport may resolve them through the Profile owner after this method
    /// releases the Operations catalog and then revalidate this snapshot.
    ///
    /// # Errors
    /// Returns invalid identities/limits, Workspace scope conflicts, or
    /// durable catalog failures.
    pub fn team_query_snapshot(
        &self,
        workspace_id: Uuid,
        team_id: Uuid,
        member_limit: u32,
        request_id: Option<Uuid>,
    ) -> Result<TeamQuerySnapshot, RuntimeError> {
        let workspace = WorkspaceId::from_uuid(require_identity(workspace_id)?);
        let team_id = require_identity(team_id)?;
        let request_id = request_id.map(require_identity).transpose()?;
        self.catalog()?
            .team_query_snapshot(workspace, team_id, member_limit, request_id)
            .map_err(Into::into)
    }

    /// Create one Team with its initial roster assigned atomically.
    ///
    /// The creator actor and every roster member's Agent identity are
    /// server-derived through the canonical member binding; a request body
    /// never names an `agent_id`. The principal must own the creator's open
    /// logical Session and have Workspace read access. The same `request_id`
    /// replays the same receipt.
    ///
    /// # Errors
    /// Returns invalid input, authorization, stale leadership fence conflict,
    /// or durable catalog failures.
    pub fn create_team(
        &self,
        spec: TeamCreateSpec,
        principal: AuthenticatedPrincipal,
    ) -> Result<TeamMutationRecord, RuntimeError> {
        let workspace =
            require_workspace(WorkspaceId::from_uuid(require_identity(spec.workspace_id)?))?;
        let (creator, participation) =
            participant_actor(workspace, spec.activity_id, spec.session_id)?;
        self.require_participation(participation, principal)?;
        let request_id = require_identity(spec.request_id)?;
        let team_id = require_identity(spec.team_id)?;
        if !(2..=64).contains(&spec.initial_members.len()) {
            return Err(RuntimeError::persistence(OperationsError::Bounds(
                "team initial roster",
            )));
        }
        if spec
            .initial_members
            .iter()
            .any(|member| member.activity_id.is_nil() || member.session_id.is_nil())
        {
            return Err(RuntimeError::InvalidInput);
        }
        let mut initial_members = Vec::with_capacity(spec.initial_members.len());
        for member in spec.initial_members {
            initial_members.push(TeamRosterMemberInput {
                activity_id: ActivityId::from_uuid(member.activity_id),
                agent_id: member_agent_id(
                    workspace.into_uuid(),
                    member.activity_id,
                    member.session_id,
                ),
                session_id: AgentSessionId::from_uuid(member.session_id),
                role: member.role,
            });
        }
        let command = TeamCreateCommand {
            request_id,
            team_id,
            workspace_id: workspace,
            actor: creator,
            expected_sender_leadership_revision: spec.expected_sender_leadership_revision,
            name: spec.name,
            purpose: spec.purpose,
            initial_members,
            observed_at: timestamp(spec.observed_at_ms)?,
        };
        let mut catalog = self.catalog()?;
        catalog
            .create_team(command)
            .map_err(RuntimeError::persistence)
    }

    /// Join the caller's logical participant to one exact Team revision.
    ///
    /// The principal must own the caller's open logical Session and have
    /// Workspace read access. The durable authority rejects non-participants
    /// and stale revisions; repeated `request_id`s replay the same receipt.
    ///
    /// # Errors
    /// Returns invalid input, authorization, non-participant/stale-revision
    /// conflict, or durable catalog failures.
    pub fn join_team(
        &self,
        spec: TeamJoinSpec,
        principal: AuthenticatedPrincipal,
    ) -> Result<TeamMutationRecord, RuntimeError> {
        let workspace =
            require_workspace(WorkspaceId::from_uuid(require_identity(spec.workspace_id)?))?;
        let (actor, participation) =
            participant_actor(workspace, spec.activity_id, spec.session_id)?;
        self.require_participation(participation, principal)?;
        let command = TeamJoinCommand {
            request_id: require_identity(spec.request_id)?,
            workspace_id: workspace,
            team_id: require_identity(spec.team_id)?,
            expected_team_revision: spec.expected_team_revision,
            actor,
            role: spec.role,
            observed_at: timestamp(spec.observed_at_ms)?,
        };
        let mut catalog = self.catalog()?;
        catalog
            .join_team(command)
            .map_err(RuntimeError::persistence)
    }

    /// Leave one Team while retaining the membership audit trail.
    /// The principal must own the caller's open logical Session and have
    /// Workspace read access.
    ///
    /// # Errors
    /// Returns invalid input, authorization, stale-revision conflict, or
    /// durable catalog failures.
    pub fn leave_team(
        &self,
        spec: TeamLeaveSpec,
        principal: AuthenticatedPrincipal,
    ) -> Result<TeamMutationRecord, RuntimeError> {
        let workspace =
            require_workspace(WorkspaceId::from_uuid(require_identity(spec.workspace_id)?))?;
        let (actor, participation) =
            participant_actor(workspace, spec.activity_id, spec.session_id)?;
        self.require_participation(participation, principal)?;
        let command = TeamLeaveCommand {
            request_id: require_identity(spec.request_id)?,
            workspace_id: workspace,
            team_id: require_identity(spec.team_id)?,
            expected_team_revision: spec.expected_team_revision,
            actor,
            observed_at: timestamp(spec.observed_at_ms)?,
        };
        let mut catalog = self.catalog()?;
        catalog
            .leave_team(command)
            .map_err(RuntimeError::persistence)
    }

    /// Retire one Team through the trusted local operator boundary.
    ///
    /// The supplied principal must hold the configured operator grant. The
    /// durable actor remains [`OperationalActor::LocalOperator`] and is never
    /// taken from the request.
    ///
    /// # Errors
    /// Returns invalid input, unauthorized actor/stale revision conflict, or
    /// durable catalog failures.
    pub fn retire_team(
        &self,
        request_id: Uuid,
        workspace_id: Uuid,
        team_id: Uuid,
        expected_team_revision: u64,
        observed_at_ms: i64,
        principal: AuthenticatedPrincipal,
    ) -> Result<TeamMutationRecord, RuntimeError> {
        self.require_operator(principal)?;
        let workspace = require_workspace(WorkspaceId::from_uuid(require_identity(workspace_id)?))?;
        let command = TeamRetireCommand {
            request_id: require_identity(request_id)?,
            workspace_id: workspace,
            team_id: require_identity(team_id)?,
            expected_team_revision,
            actor: OperationalActor::LocalOperator,
            observed_at: timestamp(observed_at_ms)?,
        };
        let mut catalog = self.catalog()?;
        catalog
            .retire_team(command)
            .map_err(RuntimeError::persistence)
    }

    /// Read one exact durable Team mutation receipt, including legacy
    /// unformed results. Reading neither repeats the mutation nor assigns
    /// missing historical roles.
    ///
    /// # Errors
    /// Returns invalid input or durable catalog failures.
    pub fn team_mutation(
        &self,
        workspace_id: Uuid,
        team_id: Uuid,
        request_id: Uuid,
    ) -> Result<Option<TeamMutationRecord>, RuntimeError> {
        let catalog = self.catalog()?;
        catalog
            .team_mutation(
                WorkspaceId::from_uuid(require_identity(workspace_id)?),
                require_identity(team_id)?,
                require_identity(request_id)?,
            )
            .map_err(RuntimeError::persistence)
    }

    /// Read a bounded page of one Team's logical memberships.
    ///
    /// # Errors
    /// Returns invalid input, wrong-team scope bounds, or durable catalog
    /// failures.
    pub fn team_members(
        &self,
        workspace_id: Uuid,
        team_id: Uuid,
        limit: u32,
    ) -> Result<TeamMembershipPage, RuntimeError> {
        let catalog = self.catalog()?;
        let page = catalog.team_members(
            WorkspaceId::from_uuid(require_identity(workspace_id)?),
            require_identity(team_id)?,
            limit,
        )?;
        Ok(TeamMembershipPage {
            items: page
                .items()
                .iter()
                .map(|member| TeamMembershipSummary {
                    participation_id: member.participation_id().into_uuid(),
                    profile_id: member.profile_id(),
                    role: member.role().map(str::to_owned),
                    state: membership_state_label(member.state()),
                    revision: member.revision(),
                    joined_at_ms: member.joined_at().get(),
                    left_at_ms: member.left_at().map(TimestampMillis::get),
                })
                .collect(),
            truncated: page.truncated(),
        })
    }

    /// Read one exact durable Team.
    ///
    /// # Errors
    /// Returns invalid input or durable catalog failures.
    pub fn team(
        &self,
        workspace_id: Uuid,
        team_id: Uuid,
    ) -> Result<Option<TeamSummary>, RuntimeError> {
        let catalog = self.catalog()?;
        let record = catalog.team(
            WorkspaceId::from_uuid(require_identity(workspace_id)?),
            team_id,
        )?;
        Ok(record.as_ref().map(crate::team_summary))
    }
}

/// Deterministic membership-state labels shared by every transport
/// projection.
#[must_use]
pub const fn membership_state_label(
    state: heterarchy_orchestration_operations::TeamMembershipState,
) -> &'static str {
    match state {
        heterarchy_orchestration_operations::TeamMembershipState::Joined => "joined",
        heterarchy_orchestration_operations::TeamMembershipState::Left => "left",
    }
}
