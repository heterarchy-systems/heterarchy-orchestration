//! Source-fenced, read-only Team query routes.

use std::collections::{HashMap, HashSet};

use axum::extract::{Path, State};
use axum::routing::post;
use axum::{Extension, Json, Router};
use forge_domain::{ActivityId, AgentSessionId, AuthenticatedPrincipal, TimestampMillis};
use heterarchy_orchestration_operations::{
    TeamCapacitySnapshot, TeamCreator, TeamMemberRecord, TeamMutationRecord, TeamRecord, TeamState,
    WorkspaceParticipationId,
    team_expansion::{TeamMutationOwnerReadback, TeamMutationSourceEvidence},
};
use heterarchy_orchestration_runtime::{OrchestrationRuntime, RuntimeError};
use uuid::Uuid;

use crate::app_support::validate_workspace_reference;
use crate::wire::team_commands::{
    TeamMutationV2Intent, TeamMutationV2ReadbackRequest, TeamMutationV2ReadbackResponse,
    TeamRosterMemberRequest,
};
use crate::wire::teams::{
    TeamCapacityReceiptV2Wire, TeamCreatorWire, TeamExpansionJoinSourceV2Wire, TeamGetQueryRequest,
    TeamGetQueryResponse, TeamListQueryRequest, TeamListQueryResponse, TeamMemberWire,
    TeamMutationV2Wire, TeamMutationWire, TeamWire,
};
use crate::{ApiError, BackendState};

const TEAM_LIST_LIMIT: u32 = 64;
const TEAM_MEMBER_LIMIT: u32 = 64;

/// Register the new source-scoped read routes. Mutation routes remain owned by
/// the parent Team adapter.
pub(crate) fn routes() -> Router<BackendState> {
    Router::new()
        .route(
            "/internal/v1/workspaces/{workspace_id}/teams/query",
            post(list_query),
        )
        .route(
            "/internal/v1/workspaces/{workspace_id}/teams/{team_id}/query",
            post(get_query),
        )
        .route(
            "/internal/v2/workspaces/{workspace_id}/teams/{team_id}/receipts/query",
            post(mutation_receipt_v2),
        )
}

async fn mutation_receipt_v2(
    State(state): State<BackendState>,
    Path((workspace_path, team_path)): Path<(String, String)>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(request): Json<TeamMutationV2ReadbackRequest>,
) -> Result<Json<TeamMutationV2ReadbackResponse>, ApiError> {
    validate_v2_readback_intent(&request.intent)?;
    let (scope, team_id, request_id) = match &request.intent {
        TeamMutationV2Intent::Create(intent) => (&intent.scope, intent.team_id, intent.request_id),
        TeamMutationV2Intent::Join(intent) => (&intent.scope, intent.team_id, intent.request_id),
    };
    let scope = scope.clone();
    if crate::canonical_uuid(&workspace_path)? != scope.workspace_id
        || crate::canonical_uuid(&team_path)? != team_id
        || team_id.is_nil()
        || request_id.is_nil()
    {
        return Err(ApiError::InvalidIdentity);
    }
    let reader_activity_id = request.reader.activity_id;
    let reader_session_id = request.reader.session_id;
    if reader_activity_id.is_nil() || reader_session_id.is_nil() {
        return Err(ApiError::InvalidIdentity);
    }
    let session_id = Some(reader_session_id);
    validate_workspace_reference(&scope, session_id)?;
    state
        .activity_scope_proof(scope.clone(), reader_activity_id)
        .await?;

    let workspace_id = scope.workspace_id;
    let intent = request.intent;
    let readback = state
        .run_workspace_reference_query(scope.clone(), session_id, principal, move |runtime, _| {
            runtime.team_mutation_owner_readback(
                workspace_id,
                team_id,
                request_id,
                reader_activity_id,
                reader_session_id,
                principal,
            )
        })
        .await?;
    let receipt = readback
        .as_ref()
        .and_then(|readback| project_owner_readback_v2(&intent, readback));
    Ok(Json(TeamMutationV2ReadbackResponse {
        scope,
        request_id,
        team_id,
        receipt,
    }))
}

fn validate_v2_readback_intent(intent: &TeamMutationV2Intent) -> Result<(), ApiError> {
    match intent {
        TeamMutationV2Intent::Create(request) => {
            if request.scope.workspace_id.is_nil()
                || request.request_id.is_nil()
                || request.team_id.is_nil()
                || request.activity_id.is_nil()
                || request.session_id.is_nil()
                || request.expected_sender_leadership_revision < 1
                || request.max_members > 8
                || usize::from(request.max_members) < request.initial_members.len()
                || !valid_team_text(&request.name, 128)
                || !valid_team_text(&request.purpose, 2_048)
            {
                return Err(ApiError::InvalidIdentity);
            }
            team_create_roster_digest_v1(&request.initial_members).map(|_| ())
        }
        TeamMutationV2Intent::Join(request) => {
            if request.scope.workspace_id.is_nil()
                || request.request_id.is_nil()
                || request.team_id.is_nil()
                || request.activity_id.is_nil()
                || request.session_id.is_nil()
                || request.expected_team_revision == 0
                || request.expected_team_revision > i64::MAX as u64
                || request.expansion_slot.decision_id.is_nil()
                || request.expansion_slot.ordinal >= 8
                || !valid_team_text(&request.role, 256)
            {
                return Err(ApiError::InvalidIdentity);
            }
            Ok(())
        }
    }
}

fn valid_team_text(value: &str, max_bytes: usize) -> bool {
    !value.trim().is_empty() && value.len() <= max_bytes
}

fn team_create_roster_digest_v1(
    members: &[TeamRosterMemberRequest],
) -> Result<heterarchy_orchestration_operations::team_expansion::TeamCreateRosterDigest, ApiError> {
    let inputs = members
        .iter()
        .map(|member| {
            heterarchy_orchestration_operations::team_expansion::TeamRosterDigestInput::new(
                ActivityId::from_uuid(member.activity_id),
                AgentSessionId::from_uuid(member.session_id),
                member.role.as_str(),
            )
        })
        .collect::<Vec<_>>();
    heterarchy_orchestration_operations::team_expansion::team_create_roster_digest(&inputs)
        .map_err(|_| ApiError::InvalidIdentity)
}

async fn list_query(
    State(state): State<BackendState>,
    Path(workspace_path): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(request): Json<TeamListQueryRequest>,
) -> Result<Json<TeamListQueryResponse>, ApiError> {
    validate_workspace_reference(&request.scope, request.session_id)?;
    if crate::canonical_uuid(&workspace_path)? != request.scope.workspace_id {
        return Err(ApiError::InvalidIdentity);
    }
    if !(1..=TEAM_LIST_LIMIT).contains(&request.limit) {
        return Err(ApiError::Runtime(RuntimeError::InvalidInput));
    }

    let scope = request.scope.clone();
    let session_id = request.session_id;
    let workspace_id = scope.workspace_id;
    let limit = request.limit;
    let page = state
        .run_workspace_reference_query(scope.clone(), session_id, principal, move |runtime, _| {
            runtime.team_records(workspace_id, limit)
        })
        .await?;

    Ok(Json(TeamListQueryResponse {
        scope,
        session_id,
        limit,
        teams: page.items().iter().map(team_wire).collect(),
        truncated: page.truncated(),
    }))
}

async fn get_query(
    State(state): State<BackendState>,
    Path((workspace_path, team_path)): Path<(String, String)>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(request): Json<TeamGetQueryRequest>,
) -> Result<Json<TeamGetQueryResponse>, ApiError> {
    validate_workspace_reference(&request.scope, request.session_id)?;
    if crate::canonical_uuid(&workspace_path)? != request.scope.workspace_id
        || crate::canonical_uuid(&team_path)? != request.team_id
    {
        return Err(ApiError::InvalidIdentity);
    }

    let scope = request.scope.clone();
    let session_id = request.session_id;
    let workspace_id = scope.workspace_id;
    let team_id = request.team_id;
    let request_id = request.request_id;
    let (snapshot, profile_names) = state
        .run_workspace_reference_query(scope.clone(), session_id, principal, move |runtime, _| {
            let first = runtime.team_query_snapshot(
                workspace_id,
                team_id,
                TEAM_MEMBER_LIMIT,
                request_id,
            )?;
            let profile_names = resolve_member_profile_names(runtime, first.members.items())?;
            let confirmed = runtime.team_query_snapshot(
                workspace_id,
                team_id,
                TEAM_MEMBER_LIMIT,
                request_id,
            )?;
            if first != confirmed {
                return Err(RuntimeError::ResyncRequired);
            }
            Ok((first, profile_names))
        })
        .await?;

    let members = snapshot
        .members
        .items()
        .iter()
        .map(|member| {
            team_member_wire(
                member,
                member
                    .profile_id()
                    .and_then(|profile_id| profile_names.get(&profile_id))
                    .map(String::as_str),
            )
        })
        .collect();
    Ok(Json(TeamGetQueryResponse {
        scope,
        session_id,
        team_id,
        request_id,
        team: snapshot.team.as_ref().map(team_wire),
        members,
        members_truncated: snapshot.members.truncated(),
        mutation: snapshot.mutation.as_ref().and_then(|record| {
            let has_v2_source_evidence =
                record.team_capacity().is_some() || record.expansion_slot().is_some();
            (!has_v2_source_evidence).then(|| team_mutation_wire(record))
        }),
    }))
}

fn resolve_member_profile_names(
    runtime: &OrchestrationRuntime,
    members: &[TeamMemberRecord],
) -> Result<HashMap<Uuid, String>, RuntimeError> {
    let mut names = HashMap::new();
    let mut resolved = HashSet::new();
    for profile_id in members.iter().filter_map(TeamMemberRecord::profile_id) {
        if !resolved.insert(profile_id) {
            continue;
        }
        if let Some(profile) = runtime.agent_profile(profile_id)? {
            names.insert(profile_id, profile.display_name);
        }
    }
    Ok(names)
}

fn team_wire(team: &TeamRecord) -> TeamWire {
    TeamWire {
        team_id: team.team_id(),
        workspace_id: team.workspace_id().into_uuid(),
        name: team.name().to_owned(),
        purpose: team.purpose().to_owned(),
        state: match team.state() {
            TeamState::Active => "active",
            TeamState::Retired => "retired",
        }
        .to_owned(),
        revision: team.revision(),
        created_by: creator_wire(team.created_by()),
        created_at_ms: team.created_at().get(),
        updated_at_ms: team.updated_at().get(),
        formed_at_ms: team.formed_at().map(TimestampMillis::get),
    }
}

fn creator_wire(creator: &TeamCreator) -> TeamCreatorWire {
    TeamCreatorWire {
        key: creator.key().to_owned(),
        display_name: creator.display_name().to_owned(),
        participation_id: creator
            .participation_id()
            .map(WorkspaceParticipationId::into_uuid),
        profile_id: creator.profile_id(),
        participant_id: creator.participant_id(),
        session_id: creator.session_id().map(AgentSessionId::into_uuid),
    }
}

fn team_member_wire(member: &TeamMemberRecord, display_name: Option<&str>) -> TeamMemberWire {
    TeamMemberWire {
        workspace_id: member.workspace_id().into_uuid(),
        team_id: member.team_id(),
        participation_id: member.participation_id().into_uuid(),
        profile_id: member.profile_id(),
        display_name: display_name.map(str::to_owned),
        role: member.role().map(str::to_owned),
        state: heterarchy_orchestration_runtime::teams::membership_state_label(member.state())
            .to_owned(),
        revision: member.revision(),
        joined_at_ms: member.joined_at().get(),
        left_at_ms: member.left_at().map(TimestampMillis::get),
    }
}

pub(super) fn team_mutation_wire(record: &TeamMutationRecord) -> TeamMutationWire {
    TeamMutationWire {
        team: team_wire(record.team()),
        membership: record
            .membership()
            .map(|member| team_member_wire(member, None)),
        initial_members: record
            .initial_members()
            .iter()
            .map(|member| team_member_wire(member, None))
            .collect(),
        created: record.created(),
        replayed: record.replayed(),
    }
}

pub(super) fn team_mutation_v2_wire(
    record: &TeamMutationRecord,
    expansion_join_source: Option<TeamExpansionJoinSourceV2Wire>,
) -> TeamMutationV2Wire {
    let capacity = record.team_capacity().map(team_capacity_receipt_v2_wire);
    let initial_roster_digest_v1 = record
        .initial_roster_digest()
        .map(heterarchy_orchestration_operations::team_expansion::TeamCreateRosterDigest::to_hex);
    let expansion_slot = record.expansion_slot().map(|slot| {
        crate::wire::team_commands::TeamExpansionSlotV2Request {
            decision_id: slot.approval_id(),
            ordinal: slot.ordinal(),
        }
    });
    TeamMutationV2Wire {
        team: team_wire(record.team()),
        membership: record
            .membership()
            .map(|member| team_member_wire(member, None)),
        initial_members: record
            .initial_members()
            .iter()
            .map(|member| team_member_wire(member, None))
            .collect(),
        capacity,
        initial_roster_digest_v1,
        expansion_slot,
        expansion_join_source,
        created: record.created(),
        replayed: record.replayed(),
    }
}

fn team_capacity_receipt_v2_wire(snapshot: &TeamCapacitySnapshot) -> TeamCapacityReceiptV2Wire {
    TeamCapacityReceiptV2Wire {
        team_id: snapshot.team_id(),
        workspace_id: snapshot.workspace_id().into_uuid(),
        max_members: snapshot.max_members().get(),
        revision: snapshot.revision(),
        recorded_at_ms: snapshot.recorded_at().get(),
        actor: crate::wire::team_expansion::TeamExpansionActorReceiptWire {
            activity_id: snapshot.actor().activity_id().into_uuid(),
            agent_id: snapshot.actor().agent_id().into_uuid(),
            session_id: snapshot.actor().session_id().into_uuid(),
            participation_id: snapshot.actor().participation_id().into_uuid(),
            profile_id: snapshot.actor().profile_id(),
            participant_id: snapshot.actor().participant_id(),
            display_name: snapshot.actor().display_name().to_owned(),
        },
        leadership_revision: snapshot.leadership_revision(),
    }
}

fn project_owner_readback_v2(
    intent: &TeamMutationV2Intent,
    readback: &TeamMutationOwnerReadback,
) -> Option<TeamMutationV2Wire> {
    let record = readback.mutation();
    match (intent, readback.source()?) {
        (
            TeamMutationV2Intent::Create(request),
            TeamMutationSourceEvidence::CappedCreate {
                capacity,
                roster_digest,
            },
        ) => {
            let expected_digest = team_create_roster_digest_v1(&request.initial_members).ok()?;
            let expected_digest_hex = expected_digest.to_hex();
            if expected_digest != *roster_digest
                || record.team_capacity() != Some(capacity)
                || record.initial_roster_digest() != Some(*roster_digest)
                || !record.created()
                || record.team().workspace_id().into_uuid() != request.scope.workspace_id
                || record.team().team_id() != request.team_id
                || record.team().name() != request.name
                || record.team().purpose() != request.purpose
                || record.team().revision() != 1
                || record.team().state() != TeamState::Active
                || record.team().created_by().session_id()
                    != Some(AgentSessionId::from_uuid(request.session_id))
                || capacity.workspace_id().into_uuid() != request.scope.workspace_id
                || capacity.team_id() != request.team_id
                || capacity.max_members().get() != request.max_members
                || capacity.leadership_revision() != request.expected_sender_leadership_revision
                || capacity.actor().activity_id().into_uuid() != request.activity_id
                || capacity.actor().agent_id().into_uuid()
                    != heterarchy_orchestration_runtime::leadership::member_agent_id(
                        request.scope.workspace_id,
                        request.activity_id,
                        request.session_id,
                    )
                    .into_uuid()
                || capacity.actor().session_id().into_uuid() != request.session_id
            {
                return None;
            }
            let mutation = team_mutation_v2_wire(record, None);
            mutation
                .initial_roster_digest_v1
                .as_deref()
                .is_some_and(|digest| digest == expected_digest_hex)
                .then_some(mutation)
        }
        (
            TeamMutationV2Intent::Join(request),
            TeamMutationSourceEvidence::SlotBackedJoin(source),
        ) => {
            let source_actor_matches = match source.actor() {
                heterarchy_orchestration_operations::OperationalActor::Participant {
                    activity_id,
                    agent_id,
                    session_id,
                } => {
                    activity_id.into_uuid() == request.activity_id
                        && agent_id.into_uuid()
                            == heterarchy_orchestration_runtime::leadership::member_agent_id(
                                request.scope.workspace_id,
                                request.activity_id,
                                request.session_id,
                            )
                            .into_uuid()
                        && session_id.into_uuid() == request.session_id
                }
                heterarchy_orchestration_operations::OperationalActor::LocalOperator => false,
            };
            let slot = source.slot();
            let next_revision = request.expected_team_revision.checked_add(1)?;
            let membership = record.membership()?;
            if !source_actor_matches
                || slot.approval_id() != request.expansion_slot.decision_id
                || slot.ordinal() != request.expansion_slot.ordinal
                || record.expansion_slot() != Some(slot)
                || source.role() != request.role
                || !crate::wire::teams::v2_join_created_flag_matches_membership_revision(
                    record.created(),
                    membership.revision(),
                )
                || record.team().workspace_id().into_uuid() != request.scope.workspace_id
                || record.team().team_id() != request.team_id
                || record.team().state() != TeamState::Active
                || record.team().revision() != next_revision
                || membership.state()
                    != heterarchy_orchestration_operations::TeamMembershipState::Joined
                || membership.role() != Some(request.role.as_str())
            {
                return None;
            }
            Some(team_mutation_v2_wire(
                record,
                Some(join_source_v2_wire(source)?),
            ))
        }
        _ => None,
    }
}

fn join_source_v2_wire(
    source: &heterarchy_orchestration_operations::team_expansion::TeamExpansionJoinSource,
) -> Option<TeamExpansionJoinSourceV2Wire> {
    match source.actor() {
        heterarchy_orchestration_operations::OperationalActor::Participant {
            activity_id,
            agent_id,
            session_id,
        } => Some(TeamExpansionJoinSourceV2Wire {
            activity_id: activity_id.into_uuid(),
            agent_id: agent_id.into_uuid(),
            session_id: session_id.into_uuid(),
            decision_id: source.slot().approval_id(),
            ordinal: source.slot().ordinal(),
            role: source.role().to_owned(),
        }),
        heterarchy_orchestration_operations::OperationalActor::LocalOperator => None,
    }
}
