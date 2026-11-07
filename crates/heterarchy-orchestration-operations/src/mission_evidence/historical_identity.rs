use forge_domain::{
    ActivityId, AgentInstanceId, AgentSessionId, AgentTransportKind, TimestampMillis, WorkspaceId,
};
use rusqlite::{Connection, OptionalExtension, params};
use uuid::Uuid;

use super::model::{
    MissionEvidenceIdentityActor, MissionEvidenceIdentityProofParts,
    MissionEvidenceParticipationProvenance, MissionEvidenceProfileProvenance,
    MissionEvidenceRegistrationProvenance, MissionEvidenceReplacementProvenance,
};
use super::{
    MissionEvidenceIdentityPendingReason, MissionEvidenceIdentityProof,
    MissionEvidenceIdentityResolution, MissionEvidenceRegistrationKind,
};
use crate::{OperationsError, RegistrationRequest, WorkspaceParticipationId};

const MAX_IDENTITY_CANDIDATES: usize = 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CandidateKind {
    Original,
    Replacement,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Candidate {
    kind: CandidateKind,
    request_id: Uuid,
}

#[derive(Debug)]
struct OriginalRow {
    request_id: Vec<u8>,
    migration_id: Vec<u8>,
    workspace_id: Vec<u8>,
    destination_id: Vec<u8>,
    activity_id: Vec<u8>,
    session_id: Vec<u8>,
    participant_id: Vec<u8>,
    agent_id: Vec<u8>,
    request_wire: Vec<u8>,
    phase: i64,
    reserved_at_ms: Option<i64>,
}

#[derive(Debug, Clone, Copy)]
struct ParticipationMapping {
    request_id: Uuid,
    participation_id: WorkspaceParticipationId,
    profile_id: Uuid,
    session_id: AgentSessionId,
    participant_id: Uuid,
    participant_membership_revision: u64,
    mapped_at_ms: i64,
}

struct OriginalRegistrationContext {
    request: RegistrationRequest,
    migration_id: Uuid,
    activity_id: Uuid,
    participant_id: Uuid,
    reserved_at_ms: i64,
}

struct OriginalActivationProvenance {
    freeze_revision: u64,
    membership_commit_digest: [u8; 32],
    activated_at_ms: i64,
}

enum OriginalActivationResolution {
    RegistrationPending,
    ActivationPending,
    Ready(OriginalActivationProvenance),
}

struct OriginalLocalLineage {
    profile: Option<MissionEvidenceProfileProvenance>,
    participation: Option<MissionEvidenceParticipationProvenance>,
}

enum OriginalLocalLineageResolution {
    Pending,
    Ready(OriginalLocalLineage),
}

#[derive(Clone, Copy)]
struct OriginalParticipationQuery {
    workspace_id: WorkspaceId,
    profile_id: Uuid,
    profile_link_request_id: Uuid,
    profile_revision: u64,
    session_revision: u64,
    participant_id: Uuid,
    session_id: AgentSessionId,
}

struct ReplacementContext {
    migration_id: Uuid,
    workspace_id: WorkspaceId,
    destination_id: Uuid,
    participation_id: WorkspaceParticipationId,
    profile_id: Uuid,
    profile_link_request_id: Uuid,
    previous_session_id: AgentSessionId,
    session_id: AgentSessionId,
    profile_revision: u64,
    session_revision: u64,
    participant_id: Uuid,
    participant_membership_revision: u64,
    expected_participation_revision: u64,
    freeze_revision: u64,
    commit_digest: [u8; 32],
    joined_event_id: Uuid,
    activated_at_ms: i64,
}

struct ReplacementLineage {
    participation: MissionEvidenceParticipationProvenance,
    replacement: MissionEvidenceReplacementProvenance,
}

struct RawReplacementCommitFields {
    freeze_revision: Option<i64>,
    reserved_at_ms: Option<i64>,
    commit_digest: Option<Vec<u8>>,
    committed_at_ms: Option<i64>,
    joined_event_id: Option<Vec<u8>>,
    activated_at_ms: Option<i64>,
    profile_revision: i64,
    session_revision: i64,
    expected_participation_revision: i64,
    participant_membership_revision: i64,
}

struct ReplacementCommitFields {
    freeze_revision: u64,
    commit_digest: [u8; 32],
    joined_event_id: Uuid,
    activated_at_ms: i64,
    profile_revision: u64,
    session_revision: u64,
    expected_participation_revision: u64,
    participant_membership_revision: u64,
}

#[derive(Debug)]
struct RawParticipationMapping {
    request_id: Vec<u8>,
    workspace_id: Vec<u8>,
    participation_id: Vec<u8>,
    profile_id: Vec<u8>,
    profile_mapping_request_id: Vec<u8>,
    profile_revision: i64,
    session_id: Vec<u8>,
    session_revision: i64,
    participant_id: Vec<u8>,
    participant_membership_revision: i64,
    mapped_at_ms: i64,
}

#[derive(Debug)]
struct ReplacementRow {
    request_id: Vec<u8>,
    migration_id: Vec<u8>,
    workspace_id: Vec<u8>,
    destination_id: Vec<u8>,
    participation_id: Vec<u8>,
    profile_id: Vec<u8>,
    profile_revision: i64,
    profile_mapping_request_id: Vec<u8>,
    previous_session_id: Vec<u8>,
    new_session_id: Vec<u8>,
    new_session_revision: i64,
    participant_id: Vec<u8>,
    participant_membership_revision: i64,
    expected_participation_revision: i64,
    activity_id: Vec<u8>,
    agent_id: Vec<u8>,
    request_wire: Vec<u8>,
    phase: i64,
    freeze_revision: Option<i64>,
    reserved_at_ms: Option<i64>,
    commit_digest: Option<Vec<u8>>,
    committed_at_ms: Option<i64>,
    joined_event_id: Option<Vec<u8>>,
    activated_at_ms: Option<i64>,
}

/// Resolve one source Agent/Session tuple only through immutable O registration
/// and activation history. It never consults current effect bindings or names.
pub(super) fn resolve(
    connection: &Connection,
    destination_id: Uuid,
    workspace_id: WorkspaceId,
    expected_activity_id: Option<ActivityId>,
    agent_id: AgentInstanceId,
    session_id: AgentSessionId,
) -> Result<MissionEvidenceIdentityResolution, OperationsError> {
    if destination_id.is_nil()
        || workspace_id.into_uuid().is_nil()
        || agent_id.into_uuid().is_nil()
        || session_id.into_uuid().is_nil()
    {
        return Err(OperationsError::Conflict(
            "mission evidence historical actor identity",
        ));
    }

    let mut candidates = original_candidates(connection, workspace_id, agent_id, session_id)?;
    candidates.extend(replacement_candidates(
        connection,
        workspace_id,
        agent_id,
        session_id,
    )?);
    if candidates.is_empty() {
        return Ok(pending(
            agent_id,
            session_id,
            MissionEvidenceIdentityPendingReason::NoRegistrationHistory,
            None,
            None,
        ));
    }
    if candidates.len() != 1 {
        return Ok(pending(
            agent_id,
            session_id,
            MissionEvidenceIdentityPendingReason::AmbiguousHistory,
            None,
            None,
        ));
    }
    let candidate = candidates[0];
    match candidate.kind {
        CandidateKind::Original => resolve_original(
            connection,
            destination_id,
            workspace_id,
            expected_activity_id,
            agent_id,
            session_id,
            candidate.request_id,
        ),
        CandidateKind::Replacement => resolve_replacement(
            connection,
            destination_id,
            workspace_id,
            expected_activity_id,
            agent_id,
            session_id,
            candidate.request_id,
        ),
    }
}

fn original_candidates(
    connection: &Connection,
    workspace_id: WorkspaceId,
    agent_id: AgentInstanceId,
    session_id: AgentSessionId,
) -> Result<Vec<Candidate>, OperationsError> {
    let mut statement = connection.prepare(
        "SELECT substr(request_id,1,17) FROM ops_registration_requests \
         WHERE workspace_id=?1 AND agent_id=?2 AND session_id=?3 \
         ORDER BY request_id LIMIT ?4",
    )?;
    let rows = statement.query_map(
        params![
            workspace_id.into_uuid().as_bytes(),
            agent_id.into_uuid().as_bytes(),
            session_id.into_uuid().as_bytes(),
            i64::try_from(MAX_IDENTITY_CANDIDATES)
                .map_err(|_| OperationsError::Bounds("mission evidence identity candidates"))?,
        ],
        |row| row.get::<_, Vec<u8>>(0),
    )?;
    rows.map(|row| {
        Ok(Candidate {
            kind: CandidateKind::Original,
            request_id: decode_uuid(row?, "historical registration request")?,
        })
    })
    .collect()
}

fn replacement_candidates(
    connection: &Connection,
    workspace_id: WorkspaceId,
    agent_id: AgentInstanceId,
    session_id: AgentSessionId,
) -> Result<Vec<Candidate>, OperationsError> {
    let mut statement = connection.prepare(
        "SELECT substr(request_id,1,17) FROM ops_workspace_participation_replacement_registrations \
         WHERE workspace_id=?1 AND agent_id=?2 AND new_session_id=?3 \
         ORDER BY request_id LIMIT ?4",
    )?;
    let rows = statement.query_map(
        params![
            workspace_id.into_uuid().as_bytes(),
            agent_id.into_uuid().as_bytes(),
            session_id.into_uuid().as_bytes(),
            i64::try_from(MAX_IDENTITY_CANDIDATES)
                .map_err(|_| OperationsError::Bounds("mission evidence identity candidates"))?,
        ],
        |row| row.get::<_, Vec<u8>>(0),
    )?;
    rows.map(|row| {
        Ok(Candidate {
            kind: CandidateKind::Replacement,
            request_id: decode_uuid(row?, "historical replacement request")?,
        })
    })
    .collect()
}

fn resolve_original(
    connection: &Connection,
    destination_id: Uuid,
    workspace_id: WorkspaceId,
    expected_activity_id: Option<ActivityId>,
    agent_id: AgentInstanceId,
    session_id: AgentSessionId,
    request_id: Uuid,
) -> Result<MissionEvidenceIdentityResolution, OperationsError> {
    let Some(context) = load_original_registration_context(
        connection,
        destination_id,
        workspace_id,
        expected_activity_id,
        agent_id,
        session_id,
        request_id,
    )?
    else {
        return Ok(pending(
            agent_id,
            session_id,
            MissionEvidenceIdentityPendingReason::RegistrationNotCommitted,
            Some(request_id),
            None,
        ));
    };
    let activation = match load_original_activation_provenance(
        connection,
        destination_id,
        workspace_id,
        request_id,
        &context,
    )? {
        OriginalActivationResolution::RegistrationPending => {
            return Ok(pending(
                agent_id,
                session_id,
                MissionEvidenceIdentityPendingReason::RegistrationNotCommitted,
                Some(request_id),
                None,
            ));
        }
        OriginalActivationResolution::ActivationPending => {
            return Ok(pending(
                agent_id,
                session_id,
                MissionEvidenceIdentityPendingReason::ActivationMissing,
                Some(request_id),
                None,
            ));
        }
        OriginalActivationResolution::Ready(value) => value,
    };
    let lineage = match resolve_original_local_lineage(
        connection,
        workspace_id,
        session_id,
        request_id,
        context.participant_id,
        &context.request,
    )? {
        OriginalLocalLineageResolution::Pending => {
            return Ok(pending(
                agent_id,
                session_id,
                MissionEvidenceIdentityPendingReason::ProfileLinkMissing,
                Some(request_id),
                Some(request_id),
            ));
        }
        OriginalLocalLineageResolution::Ready(value) => value,
    };
    let proof = MissionEvidenceIdentityProof::from_parts(&MissionEvidenceIdentityProofParts {
        actor: MissionEvidenceIdentityActor {
            agent: agent_id,
            session: session_id,
            participant: context.participant_id,
        },
        registration: MissionEvidenceRegistrationProvenance {
            kind: MissionEvidenceRegistrationKind::Original,
            request_id,
            migration_id: context.migration_id,
            destination_id,
            freeze_revision: activation.freeze_revision,
            membership_commit_digest: activation.membership_commit_digest,
            activation_receipt_id: request_id,
            activated_at_ms: activation.activated_at_ms,
        },
        profile: lineage.profile,
        participation: lineage.participation,
        replacement: None,
    })?;
    Ok(MissionEvidenceIdentityResolution::Verified(Box::new(proof)))
}

fn load_original_registration_context(
    connection: &Connection,
    destination_id: Uuid,
    workspace_id: WorkspaceId,
    expected_activity_id: Option<ActivityId>,
    agent_id: AgentInstanceId,
    session_id: AgentSessionId,
    request_id: Uuid,
) -> Result<Option<OriginalRegistrationContext>, OperationsError> {
    let Some(raw) = load_original(connection, request_id)? else {
        return Err(OperationsError::Corrupt(
            "historical registration disappeared",
        ));
    };
    if decode_uuid(raw.request_id, "historical registration request")? != request_id {
        return Err(OperationsError::Corrupt("historical registration request"));
    }
    let request = RegistrationRequest::decode(&raw.request_wire)?;
    let migration_id = decode_uuid(raw.migration_id, "historical registration migration")?;
    let stored_workspace = decode_workspace(raw.workspace_id)?;
    let stored_destination =
        decode_uuid(raw.destination_id, "historical registration destination")?;
    let activity_id = decode_uuid(raw.activity_id, "historical registration Activity")?;
    let stored_session = decode_session(raw.session_id)?;
    let participant_id = decode_uuid(raw.participant_id, "historical registration participant")?;
    let stored_agent = decode_agent(raw.agent_id)?;
    let binding = request.binding();
    if binding.request_id() != request_id
        || stored_workspace != workspace_id
        || stored_destination != destination_id
        || stored_agent != agent_id
        || stored_session != session_id
        || binding.activity_id().into_uuid() != activity_id
        || binding.agent_id() != agent_id
        || binding.session_id() != session_id
        || request.proposed_participant_id() != participant_id
    {
        return Err(OperationsError::Corrupt("historical registration identity"));
    }
    if expected_activity_id.is_some_and(|expected| expected.into_uuid() != activity_id) {
        return Err(OperationsError::Conflict(
            "mission evidence target Activity mismatch",
        ));
    }
    if raw.phase != 0 && raw.phase != 1 {
        return Err(OperationsError::Corrupt("historical registration phase"));
    }
    if (raw.phase == 0 && raw.reserved_at_ms.is_some())
        || raw.reserved_at_ms.is_some_and(|value| value < 0)
    {
        return Err(OperationsError::Corrupt(
            "historical registration reservation",
        ));
    }
    if raw.phase != 1 || raw.reserved_at_ms.is_none() {
        return Ok(None);
    }
    let reserved_at_ms = raw.reserved_at_ms.ok_or(OperationsError::Corrupt(
        "historical registration reservation",
    ))?;
    Ok(Some(OriginalRegistrationContext {
        request,
        migration_id,
        activity_id,
        participant_id,
        reserved_at_ms,
    }))
}

fn load_original_activation_provenance(
    connection: &Connection,
    destination_id: Uuid,
    workspace_id: WorkspaceId,
    request_id: Uuid,
    context: &OriginalRegistrationContext,
) -> Result<OriginalActivationResolution, OperationsError> {
    let Some(progress) = crate::registration::committed_progress_at(connection, request_id)? else {
        return Ok(OriginalActivationResolution::RegistrationPending);
    };
    if progress.workspace_id() != workspace_id
        || progress.migration_id() != context.migration_id
        || progress.participant_id() != context.participant_id
        || progress.request() != &context.request
        || progress.reserved_at().map(TimestampMillis::get) != Some(context.reserved_at_ms)
    {
        return Err(OperationsError::Corrupt(
            "historical committed registration identity",
        ));
    }
    let Some(membership) = crate::membership::commit_at(connection, destination_id, request_id)?
    else {
        return Ok(OriginalActivationResolution::RegistrationPending);
    };
    if membership.request_id() != request_id
        || membership.migration_id() != context.migration_id
        || membership.workspace_id() != workspace_id
        || membership.destination_id() != destination_id
        || membership.participant_id() != context.participant_id
        || membership.binding() != context.request.binding()
        || membership.freeze_revision() == 0
        || membership.reserved_at().get() != context.reserved_at_ms
    {
        return Err(OperationsError::Corrupt("historical membership identity"));
    }
    validate_import(
        connection,
        context.migration_id,
        workspace_id,
        destination_id,
        i64::try_from(membership.freeze_revision())
            .map_err(|_| OperationsError::Corrupt("historical membership freeze revision"))?,
    )?;
    validate_membership_event(
        connection,
        workspace_id,
        context.participant_id,
        Some(context.activity_id),
        membership.joined_event_id(),
        Some(membership.committed_at().get()),
    )?;
    let Some(activation) =
        crate::activation::activation_at(connection, destination_id, request_id)?
    else {
        return Ok(OriginalActivationResolution::ActivationPending);
    };
    if activation.request_id() != request_id
        || activation.workspace_id() != workspace_id
        || activation.destination_id() != destination_id
        || activation.participant_id() != context.participant_id
        || activation.binding() != context.request.binding()
        || activation.membership_digest() != membership.commit_digest()
        || activation.display_name() != context.request.display_name()
        || activation.declared_role() != context.request.declared_role()
        || activation.transport() != context.request.transport()
    {
        return Err(OperationsError::Corrupt("historical activation identity"));
    }
    Ok(OriginalActivationResolution::Ready(
        OriginalActivationProvenance {
            freeze_revision: membership.freeze_revision(),
            membership_commit_digest: activation.membership_digest(),
            activated_at_ms: activation.activated_at().get(),
        },
    ))
}

fn resolve_original_local_lineage(
    connection: &Connection,
    workspace_id: WorkspaceId,
    session_id: AgentSessionId,
    request_id: Uuid,
    participant_id: Uuid,
    request: &RegistrationRequest,
) -> Result<OriginalLocalLineageResolution, OperationsError> {
    let Some(scope) = crate::registration_profile::load(connection, request_id)? else {
        if request.transport() != AgentTransportKind::StreamableHttp
            || crate::native_session_participation_id(
                workspace_id.into_uuid(),
                session_id.into_uuid(),
            ) != participant_id
        {
            return Ok(OriginalLocalLineageResolution::Pending);
        }
        return Ok(OriginalLocalLineageResolution::Ready(
            OriginalLocalLineage {
                profile: None,
                participation: None,
            },
        ));
    };
    if scope.request_id() != request_id
        || scope.workspace_id() != workspace_id
        || scope.session_id() != session_id
    {
        return Err(OperationsError::Corrupt("historical Profile scope"));
    }
    let profile_id = scope.profile_id().into_uuid();
    let profile_link_request_id = scope.profile_link_request_id();
    let profile_revision = positive(
        scope.profile_revision().get(),
        "historical Profile revision",
    )?;
    let session_revision = positive(
        scope.session_revision().get(),
        "historical Session revision",
    )?;
    let participation = load_original_participation_mapping(
        connection,
        OriginalParticipationQuery {
            workspace_id,
            profile_id,
            profile_link_request_id,
            profile_revision,
            session_revision,
            participant_id,
            session_id,
        },
    )?;
    Ok(OriginalLocalLineageResolution::Ready(
        OriginalLocalLineage {
            profile: Some(MissionEvidenceProfileProvenance {
                profile_id,
                mapping_request_id: profile_link_request_id,
                profile_revision,
                session_revision,
            }),
            participation: participation.map(|value| MissionEvidenceParticipationProvenance {
                participation_id: value.participation_id,
                mapping_request_id: Some(value.request_id),
                participant_membership_revision: value.participant_membership_revision,
                mapped_at_ms: Some(value.mapped_at_ms),
            }),
        },
    ))
}

fn resolve_replacement(
    connection: &Connection,
    destination_id: Uuid,
    workspace_id: WorkspaceId,
    expected_activity_id: Option<ActivityId>,
    agent_id: AgentInstanceId,
    session_id: AgentSessionId,
    request_id: Uuid,
) -> Result<MissionEvidenceIdentityResolution, OperationsError> {
    let Some(context) = load_replacement_context(
        connection,
        destination_id,
        workspace_id,
        expected_activity_id,
        agent_id,
        session_id,
        request_id,
    )?
    else {
        return Ok(pending(
            agent_id,
            session_id,
            MissionEvidenceIdentityPendingReason::ActivationMissing,
            Some(request_id),
            None,
        ));
    };
    validate_replacement_import_and_event(connection, &context)?;
    let Some(lineage) = load_replacement_lineage(connection, &context)? else {
        return Ok(pending(
            agent_id,
            session_id,
            MissionEvidenceIdentityPendingReason::ReplacementRebindMissing,
            Some(request_id),
            Some(request_id),
        ));
    };
    let proof = MissionEvidenceIdentityProof::from_parts(&MissionEvidenceIdentityProofParts {
        actor: MissionEvidenceIdentityActor {
            agent: agent_id,
            session: session_id,
            participant: context.participant_id,
        },
        registration: MissionEvidenceRegistrationProvenance {
            kind: MissionEvidenceRegistrationKind::Replacement,
            request_id,
            migration_id: context.migration_id,
            destination_id,
            freeze_revision: context.freeze_revision,
            membership_commit_digest: context.commit_digest,
            activation_receipt_id: request_id,
            activated_at_ms: context.activated_at_ms,
        },
        profile: Some(MissionEvidenceProfileProvenance {
            profile_id: context.profile_id,
            mapping_request_id: context.profile_link_request_id,
            profile_revision: context.profile_revision,
            session_revision: context.session_revision,
        }),
        participation: Some(lineage.participation),
        replacement: Some(lineage.replacement),
    })?;
    Ok(MissionEvidenceIdentityResolution::Verified(Box::new(proof)))
}

fn load_replacement_context(
    connection: &Connection,
    destination_id: Uuid,
    workspace_id: WorkspaceId,
    expected_activity_id: Option<ActivityId>,
    agent_id: AgentInstanceId,
    session_id: AgentSessionId,
    request_id: Uuid,
) -> Result<Option<ReplacementContext>, OperationsError> {
    let Some(raw) = load_replacement(connection, request_id)? else {
        return Err(OperationsError::Corrupt(
            "historical replacement disappeared",
        ));
    };
    let registration = RegistrationRequest::decode(&raw.request_wire)?;
    let stored_request_id = decode_uuid(raw.request_id, "historical replacement request")?;
    let migration_id = decode_uuid(raw.migration_id, "historical replacement migration")?;
    let stored_workspace = decode_workspace(raw.workspace_id)?;
    let stored_destination = decode_uuid(raw.destination_id, "historical replacement destination")?;
    let participation_id = decode_participation(raw.participation_id)?;
    let profile_id = decode_uuid(raw.profile_id, "historical replacement Profile")?;
    let profile_link_request_id = decode_uuid(
        raw.profile_mapping_request_id,
        "historical replacement Profile link",
    )?;
    let previous_session_id = decode_session(raw.previous_session_id)?;
    let stored_session_id = decode_session(raw.new_session_id)?;
    let participant_id = decode_uuid(raw.participant_id, "historical replacement participant")?;
    let activity_id = decode_uuid(raw.activity_id, "historical replacement Activity")?;
    let stored_agent_id = decode_agent(raw.agent_id)?;
    let binding = registration.binding();
    if stored_request_id != request_id
        || binding.request_id() != request_id
        || stored_workspace != workspace_id
        || stored_destination != destination_id
        || stored_session_id != session_id
        || stored_agent_id != agent_id
        || binding.activity_id().into_uuid() != activity_id
        || binding.agent_id() != agent_id
        || binding.session_id() != session_id
        || registration.proposed_participant_id() != participant_id
    {
        return Err(OperationsError::Corrupt("historical replacement identity"));
    }
    if expected_activity_id.is_some_and(|expected| expected.into_uuid() != activity_id) {
        return Err(OperationsError::Conflict(
            "mission evidence target Activity mismatch",
        ));
    }
    if !(0..=3).contains(&raw.phase) {
        return Err(OperationsError::Corrupt("historical replacement phase"));
    }
    if raw.phase != 3 {
        return Ok(None);
    }
    let committed = decode_replacement_commit_fields(RawReplacementCommitFields {
        freeze_revision: raw.freeze_revision,
        reserved_at_ms: raw.reserved_at_ms,
        commit_digest: raw.commit_digest,
        committed_at_ms: raw.committed_at_ms,
        joined_event_id: raw.joined_event_id,
        activated_at_ms: raw.activated_at_ms,
        profile_revision: raw.profile_revision,
        session_revision: raw.new_session_revision,
        expected_participation_revision: raw.expected_participation_revision,
        participant_membership_revision: raw.participant_membership_revision,
    })?;
    Ok(Some(ReplacementContext {
        migration_id,
        workspace_id,
        destination_id,
        participation_id,
        profile_id,
        profile_link_request_id,
        previous_session_id,
        session_id,
        profile_revision: committed.profile_revision,
        session_revision: committed.session_revision,
        participant_id,
        participant_membership_revision: committed.participant_membership_revision,
        expected_participation_revision: committed.expected_participation_revision,
        freeze_revision: committed.freeze_revision,
        commit_digest: committed.commit_digest,
        joined_event_id: committed.joined_event_id,
        activated_at_ms: committed.activated_at_ms,
    }))
}

fn decode_replacement_commit_fields(
    raw: RawReplacementCommitFields,
) -> Result<ReplacementCommitFields, OperationsError> {
    let freeze_revision = positive(
        raw.freeze_revision.ok_or(OperationsError::Corrupt(
            "historical replacement freeze revision",
        ))?,
        "historical replacement freeze revision",
    )?;
    nonnegative(
        raw.reserved_at_ms.ok_or(OperationsError::Corrupt(
            "historical replacement reservation",
        ))?,
        "historical replacement reservation",
    )?;
    let commit_digest = decode_digest(
        raw.commit_digest
            .ok_or(OperationsError::Corrupt("historical replacement commit"))?,
        "historical replacement commit digest",
    )?;
    nonnegative(
        raw.committed_at_ms.ok_or(OperationsError::Corrupt(
            "historical replacement commit time",
        ))?,
        "historical replacement commit time",
    )?;
    let joined_event_id = decode_uuid(
        raw.joined_event_id
            .ok_or(OperationsError::Corrupt("historical replacement event"))?,
        "historical replacement event",
    )?;
    let activated_at_ms = nonnegative(
        raw.activated_at_ms.ok_or(OperationsError::Corrupt(
            "historical replacement activation",
        ))?,
        "historical replacement activation",
    )?;
    Ok(ReplacementCommitFields {
        freeze_revision,
        commit_digest,
        joined_event_id,
        activated_at_ms,
        profile_revision: positive(
            raw.profile_revision,
            "historical replacement Profile revision",
        )?,
        session_revision: positive(
            raw.session_revision,
            "historical replacement Session revision",
        )?,
        expected_participation_revision: positive(
            raw.expected_participation_revision,
            "historical replacement expected participation revision",
        )?,
        participant_membership_revision: positive(
            raw.participant_membership_revision,
            "historical replacement membership revision",
        )?,
    })
}

fn validate_replacement_import_and_event(
    connection: &Connection,
    context: &ReplacementContext,
) -> Result<(), OperationsError> {
    validate_import(
        connection,
        context.migration_id,
        context.workspace_id,
        context.destination_id,
        i64::try_from(context.freeze_revision)
            .map_err(|_| OperationsError::Corrupt("historical replacement freeze revision"))?,
    )?;
    validate_membership_event(
        connection,
        context.workspace_id,
        context.participant_id,
        None,
        context.joined_event_id,
        None,
    )
}

fn load_replacement_lineage(
    connection: &Connection,
    context: &ReplacementContext,
) -> Result<Option<ReplacementLineage>, OperationsError> {
    let Some(rebind) = crate::session_replacement::load_rebind_by_session(
        connection,
        context.workspace_id,
        context.session_id,
    )?
    else {
        return Ok(None);
    };
    if rebind.workspace_id() != context.workspace_id
        || rebind.participation_id() != context.participation_id
        || rebind.profile_id().into_uuid() != context.profile_id
        || positive(
            rebind.profile_revision().get(),
            "historical Session rebind Profile revision",
        )? != context.profile_revision
        || rebind.profile_mapping_request_id() != context.profile_link_request_id
        || rebind.previous_session_id() != context.previous_session_id
        || rebind.new_session_id() != context.session_id
        || positive(
            rebind.new_session_revision().get(),
            "historical Session rebind Session revision",
        )? != context.session_revision
        || rebind.participant_id() != context.participant_id
        || rebind.participant_membership_revision() != context.participant_membership_revision
        || rebind.expected_participation_revision() != context.expected_participation_revision
        || context.expected_participation_revision.checked_add(1)
            != Some(rebind.result_participation_revision())
    {
        return Err(OperationsError::Corrupt("historical Session rebind"));
    }
    let source_mapping = load_replacement_source_mapping(
        connection,
        context.workspace_id,
        context.participation_id,
        context.profile_id,
        context.participant_id,
    )?;
    if source_mapping.as_ref().is_some_and(|mapping| {
        mapping.session_id != context.previous_session_id
            || mapping.profile_id != context.profile_id
            || mapping.participation_id != context.participation_id
            || mapping.participant_id != context.participant_id
            || mapping.participant_membership_revision != context.participant_membership_revision
    }) {
        return Err(OperationsError::Corrupt(
            "replacement participation origin mapping",
        ));
    }
    let (mapping_request_id, mapping_member_revision, mapping_at) = match source_mapping {
        Some(value) => (
            Some(value.request_id),
            Some(value.participant_membership_revision),
            Some(value.mapped_at_ms),
        ),
        None => (None, None, None),
    };
    Ok(Some(ReplacementLineage {
        participation: MissionEvidenceParticipationProvenance {
            participation_id: context.participation_id,
            mapping_request_id,
            participant_membership_revision: mapping_member_revision
                .unwrap_or(context.participant_membership_revision),
            mapped_at_ms: mapping_at,
        },
        replacement: MissionEvidenceReplacementProvenance {
            rebind_request_id: rebind.request_id(),
            rebind_revision: rebind.result_participation_revision(),
        },
    }))
}

fn load_original(
    connection: &Connection,
    request_id: Uuid,
) -> Result<Option<OriginalRow>, OperationsError> {
    Ok(connection
        .query_row(
            "SELECT substr(request_id,1,17),substr(migration_id,1,17),substr(workspace_id,1,17),substr(destination_id,1,17),substr(activity_id,1,17),substr(session_id,1,17),substr(participant_id,1,17),substr(agent_id,1,17),substr(request_wire,1,4097),phase,reserved_at_ms \
             FROM ops_registration_requests WHERE request_id=?1 LIMIT 1",
            params![request_id.as_bytes()],
            |row| {
                Ok(OriginalRow {
                    request_id: row.get(0)?,
                    migration_id: row.get(1)?,
                    workspace_id: row.get(2)?,
                    destination_id: row.get(3)?,
                    activity_id: row.get(4)?,
                    session_id: row.get(5)?,
                    participant_id: row.get(6)?,
                    agent_id: row.get(7)?,
                    request_wire: row.get(8)?,
                    phase: row.get(9)?,
                    reserved_at_ms: row.get(10)?,
                })
            },
        )
        .optional()?)
}

fn load_original_participation_mapping(
    connection: &Connection,
    query: OriginalParticipationQuery,
) -> Result<Option<ParticipationMapping>, OperationsError> {
    let OriginalParticipationQuery {
        workspace_id,
        profile_id,
        profile_link_request_id,
        profile_revision,
        session_revision,
        participant_id,
        session_id,
    } = query;
    let raw = connection
        .query_row(
            "SELECT substr(request_id,1,17),substr(workspace_id,1,17),substr(participation_id,1,17),substr(agent_profile_id,1,17),substr(profile_mapping_request_id,1,17),profile_revision,substr(session_id,1,17),session_revision,substr(participant_id,1,17),participant_membership_revision,mapped_at_ms \
             FROM ops_workspace_participation_mappings WHERE workspace_id=?1 AND participant_id=?2 LIMIT 1",
            params![workspace_id.into_uuid().as_bytes(), participant_id.as_bytes()],
            |row| {
                Ok(RawParticipationMapping {
                    request_id: row.get(0)?,
                    workspace_id: row.get(1)?,
                    participation_id: row.get(2)?,
                    profile_id: row.get(3)?,
                    profile_mapping_request_id: row.get(4)?,
                    profile_revision: row.get(5)?,
                    session_id: row.get(6)?,
                    session_revision: row.get(7)?,
                    participant_id: row.get(8)?,
                    participant_membership_revision: row.get(9)?,
                    mapped_at_ms: row.get(10)?,
                })
            },
        )
        .optional()?;
    let Some(row) = raw else {
        return Ok(None);
    };
    let stored_workspace = decode_workspace(row.workspace_id.clone())?;
    let stored_participant = decode_uuid(
        row.participant_id.clone(),
        "participation mapping participant",
    )?;
    let stored_profile = decode_uuid(row.profile_id.clone(), "participation mapping Profile")?;
    let stored_profile_link = decode_uuid(
        row.profile_mapping_request_id.clone(),
        "participation mapping Profile link",
    )?;
    let stored_profile_revision = positive(
        row.profile_revision,
        "participation mapping Profile revision",
    )?;
    let stored_session_revision = positive(
        row.session_revision,
        "participation mapping Session revision",
    )?;
    if stored_workspace != workspace_id
        || stored_participant != participant_id
        || stored_profile != profile_id
        || stored_profile_link != profile_link_request_id
        || stored_profile_revision != profile_revision
        || decode_session(row.session_id.clone())? != session_id
        || stored_session_revision != session_revision
    {
        return Err(OperationsError::Corrupt("historical participation mapping"));
    }
    let participation_id = decode_participation(row.participation_id.clone())?;
    let mapped_at_ms = nonnegative(row.mapped_at_ms, "participation mapping timestamp")?;
    let participant_membership_revision = positive(
        row.participant_membership_revision,
        "participation mapping membership revision",
    )?;
    validate_participation_anchor(connection, workspace_id, profile_id, participation_id)?;
    Ok(Some(ParticipationMapping {
        request_id: decode_uuid(row.request_id, "participation mapping request")?,
        participation_id,
        profile_id,
        session_id,
        participant_id,
        participant_membership_revision,
        mapped_at_ms,
    }))
}

fn validate_participation_anchor(
    connection: &Connection,
    workspace_id: WorkspaceId,
    profile_id: Uuid,
    participation_id: WorkspaceParticipationId,
) -> Result<(), OperationsError> {
    let row = connection
        .query_row(
            "SELECT substr(participation_id,1,17),revision FROM ops_workspace_participations WHERE workspace_id=?1 AND agent_profile_id=?2 LIMIT 1",
            params![workspace_id.into_uuid().as_bytes(), profile_id.as_bytes()],
            |row| Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, i64>(1)?)),
        )
        .optional()?
        .ok_or(OperationsError::Corrupt("participation anchor missing"))?;
    if decode_participation(row.0)? != participation_id || row.1 < 1 {
        return Err(OperationsError::Corrupt("participation anchor identity"));
    }
    Ok(())
}

fn load_replacement(
    connection: &Connection,
    request_id: Uuid,
) -> Result<Option<ReplacementRow>, OperationsError> {
    Ok(connection
        .query_row(
            "SELECT substr(request_id,1,17),substr(migration_id,1,17),substr(workspace_id,1,17),substr(destination_id,1,17),substr(participation_id,1,17),substr(agent_profile_id,1,17),profile_revision,substr(profile_mapping_request_id,1,17),substr(previous_session_id,1,17),substr(new_session_id,1,17),new_session_revision,substr(participant_id,1,17),participant_membership_revision,expected_participation_revision,substr(activity_id,1,17),substr(agent_id,1,17),substr(request_wire,1,4097),phase,freeze_revision,reserved_at_ms,substr(commit_digest,1,33),committed_at_ms,substr(joined_event_id,1,17),activated_at_ms \
             FROM ops_workspace_participation_replacement_registrations WHERE request_id=?1 LIMIT 1",
            params![request_id.as_bytes()],
            |row| {
                Ok(ReplacementRow {
                    request_id: row.get(0)?,
                    migration_id: row.get(1)?,
                    workspace_id: row.get(2)?,
                    destination_id: row.get(3)?,
                    participation_id: row.get(4)?,
                    profile_id: row.get(5)?,
                    profile_revision: row.get(6)?,
                    profile_mapping_request_id: row.get(7)?,
                    previous_session_id: row.get(8)?,
                    new_session_id: row.get(9)?,
                    new_session_revision: row.get(10)?,
                    participant_id: row.get(11)?,
                    participant_membership_revision: row.get(12)?,
                    expected_participation_revision: row.get(13)?,
                    activity_id: row.get(14)?,
                    agent_id: row.get(15)?,
                    request_wire: row.get(16)?,
                    phase: row.get(17)?,
                    freeze_revision: row.get(18)?,
                    reserved_at_ms: row.get(19)?,
                    commit_digest: row.get(20)?,
                    committed_at_ms: row.get(21)?,
                    joined_event_id: row.get(22)?,
                    activated_at_ms: row.get(23)?,
                })
            },
        )
        .optional()?)
}

fn load_replacement_source_mapping(
    connection: &Connection,
    workspace_id: WorkspaceId,
    participation_id: WorkspaceParticipationId,
    profile_id: Uuid,
    participant_id: Uuid,
) -> Result<Option<ParticipationMapping>, OperationsError> {
    let row = connection
        .query_row(
            "SELECT substr(request_id,1,17),substr(workspace_id,1,17),substr(participation_id,1,17),substr(agent_profile_id,1,17),substr(profile_mapping_request_id,1,17),profile_revision,substr(session_id,1,17),session_revision,substr(participant_id,1,17),participant_membership_revision,mapped_at_ms \
             FROM ops_workspace_participation_mappings WHERE workspace_id=?1 AND participation_id=?2 AND agent_profile_id=?3 AND participant_id=?4 LIMIT 1",
            params![
                workspace_id.into_uuid().as_bytes(),
                participation_id.into_uuid().as_bytes(),
                profile_id.as_bytes(),
                participant_id.as_bytes(),
            ],
            |row| {
                Ok(RawParticipationMapping {
                    request_id: row.get(0)?,
                    workspace_id: row.get(1)?,
                    participation_id: row.get(2)?,
                    profile_id: row.get(3)?,
                    profile_mapping_request_id: row.get(4)?,
                    profile_revision: row.get(5)?,
                    session_id: row.get(6)?,
                    session_revision: row.get(7)?,
                    participant_id: row.get(8)?,
                    participant_membership_revision: row.get(9)?,
                    mapped_at_ms: row.get(10)?,
                })
            },
        )
        .optional()?;
    let Some(row) = row else {
        return Ok(None);
    };
    if decode_workspace(row.workspace_id.clone())? != workspace_id
        || decode_participation(row.participation_id.clone())? != participation_id
        || decode_uuid(row.profile_id.clone(), "replacement participation Profile")? != profile_id
        || decode_uuid(
            row.participant_id.clone(),
            "replacement participation participant",
        )? != participant_id
    {
        return Err(OperationsError::Corrupt(
            "replacement participation mapping identity",
        ));
    }
    let _ = decode_uuid(
        row.profile_mapping_request_id.clone(),
        "replacement participation Profile link",
    )?;
    let _ = decode_session(row.session_id.clone())?;
    positive(
        row.profile_revision,
        "replacement participation Profile revision",
    )?;
    positive(
        row.session_revision,
        "replacement participation Session revision",
    )?;
    let participant_membership_revision = positive(
        row.participant_membership_revision,
        "replacement participation membership revision",
    )?;
    let mapped_at_ms = nonnegative(row.mapped_at_ms, "replacement participation mapping time")?;
    validate_participation_anchor(connection, workspace_id, profile_id, participation_id)?;
    Ok(Some(ParticipationMapping {
        request_id: decode_uuid(row.request_id, "replacement participation mapping request")?,
        participation_id: decode_participation(row.participation_id)?,
        profile_id: decode_uuid(row.profile_id, "replacement participation Profile")?,
        session_id: decode_session(row.session_id)?,
        participant_id: decode_uuid(row.participant_id, "replacement participation participant")?,
        participant_membership_revision,
        mapped_at_ms,
    }))
}

fn validate_import(
    connection: &Connection,
    migration_id: Uuid,
    workspace_id: WorkspaceId,
    destination_id: Uuid,
    freeze_revision: i64,
) -> Result<(), OperationsError> {
    let progress = crate::catalog::load_progress(connection, migration_id)?
        .ok_or(OperationsError::Corrupt("historical actor import missing"))?;
    let import_was_verified = progress.verified_pages() == progress.page_count()
        && progress.verify_digest() == progress.digest();
    if progress.workspace_id() != workspace_id
        || progress.destination_id() != destination_id
        || progress.source_revision()
            != u64::try_from(freeze_revision)
                .map_err(|_| OperationsError::Corrupt("historical actor freeze revision"))?
        || !import_was_verified
        || !matches!(
            progress.phase(),
            crate::ImportPhase::Verified | crate::ImportPhase::Aborted
        )
    {
        return Err(OperationsError::Corrupt("historical actor import identity"));
    }
    Ok(())
}

fn validate_membership_event(
    connection: &Connection,
    workspace_id: WorkspaceId,
    participant_id: Uuid,
    activity_id: Option<Uuid>,
    event_id: Uuid,
    committed_at_ms: Option<i64>,
) -> Result<(), OperationsError> {
    let row = connection
        .query_row(
            "SELECT substr(participant_id,1,17),substr(activity_id,1,17),event_kind,membership_revision,observed_at_ms \
             FROM ops_logical_events WHERE workspace_id=?1 AND event_id=?2 LIMIT 1",
            params![workspace_id.into_uuid().as_bytes(), event_id.as_bytes()],
            |row| {
                Ok((
                    row.get::<_, Vec<u8>>(0)?,
                    row.get::<_, Vec<u8>>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, i64>(4)?,
                ))
            },
        )
        .optional()?
        .ok_or(OperationsError::Corrupt("historical JOINED event missing"))?;
    let event_activity = decode_uuid(row.1, "historical JOINED Activity")?;
    if decode_uuid(row.0, "historical JOINED participant")? != participant_id
        || activity_id.is_some_and(|activity| event_activity != activity)
        || row.2 != 0
        || row.3 != 1
        || row.4 < 0
        || committed_at_ms.is_some_and(|at| row.4 != at)
    {
        return Err(OperationsError::Corrupt("historical JOINED event identity"));
    }
    Ok(())
}

fn pending(
    agent_id: AgentInstanceId,
    session_id: AgentSessionId,
    reason: MissionEvidenceIdentityPendingReason,
    registration_request_id: Option<Uuid>,
    activation_receipt_id: Option<Uuid>,
) -> MissionEvidenceIdentityResolution {
    MissionEvidenceIdentityResolution::Pending {
        agent_id,
        session_id,
        reason,
        registration_request_id,
        activation_receipt_id,
    }
}

fn decode_uuid(bytes: Vec<u8>, label: &'static str) -> Result<Uuid, OperationsError> {
    let bytes: [u8; 16] = bytes
        .try_into()
        .map_err(|_| OperationsError::Corrupt(label))?;
    let value = Uuid::from_bytes(bytes);
    if value.is_nil() {
        return Err(OperationsError::Corrupt(label));
    }
    Ok(value)
}

fn decode_workspace(bytes: Vec<u8>) -> Result<WorkspaceId, OperationsError> {
    Ok(WorkspaceId::from_uuid(decode_uuid(
        bytes,
        "historical Workspace",
    )?))
}

fn decode_agent(bytes: Vec<u8>) -> Result<AgentInstanceId, OperationsError> {
    Ok(AgentInstanceId::from_uuid(decode_uuid(
        bytes,
        "historical Agent",
    )?))
}

fn decode_session(bytes: Vec<u8>) -> Result<AgentSessionId, OperationsError> {
    Ok(AgentSessionId::from_uuid(decode_uuid(
        bytes,
        "historical Session",
    )?))
}

fn decode_participation(bytes: Vec<u8>) -> Result<WorkspaceParticipationId, OperationsError> {
    WorkspaceParticipationId::from_uuid(decode_uuid(bytes, "historical participation")?)
        .map_err(|_| OperationsError::Corrupt("historical participation"))
}

fn decode_digest(bytes: Vec<u8>, label: &'static str) -> Result<[u8; 32], OperationsError> {
    let value: [u8; 32] = bytes
        .try_into()
        .map_err(|_| OperationsError::Corrupt(label))?;
    if value == [0; 32] {
        return Err(OperationsError::Corrupt(label));
    }
    Ok(value)
}

fn positive(value: i64, label: &'static str) -> Result<u64, OperationsError> {
    u64::try_from(value)
        .ok()
        .filter(|value| *value > 0)
        .ok_or(OperationsError::Corrupt(label))
}

fn nonnegative(value: i64, label: &'static str) -> Result<i64, OperationsError> {
    if value < 0 {
        Err(OperationsError::Corrupt(label))
    } else {
        Ok(value)
    }
}
