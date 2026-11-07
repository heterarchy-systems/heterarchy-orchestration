//! Durable admission of a Team's temporary-assistance request.
//!
//! Admission records quota history and immutable intent only. Its lifecycle
//! child owns authenticated consent and inbound Team-seat reservation; neither
//! operation activates work or proves that requested source work exists.
//! `requested_scope` remains bounded request intent until a source owner captures
//! the immutable work snapshot.

use forge_domain::{ActivityId, AgentInstanceId, AgentSessionId, TimestampMillis, WorkspaceId};
use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use uuid::Uuid;

use crate::{
    CurrentLeadershipAuthority, OperationsCatalog, OperationsError, WorkspaceParticipationId,
    operational_actor::{OperationalActor, ResolvedOperationalActor, resolve_actor},
    wire,
};

pub(crate) mod lifecycle;
mod quota;
mod schema;

pub use lifecycle::{
    TemporaryDispatchAdmissionBinding, TemporaryDispatchConsentCommand,
    TemporaryDispatchConsentKind, TemporaryDispatchConsentReceipt,
    TemporaryDispatchConsentWithdrawalCommand, TemporaryDispatchConsentWithdrawalReceipt,
    TemporaryDispatchReservationReceipt, TemporaryDispatchReservationState,
    TemporaryDispatchReserveCommand,
};

pub(crate) use schema::{create_schema, schema_objects};

const MAX_REQUESTED_SCOPE_BYTES: usize = 2_048;
const TF_ADMISSION_WINDOW_MS: i64 = 60 * 60 * 1_000;

/// Bounded request-supplied scope description.
///
/// This value records intent only. It is not a Forge Work, Unit, criteria, or
/// source-evidence snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TemporaryDispatchRequestedScope(String);

impl TemporaryDispatchRequestedScope {
    /// Construct bounded, non-empty request intent.
    ///
    /// # Errors
    /// Rejects empty scope text or text larger than 2,048 UTF-8 bytes.
    pub fn new(value: String) -> Result<Self, OperationsError> {
        if value.trim().is_empty() || value.len() > MAX_REQUESTED_SCOPE_BYTES {
            return Err(OperationsError::Bounds(
                "temporary dispatch requested scope",
            ));
        }
        Ok(Self(value))
    }

    /// The exact text supplied in the immutable request payload.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Exact, typed intent for one new temporary-assistance request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TemporaryDispatchAdmissionRequest {
    request_id: Uuid,
    workspace_id: WorkspaceId,
    requesting_team_id: Uuid,
    expected_leadership_revision: u64,
    actor: OperationalActor,
    target_participant_id: Uuid,
    requested_scope: TemporaryDispatchRequestedScope,
    requested_end_at: TimestampMillis,
}

impl TemporaryDispatchAdmissionRequest {
    /// Construct a request without accepting caller-selected time or role.
    ///
    /// `requesting_team_id` is the receiving Team whose current Leader submits
    /// the request. The target's home Team is resolved from current membership
    /// inside the same SQLite transaction.
    ///
    /// # Errors
    /// Rejects nil identities, non-participant actors, invalid revisions, or
    /// an unbounded scope.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        request_id: Uuid,
        workspace_id: WorkspaceId,
        requesting_team_id: Uuid,
        expected_leadership_revision: u64,
        actor: OperationalActor,
        target_participant_id: Uuid,
        requested_scope: TemporaryDispatchRequestedScope,
        requested_end_at: TimestampMillis,
    ) -> Result<Self, OperationsError> {
        let leadership_revision = i64::try_from(expected_leadership_revision)
            .map_err(|_| OperationsError::Bounds("Team leadership revision"))?;
        if request_id.is_nil()
            || workspace_id.into_uuid().is_nil()
            || requesting_team_id.is_nil()
            || target_participant_id.is_nil()
            || leadership_revision <= 0
            || matches!(actor, OperationalActor::LocalOperator)
        {
            return Err(OperationsError::Conflict(
                "temporary dispatch request identity",
            ));
        }
        Ok(Self {
            request_id,
            workspace_id,
            requesting_team_id,
            expected_leadership_revision,
            actor,
            target_participant_id,
            requested_scope,
            requested_end_at,
        })
    }

    /// The immutable request identity.
    #[must_use]
    pub const fn request_id(&self) -> Uuid {
        self.request_id
    }

    /// The exact Workspace scope.
    #[must_use]
    pub const fn workspace_id(&self) -> WorkspaceId {
        self.workspace_id
    }

    /// The receiving/requesting Team whose quota applies.
    #[must_use]
    pub const fn requesting_team_id(&self) -> Uuid {
        self.requesting_team_id
    }

    /// The exact Team leadership revision asserted by the authenticated caller.
    #[must_use]
    pub const fn expected_leadership_revision(&self) -> u64 {
        self.expected_leadership_revision
    }

    /// The transport actor resolved again inside the mutation transaction.
    #[must_use]
    pub const fn actor(&self) -> OperationalActor {
        self.actor
    }

    /// The stable logical participant requested for temporary assistance.
    #[must_use]
    pub const fn target_participant_id(&self) -> Uuid {
        self.target_participant_id
    }

    /// Bounded request intent; it does not prove source work.
    #[must_use]
    pub fn requested_scope(&self) -> &TemporaryDispatchRequestedScope {
        &self.requested_scope
    }

    /// Requested absolute end time, checked against trusted admission time.
    #[must_use]
    pub const fn requested_end_at(&self) -> TimestampMillis {
        self.requested_end_at
    }
}

/// Original authenticated transport plus the durable logical actor identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TemporaryDispatchActorProvenance {
    activity_id: ActivityId,
    agent_id: AgentInstanceId,
    session_id: AgentSessionId,
    participation_id: WorkspaceParticipationId,
    profile_id: Uuid,
    participant_id: Uuid,
    display_name: String,
}

impl TemporaryDispatchActorProvenance {
    /// Original transport Activity.
    #[must_use]
    pub const fn activity_id(&self) -> ActivityId {
        self.activity_id
    }

    /// Original transport Agent instance.
    #[must_use]
    pub const fn agent_id(&self) -> AgentInstanceId {
        self.agent_id
    }

    /// Original transport Session, retained after a later Session rebind.
    pub const fn session_id(&self) -> AgentSessionId {
        self.session_id
    }

    /// Stable Workspace participation identity.
    #[must_use]
    pub const fn participation_id(&self) -> WorkspaceParticipationId {
        self.participation_id
    }

    /// Stable Agent Profile identity.
    #[must_use]
    pub const fn profile_id(&self) -> Uuid {
        self.profile_id
    }

    /// Stable logical participant identity.
    #[must_use]
    pub const fn participant_id(&self) -> Uuid {
        self.participant_id
    }

    /// Display-name snapshot from the resolved logical actor.
    #[must_use]
    pub fn display_name(&self) -> &str {
        &self.display_name
    }
}

/// Definitive result of one temporary-dispatch admission attempt.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TemporaryDispatchAdmissionResult {
    /// A durable admission receipt entered Team quota history.
    Admitted(Box<TemporaryDispatchRequestRecord>),
    /// The request was blocked by the rolling quota or fixed lock.
    /// This result is not persisted, counted, or used to change the lock.
    /// The same request ID may be evaluated again later.
    QuotaRejected {
        /// Request identity supplied for this attempt.
        request_id: Uuid,
        /// Request Workspace.
        workspace_id: WorkspaceId,
        /// Receiving Team whose quota applies.
        requesting_team_id: Uuid,
        /// Fixed boundary at or after which a new attempt may be evaluated.
        retry_at: TimestampMillis,
    },
}

/// Immutable admission receipt or quota-rejection readback.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TemporaryDispatchRequestRecord {
    request_id: Uuid,
    workspace_id: WorkspaceId,
    requesting_team_id: Uuid,
    leadership_revision: u64,
    actor: TemporaryDispatchActorProvenance,
    target_home_team_id: Uuid,
    target_participant_id: Uuid,
    requested_scope: TemporaryDispatchRequestedScope,
    requested_end_at: TimestampMillis,
    admitted_at: TimestampMillis,
    lock_until: Option<TimestampMillis>,
    replayed: bool,
}

impl TemporaryDispatchRequestRecord {
    /// Immutable request identity.
    #[must_use]
    pub const fn request_id(&self) -> Uuid {
        self.request_id
    }

    /// Request Workspace.
    #[must_use]
    pub const fn workspace_id(&self) -> WorkspaceId {
        self.workspace_id
    }

    /// Receiving Team whose quota history applies.
    #[must_use]
    pub const fn requesting_team_id(&self) -> Uuid {
        self.requesting_team_id
    }

    /// Confirmed Team leadership revision used by the admission check.
    #[must_use]
    pub const fn leadership_revision(&self) -> u64 {
        self.leadership_revision
    }

    /// Original request actor and stable logical identity.
    #[must_use]
    pub const fn actor(&self) -> &TemporaryDispatchActorProvenance {
        &self.actor
    }

    /// Home Team derived from one unambiguous current normal membership.
    #[must_use]
    pub const fn target_home_team_id(&self) -> Uuid {
        self.target_home_team_id
    }

    /// Stable logical target participant.
    #[must_use]
    pub const fn target_participant_id(&self) -> Uuid {
        self.target_participant_id
    }

    /// Exact bounded request intent.
    #[must_use]
    pub fn requested_scope(&self) -> &TemporaryDispatchRequestedScope {
        &self.requested_scope
    }

    /// Requested end time.
    #[must_use]
    pub const fn requested_end_at(&self) -> TimestampMillis {
        self.requested_end_at
    }

    /// Trusted server time sampled inside the IMMEDIATE transaction.
    #[must_use]
    pub const fn admitted_at(&self) -> TimestampMillis {
        self.admitted_at
    }

    /// Fixed lock created by this third admission, when applicable.
    #[must_use]
    pub const fn lock_until(&self) -> Option<TimestampMillis> {
        self.lock_until
    }

    /// Whether this result came from exact request-ID readback.
    #[must_use]
    pub const fn replayed(&self) -> bool {
        self.replayed
    }

    fn with_replayed(mut self) -> Self {
        self.replayed = true;
        self
    }

    fn matches_intent(
        &self,
        request: &TemporaryDispatchAdmissionRequest,
        actor: &TemporaryDispatchActorProvenance,
    ) -> bool {
        self.matches_payload(request)
            && self.matches_logical_actor(actor)
            && match request.actor {
                OperationalActor::Participant {
                    activity_id,
                    agent_id,
                    session_id,
                } => {
                    self.actor.activity_id == activity_id
                        && self.actor.agent_id == agent_id
                        && self.actor.session_id == session_id
                }
                OperationalActor::LocalOperator => false,
            }
    }

    fn matches_readback_intent(
        &self,
        request: &TemporaryDispatchAdmissionRequest,
        actor: &TemporaryDispatchActorProvenance,
    ) -> bool {
        self.matches_payload(request) && self.matches_logical_actor(actor)
    }

    fn matches_payload(&self, request: &TemporaryDispatchAdmissionRequest) -> bool {
        self.workspace_id == request.workspace_id
            && self.requesting_team_id == request.requesting_team_id
            && self.leadership_revision == request.expected_leadership_revision
            && self.target_participant_id == request.target_participant_id
            && self.requested_scope == request.requested_scope
            && self.requested_end_at == request.requested_end_at
    }

    fn matches_logical_actor(&self, actor: &TemporaryDispatchActorProvenance) -> bool {
        self.actor.participant_id == actor.participant_id
            && self.actor.profile_id == actor.profile_id
            && self.actor.participation_id == actor.participation_id
    }
}

impl OperationsCatalog {
    /// Admit one valid new temporary-assistance request under the Team quota.
    ///
    /// This records a new-request admission receipt only; consent, destination
    /// capacity, activation, fixed-work snapshots, draining and return remain
    /// later lifecycle obligations. This method does not establish extensions.
    /// The Runtime/Backend supplies a trusted one-shot
    /// clock closure; the closure runs once inside this transaction after
    /// validation. Exact replays are resolved before clock and quota checks.
    /// A quota rejection is returned without a write; it never enters
    /// admission history or changes the fixed lock. An admitted receipt can be
    /// read back exactly after an uncertain commit or Session rebind.
    ///
    /// # Errors
    /// Rejects stale/unauthorized Team leadership, malformed or ambiguous
    /// membership, clock failure/regression/overflow, conflicting request-ID
    /// reuse, corrupt durable state and SQLite failures.
    pub fn admit_temporary_dispatch(
        &mut self,
        request: &TemporaryDispatchAdmissionRequest,
        trusted_now: impl FnOnce() -> Result<TimestampMillis, OperationsError>,
    ) -> Result<TemporaryDispatchAdmissionResult, OperationsError> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        crate::teams::ensure_verified_workspace(&transaction, request.workspace_id)?;
        let resolved_actor = resolve_actor(&transaction, request.workspace_id, request.actor)?;
        let actor = resolve_actor_provenance(request.actor, &resolved_actor)?;

        if let Some(existing) = load_request(&transaction, request.request_id)? {
            if !existing.matches_intent(request, &actor) {
                return Err(OperationsError::Conflict(
                    "temporary dispatch request replay",
                ));
            }
            let record = existing.with_replayed();
            transaction.commit()?;
            return Ok(TemporaryDispatchAdmissionResult::Admitted(Box::new(record)));
        }

        crate::teams::ensure_team_formed(
            &transaction,
            request.workspace_id,
            request.requesting_team_id,
        )?;
        ensure_current_requesting_leader(&transaction, request, &actor)?;
        ensure_joined_requesting_member(&transaction, request, &actor)?;
        let home_team_id = resolve_target_home_team(
            &transaction,
            request.workspace_id,
            request.requesting_team_id,
            request.target_participant_id,
        )?;

        // This is the only clock sample. It occurs after all authority and
        // payload checks and before any quota-history insertion.
        let now = sample_clock(trusted_now)?;
        if request.requested_end_at <= now {
            return Err(OperationsError::Bounds(
                "temporary dispatch requested end time",
            ));
        }

        let outcome =
            quota::record_admission_attempt(&transaction, request, &actor, home_team_id, now)?;
        let record = match outcome {
            rejected @ TemporaryDispatchAdmissionResult::QuotaRejected { .. } => {
                transaction.rollback()?;
                return Ok(rejected);
            }
            TemporaryDispatchAdmissionResult::Admitted(record) => record,
        };
        match transaction.commit() {
            Ok(()) => Ok(TemporaryDispatchAdmissionResult::Admitted(record)),
            Err(source) => match load_request(&self.connection, request.request_id) {
                Ok(Some(existing)) if existing.matches_intent(request, &actor) => Ok(
                    TemporaryDispatchAdmissionResult::Admitted(Box::new(existing.with_replayed())),
                ),
                Ok(Some(_)) => Err(OperationsError::Conflict(
                    "temporary dispatch request replay after uncertain commit",
                )),
                Ok(None) | Err(_) => Err(OperationsError::OperationCommitUnconfirmed {
                    workspace_id: request.workspace_id,
                    request_id: request.request_id,
                    source,
                }),
            },
        }
    }

    /// Read one exact request after an uncertain response or later Session rebind.
    ///
    /// The current Session/Activity/Agent binding must still resolve to the
    /// same stable logical actor that owns the original request. The original
    /// transport provenance returned in the record is never rewritten.
    ///
    /// # Errors
    /// Rejects an invalid current actor binding, corrupt receipt data and
    /// SQLite failures.
    pub fn read_temporary_dispatch_request(
        &mut self,
        request: &TemporaryDispatchAdmissionRequest,
    ) -> Result<Option<TemporaryDispatchRequestRecord>, OperationsError> {
        let transaction = self.connection.unchecked_transaction()?;
        crate::teams::ensure_verified_workspace(&transaction, request.workspace_id)?;
        let resolved_actor = resolve_actor(&transaction, request.workspace_id, request.actor)?;
        let actor = resolve_actor_provenance(request.actor, &resolved_actor)?;
        let record = load_request(&transaction, request.request_id)?;
        let record = record.and_then(|record| {
            record
                .matches_readback_intent(request, &actor)
                .then(|| record.with_replayed())
        });
        transaction.commit()?;
        Ok(record)
    }
}

fn resolve_actor_provenance(
    actor_input: OperationalActor,
    actor: &ResolvedOperationalActor,
) -> Result<TemporaryDispatchActorProvenance, OperationsError> {
    let OperationalActor::Participant {
        activity_id,
        agent_id,
        session_id,
    } = actor_input
    else {
        return Err(OperationsError::Conflict(
            "temporary dispatch requires a Team Leader participant",
        ));
    };
    if !actor.is_profile_participant() || actor.session_id() != Some(session_id) {
        return Err(OperationsError::Conflict(
            "temporary dispatch requires a logical Profile participant",
        ));
    }
    Ok(TemporaryDispatchActorProvenance {
        activity_id,
        agent_id,
        session_id,
        participation_id: actor.participation_id().ok_or(OperationsError::Corrupt(
            "temporary dispatch actor participation",
        ))?,
        profile_id: actor
            .profile_id()
            .ok_or(OperationsError::Corrupt("temporary dispatch actor Profile"))?,
        participant_id: actor.participant_id().ok_or(OperationsError::Corrupt(
            "temporary dispatch actor participant",
        ))?,
        display_name: actor.display_name().to_owned(),
    })
}

fn ensure_current_requesting_leader(
    transaction: &Transaction<'_>,
    request: &TemporaryDispatchAdmissionRequest,
    actor: &TemporaryDispatchActorProvenance,
) -> Result<(), OperationsError> {
    let state = transaction
        .query_row(
            "SELECT state FROM ops_leadership WHERE workspace_id=?1 AND scope_kind=2 AND activity_id IS NULL AND team_id=?2",
            params![
                request.workspace_id.into_uuid().as_bytes(),
                request.requesting_team_id.as_bytes(),
            ],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .ok_or(OperationsError::Conflict(
            "requesting Team has no current Leader",
        ))?;
    let authority = crate::leadership::current_leadership_authority_in_transaction(
        transaction,
        crate::LeadershipScope::team(request.workspace_id, request.requesting_team_id),
    )?
    .map(|value: CurrentLeadershipAuthority| (value.participant_id(), value.leadership_revision()));
    validate_requesting_leader_authority(
        &state,
        authority,
        actor.participant_id,
        i64::try_from(request.expected_leadership_revision)
            .map_err(|_| OperationsError::Bounds("Team leadership revision"))?,
    )
}

fn sample_clock(
    trusted_now: impl FnOnce() -> Result<TimestampMillis, OperationsError>,
) -> Result<TimestampMillis, OperationsError> {
    trusted_now()
}

fn validate_requesting_leader_authority(
    state: &str,
    authority: Option<(Uuid, i64)>,
    actor_participant_id: Uuid,
    expected_revision: i64,
) -> Result<(), OperationsError> {
    if !matches!(state, "ACTIVE" | "STABILIZING" | "ELECTING") {
        return Err(OperationsError::Conflict(
            "requesting Team Leader is not active for new requests",
        ));
    }
    let Some((leader_participant_id, revision)) = authority else {
        return Err(OperationsError::Conflict(
            "requesting Team has no confirmed current Leader",
        ));
    };
    if leader_participant_id != actor_participant_id || revision != expected_revision {
        return Err(OperationsError::Conflict(
            "requesting Team leadership changed",
        ));
    }
    Ok(())
}

fn ensure_joined_requesting_member(
    transaction: &Transaction<'_>,
    request: &TemporaryDispatchAdmissionRequest,
    actor: &TemporaryDispatchActorProvenance,
) -> Result<(), OperationsError> {
    let joined: bool = transaction.query_row(
        "SELECT EXISTS(SELECT 1 FROM ops_team_members AS member JOIN ops_workspace_participation_mappings AS mapping ON mapping.workspace_id=member.workspace_id AND mapping.participation_id=member.participation_id WHERE member.workspace_id=?1 AND member.team_id=?2 AND member.state='JOINED' AND mapping.participant_id=?3)",
        params![
            request.workspace_id.into_uuid().as_bytes(),
            request.requesting_team_id.as_bytes(),
            actor.participant_id.as_bytes(),
        ],
        |row| row.get(0),
    )?;
    if joined {
        Ok(())
    } else {
        Err(OperationsError::Conflict(
            "requesting Team Leader is not a current Team member",
        ))
    }
}

fn resolve_target_home_team(
    transaction: &Transaction<'_>,
    workspace_id: WorkspaceId,
    requesting_team_id: Uuid,
    target_participant_id: Uuid,
) -> Result<Uuid, OperationsError> {
    let target_identity = {
        let mut statement = transaction.prepare(
            "SELECT mapping.participation_id,mapping.agent_profile_id,participation.participation_id FROM ops_workspace_participation_mappings AS mapping LEFT JOIN ops_workspace_participations AS participation ON participation.workspace_id=mapping.workspace_id AND participation.agent_profile_id=mapping.agent_profile_id WHERE mapping.workspace_id=?1 AND mapping.participant_id=?2 LIMIT 2",
        )?;
        let mut rows = statement.query(params![
            workspace_id.into_uuid().as_bytes(),
            target_participant_id.as_bytes(),
        ])?;
        let identity = rows
            .next()?
            .map(|row| {
                Ok::<_, rusqlite::Error>((
                    row.get::<_, Vec<u8>>(0)?,
                    row.get::<_, Option<Vec<u8>>>(1)?,
                    row.get::<_, Option<Vec<u8>>>(2)?,
                ))
            })
            .transpose()?
            .ok_or(OperationsError::Conflict(
                "temporary dispatch target has no participation identity",
            ))?;
        if rows.next()?.is_some() {
            return Err(OperationsError::Conflict(
                "temporary dispatch target has ambiguous participation identity",
            ));
        }
        identity
    };
    let mapped_participation_id =
        WorkspaceParticipationId::from_uuid(Uuid::from_bytes(wire::id(target_identity.0)?))?;
    let profile_id = target_identity
        .1
        .map(|value| wire::id(value).map(Uuid::from_bytes))
        .transpose()?;
    let participation_id = match (profile_id.as_ref(), target_identity.2) {
        (Some(_), Some(canonical)) => {
            let canonical =
                WorkspaceParticipationId::from_uuid(Uuid::from_bytes(wire::id(canonical)?))?;
            if canonical != mapped_participation_id {
                return Err(OperationsError::Corrupt(
                    "temporary dispatch target Profile participation",
                ));
            }
            canonical
        }
        (Some(_), None) => {
            return Err(OperationsError::Corrupt(
                "temporary dispatch target Profile participation missing",
            ));
        }
        (None, _) => mapped_participation_id,
    };
    let profile_bytes = profile_id.map(|value| value.into_bytes().to_vec());

    let mut statement = transaction.prepare(
        "SELECT DISTINCT member.team_id FROM ops_team_members AS member LEFT JOIN ops_workspace_participations AS participation ON participation.workspace_id=member.workspace_id AND participation.participation_id=member.participation_id WHERE member.workspace_id=?1 AND member.state='JOINED' AND (member.participation_id=?2 OR (?3 IS NOT NULL AND participation.agent_profile_id=?3)) ORDER BY member.team_id LIMIT 2",
    )?;
    let mut rows = statement.query(params![
        workspace_id.into_uuid().as_bytes(),
        participation_id.into_uuid().as_bytes(),
        profile_bytes,
    ])?;
    let first = rows
        .next()?
        .map(|row| row.get::<_, Vec<u8>>(0))
        .transpose()?
        .ok_or(OperationsError::Conflict(
            "temporary dispatch target has no current home Team",
        ))?;
    if rows.next()?.is_some() {
        return Err(OperationsError::Conflict(
            "temporary dispatch target has ambiguous home Team",
        ));
    }
    let home_team_id = Uuid::from_bytes(wire::id(first)?);
    if home_team_id == requesting_team_id {
        return Err(OperationsError::Conflict(
            "temporary dispatch target is already in the requesting Team",
        ));
    }
    let target_is_leader: bool = transaction.query_row(
        "SELECT EXISTS(SELECT 1 FROM ops_leadership AS leadership LEFT JOIN ops_workspace_participation_mappings AS mapping ON mapping.workspace_id=leadership.workspace_id AND mapping.participant_id=leadership.leader_participant_id WHERE leadership.workspace_id=?1 AND (leadership.leader_participant_id=?2 OR mapping.participation_id=?3 OR (?4 IS NOT NULL AND mapping.agent_profile_id=?4)))",
        params![
            workspace_id.into_uuid().as_bytes(),
            target_participant_id.as_bytes(),
            participation_id.into_uuid().as_bytes(),
            profile_id.map(|value| value.into_bytes().to_vec()),
        ],
        |row| row.get(0),
    )?;
    if target_is_leader {
        return Err(OperationsError::Conflict(
            "temporary dispatch target is not a general Agent",
        ));
    }
    Ok(home_team_id)
}

fn load_request(
    connection: &Connection,
    request_id: Uuid,
) -> Result<Option<TemporaryDispatchRequestRecord>, OperationsError> {
    connection
        .query_row(
            "SELECT workspace_id,requesting_team_id,leadership_revision,actor_activity_id,actor_agent_id,actor_session_id,actor_participation_id,actor_profile_id,actor_participant_id,actor_display_name,target_participant_id,target_home_team_id,requested_scope,requested_end_at_ms,lock_created_until_ms,admitted_at_ms,admission_sequence FROM ops_temporary_dispatch_requests WHERE request_id=?1 LIMIT 1",
            params![request_id.as_bytes()],
            |row| decode_request_record(row, request_id),
        )
        .optional()
        .map_err(OperationsError::from)
}

fn decode_request_record(
    row: &rusqlite::Row<'_>,
    request_id: Uuid,
) -> rusqlite::Result<TemporaryDispatchRequestRecord> {
    let decode_error = |error| {
        rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Blob, Box::new(error))
    };
    let workspace_id = WorkspaceId::from_uuid(Uuid::from_bytes(
        wire::id(row.get(0)?).map_err(decode_error)?,
    ));
    let requesting_team_id = Uuid::from_bytes(wire::id(row.get(1)?).map_err(decode_error)?);
    let leadership_revision: i64 = row.get(2)?;
    let leadership_revision = u64::try_from(leadership_revision)
        .ok()
        .filter(|revision| *revision > 0)
        .ok_or_else(|| {
            decode_error(OperationsError::Corrupt(
                "temporary dispatch leadership revision",
            ))
        })?;
    let actor_activity_id = ActivityId::from_uuid(Uuid::from_bytes(
        wire::id(row.get(3)?).map_err(decode_error)?,
    ));
    let actor_agent_id = AgentInstanceId::from_uuid(Uuid::from_bytes(
        wire::id(row.get(4)?).map_err(decode_error)?,
    ));
    let actor_session_id = AgentSessionId::from_uuid(Uuid::from_bytes(
        wire::id(row.get(5)?).map_err(decode_error)?,
    ));
    let actor_participation_id = WorkspaceParticipationId::from_uuid(Uuid::from_bytes(
        wire::id(row.get(6)?).map_err(decode_error)?,
    ))
    .map_err(decode_error)?;
    let actor_profile_id = Uuid::from_bytes(wire::id(row.get(7)?).map_err(decode_error)?);
    let actor_participant_id = Uuid::from_bytes(wire::id(row.get(8)?).map_err(decode_error)?);
    let actor_display_name: String = row.get(9)?;
    if actor_display_name.is_empty() || actor_display_name.len() > 128 {
        return Err(decode_error(OperationsError::Corrupt(
            "temporary dispatch actor display name",
        )));
    }
    let target_participant_id = Uuid::from_bytes(wire::id(row.get(10)?).map_err(decode_error)?);
    let target_home_team_id = Uuid::from_bytes(wire::id(row.get(11)?).map_err(decode_error)?);
    let requested_scope =
        TemporaryDispatchRequestedScope::new(row.get(12)?).map_err(decode_error)?;
    let requested_end_at =
        decode_timestamp(row.get(13)?, "temporary dispatch end time").map_err(decode_error)?;
    let lock_created_until: Option<i64> = row.get(14)?;
    let admitted_at: i64 = row.get(15)?;
    let admission_sequence: i64 = row.get(16)?;
    if admission_sequence <= 0 {
        return Err(decode_error(OperationsError::Corrupt(
            "temporary dispatch admission sequence",
        )));
    }
    let admitted_at =
        decode_timestamp(admitted_at, "temporary dispatch admission time").map_err(decode_error)?;
    let lock_until = lock_created_until
        .map(|value| decode_timestamp(value, "temporary dispatch lock"))
        .transpose()
        .map_err(decode_error)?;
    Ok(TemporaryDispatchRequestRecord {
        request_id,
        workspace_id,
        requesting_team_id,
        leadership_revision,
        actor: TemporaryDispatchActorProvenance {
            activity_id: actor_activity_id,
            agent_id: actor_agent_id,
            session_id: actor_session_id,
            participation_id: actor_participation_id,
            profile_id: actor_profile_id,
            participant_id: actor_participant_id,
            display_name: actor_display_name,
        },
        target_home_team_id,
        target_participant_id,
        requested_scope,
        requested_end_at,
        admitted_at,
        lock_until,
        replayed: false,
    })
}

fn decode_timestamp(value: i64, label: &'static str) -> Result<TimestampMillis, OperationsError> {
    TimestampMillis::new(value).map_err(|_| OperationsError::Corrupt(label))
}

#[cfg(test)]
mod tests;
