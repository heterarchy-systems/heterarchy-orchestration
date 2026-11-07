//! SQLite read/write model for exact Home acceptance records.

use forge_domain::{
    ActivityId, AgentAssignmentId, AgentAssignmentOrigin, AgentAssignmentRevision,
    AgentAssignmentState, AgentSessionId, AgentSessionRevision, DagNodeId, DagRevisionNumber,
    ForgeBackendId, GoalId, GoalRevision, HomeHandoffAcceptanceGrant,
    HomeHandoffAcceptanceReceiptDigest, HomeHandoffAffinityAdmissionReferenceDigest,
    HomeHandoffHomeTeamAuthority, HomeHandoffNodeBindingDigest, HomeHandoffRecipientParticipation,
    HomeHandoffTakeRequest, HomeHandoffWorkAuthority, OrchestrationIssuerId,
    RegisteredAgentAssignment, TimestampMillis, WorkspaceId,
};
use forge_persistence::{
    HomeHandoffAcceptanceOutcomeKind, HomeHandoffAcceptanceOutcomeRecord, HomeHandoffProposalRecord,
};
use heterarchy_orchestration_sessions::{AgentProfileId, AgentProfileRevision};
use rusqlite::{Connection, OptionalExtension, Row, Transaction, params};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use super::model::{
    HomeAcceptanceAdmissionReceipt, HomeAcceptanceIntentReceipt,
    HomeAcceptanceProfilePinObservation, HomeAcceptanceResolutionReceipt,
    HomeAcceptanceSessionHoldObservation, HomeAcceptanceSourceIdentity,
    HomeAcceptanceWorkReservationReceipt, HomeAcceptanceWorkReservationState,
};
use crate::{NodeTeamBindingScope, OperationsError, WorkspaceParticipationId};

const DIGEST_BYTES: usize = 32;
const INTENT_DOMAIN: &[u8] = b"heterarchy.orchestration.home-acceptance.intent.v1\0";
const ADMISSION_DOMAIN: &[u8] = b"heterarchy.orchestration.home-acceptance.admission.v1\0";
const RESOLUTION_DOMAIN: &[u8] = b"heterarchy.orchestration.home-acceptance.resolution.v1\0";
const AFFINITY_DOMAIN: &[u8] = b"heterarchy.orchestration.home-acceptance.affinity.v1\0";
const NODE_BINDING_DOMAIN: &[u8] = b"heterarchy.orchestration.home-acceptance.node-binding.v1\0";

#[derive(Debug, Clone)]
pub(crate) struct IntentRow {
    pub(crate) identity: HomeAcceptanceSourceIdentity,
    pub(crate) request: HomeHandoffTakeRequest,
    pub(crate) proposal: Option<HomeHandoffProposalRecord>,
    pub(crate) proposal_snapshot: Option<Vec<u8>>,
    pub(crate) proposal_digest: Option<[u8; DIGEST_BYTES]>,
    pub(crate) predecessor_acceptance_id: Option<Uuid>,
    pub(crate) predecessor_source_digest: Option<[u8; DIGEST_BYTES]>,
    pub(crate) intent_digest: [u8; DIGEST_BYTES],
    pub(crate) recorded_at: TimestampMillis,
}

pub(crate) struct IntentInsert<'a> {
    pub(crate) identity: HomeAcceptanceSourceIdentity,
    pub(crate) request: HomeHandoffTakeRequest,
    pub(crate) proposal: Option<&'a HomeHandoffProposalRecord>,
    pub(crate) proposal_snapshot: Option<&'a [u8]>,
    pub(crate) predecessor_acceptance_id: Option<Uuid>,
    pub(crate) predecessor_source_digest: Option<[u8; DIGEST_BYTES]>,
    pub(crate) recorded_at: TimestampMillis,
}

impl IntentRow {
    pub(crate) fn receipt(&self, replayed: bool) -> HomeAcceptanceIntentReceipt {
        HomeAcceptanceIntentReceipt::from_parts(
            self.identity,
            self.request,
            self.proposal.as_ref(),
            self.predecessor_acceptance_id,
            self.predecessor_source_digest,
            self.intent_digest,
            self.recorded_at,
            replayed,
        )
    }
}

#[derive(Debug, Clone)]
pub(crate) struct AdmissionRow {
    pub(crate) receipt: HomeAcceptanceAdmissionReceipt,
    pub(crate) snapshot: Vec<u8>,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct AdmissionFacts {
    pub(crate) moving_profile_id: Uuid,
    pub(crate) recipient_profile_id: AgentProfileId,
    pub(crate) recipient_participant_id: Uuid,
    pub(crate) recipient_session_id: AgentSessionId,
    pub(crate) recipient_participation_id: WorkspaceParticipationId,
    pub(crate) recipient_participation_revision: u64,
    pub(crate) recipient_membership_revision: u64,
    pub(crate) session_hold: HomeAcceptanceSessionHoldObservation,
    pub(crate) profile_pin: HomeAcceptanceProfilePinObservation,
    pub(crate) node_scope: NodeTeamBindingScope,
    pub(crate) root_assignment_id: AgentAssignmentId,
    pub(crate) successor_assignment_id: AgentAssignmentId,
    pub(crate) authority: HomeHandoffWorkAuthority,
    pub(crate) affinity_digest: [u8; DIGEST_BYTES],
    pub(crate) node_binding_revision: Option<u64>,
    pub(crate) node_binding_digest: [u8; DIGEST_BYTES],
    pub(crate) team_membership_revision: Option<u64>,
    pub(crate) issued_at: TimestampMillis,
    pub(crate) expires_at: TimestampMillis,
    pub(crate) admitted_at: TimestampMillis,
}

#[derive(Debug, Clone)]
pub(crate) struct ResolutionRow {
    pub(crate) receipt: HomeAcceptanceResolutionReceipt,
    pub(crate) source: HomeHandoffAcceptanceOutcomeRecord,
    pub(crate) source_snapshot: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TerminalAssignmentObservation {
    pub(crate) state: AgentAssignmentState,
    pub(crate) revision: AgentAssignmentRevision,
    pub(crate) cancellation_requested: bool,
    pub(crate) digest: [u8; DIGEST_BYTES],
    pub(crate) updated_at: TimestampMillis,
}

pub(crate) fn intent_digest(
    identity: HomeAcceptanceSourceIdentity,
    request_snapshot: &[u8],
    proposal_snapshot: Option<&[u8]>,
    predecessor_acceptance_id: Option<Uuid>,
    predecessor_source_digest: Option<[u8; DIGEST_BYTES]>,
) -> [u8; DIGEST_BYTES] {
    let mut digest = Sha256::new();
    digest.update(INTENT_DOMAIN);
    digest.update(identity.issuer().into_uuid().as_bytes());
    digest.update(identity.backend().into_uuid().as_bytes());
    digest.update(identity.destination().as_bytes());
    frame(&mut digest, request_snapshot);
    match (
        proposal_snapshot,
        predecessor_acceptance_id,
        predecessor_source_digest,
    ) {
        (Some(proposal), None, None) => {
            digest.update([1]);
            frame(&mut digest, proposal);
        }
        (Some(proposal), Some(acceptance), Some(source_digest)) => {
            digest.update([2]);
            frame(&mut digest, proposal);
            digest.update(acceptance.as_bytes());
            digest.update(source_digest);
        }
        (None, None, None) => digest.update([0]),
        _ => digest.update([0xFF]),
    }
    digest.finalize().into()
}

pub(crate) fn load_intent(
    connection: &Connection,
    acceptance_id: Uuid,
) -> Result<Option<IntentRow>, OperationsError> {
    let raw = connection
        .query_row(
            "SELECT workspace_id,issuer_id,source_backend_id,source_destination_id,
                    predecessor_acceptance_id,predecessor_source_digest,request_digest,
                    request_snapshot,proposal_snapshot,proposal_digest,intent_digest,recorded_at_ms
             FROM ops_home_acceptance_intents WHERE acceptance_id=?1",
            [acceptance_id.as_bytes()],
            RawIntent::read,
        )
        .optional()?;
    raw.map(|raw| decode_intent(acceptance_id, raw)).transpose()
}

pub(crate) fn insert_intent(
    transaction: &Transaction<'_>,
    insert: &IntentInsert<'_>,
) -> Result<IntentRow, OperationsError> {
    if insert.proposal.is_some() != insert.proposal_snapshot.is_some()
        || insert.predecessor_acceptance_id.is_some() != insert.predecessor_source_digest.is_some()
        || (insert.proposal.is_none() && insert.predecessor_acceptance_id.is_some())
    {
        return Err(OperationsError::Conflict("Home acceptance intent shape"));
    }
    let request_snapshot = insert.request.canonical_snapshot();
    let proposal_digest = insert.proposal.map(|value| *value.payload_digest());
    let digest = intent_digest(
        insert.identity,
        &request_snapshot,
        insert.proposal_snapshot,
        insert.predecessor_acceptance_id,
        insert.predecessor_source_digest,
    );
    transaction.execute(
        "INSERT INTO ops_home_acceptance_intents(
            acceptance_id,workspace_id,issuer_id,source_backend_id,source_destination_id,
            predecessor_acceptance_id,predecessor_source_digest,request_digest,request_snapshot,
            proposal_snapshot,proposal_digest,intent_digest,recorded_at_ms
         ) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13)",
        params![
            insert.request.acceptance_id().as_bytes(),
            insert.request.scope().workspace_id().into_uuid().as_bytes(),
            insert.identity.issuer().into_uuid().as_bytes(),
            insert.identity.backend().into_uuid().as_bytes(),
            insert.identity.destination().as_bytes(),
            insert
                .predecessor_acceptance_id
                .map(|value| value.into_bytes().to_vec()),
            insert.predecessor_source_digest.map(|value| value.to_vec()),
            insert.request.request_digest().as_bytes().as_slice(),
            request_snapshot,
            insert.proposal_snapshot,
            proposal_digest.map(|value| value.to_vec()),
            digest.as_slice(),
            insert.recorded_at.get(),
        ],
    )?;
    Ok(IntentRow {
        identity: insert.identity,
        request: insert.request,
        proposal: insert.proposal.copied(),
        proposal_snapshot: insert.proposal_snapshot.map(ToOwned::to_owned),
        proposal_digest,
        predecessor_acceptance_id: insert.predecessor_acceptance_id,
        predecessor_source_digest: insert.predecessor_source_digest,
        intent_digest: digest,
        recorded_at: insert.recorded_at,
    })
}

pub(crate) fn load_admission(
    connection: &Connection,
    intent: &IntentRow,
    replayed: bool,
) -> Result<Option<AdmissionRow>, OperationsError> {
    let raw = connection
        .query_row(
            "SELECT workspace_id,request_digest,issuer_id,receipt_id,receipt_digest,
                    admission_snapshot,moving_profile_id,recipient_profile_id,
                    recipient_participant_id,recipient_session_id,recipient_participation_id,
                    recipient_participation_revision,recipient_membership_revision,
                    session_revision,session_acquired_at_ms,
                    profile_revision,profile_link_request_id,profile_linked_profile_revision,
                    profile_linked_session_revision,profile_acquired_at_ms,home_team_id,
                    team_membership_revision,node_binding_revision,node_binding_digest,
                    affinity_digest,activity_id,goal_id,goal_created_at_ms,dag_revision,
                    dag_base_goal_revision,node_id,root_assignment_id,successor_assignment_id,
                    authority_kind,grant_issued_at_ms,grant_expires_at_ms,admitted_at_ms
             FROM ops_home_acceptance_admissions WHERE acceptance_id=?1",
            [intent.request.acceptance_id().as_bytes()],
            RawAdmission::read,
        )
        .optional()?;
    raw.map(|raw| decode_admission(intent, raw, replayed))
        .transpose()
}

pub(crate) fn insert_admission(
    transaction: &Transaction<'_>,
    intent: &IntentRow,
    facts: &AdmissionFacts,
) -> Result<AdmissionRow, OperationsError> {
    if intent.proposal.is_none() {
        return Err(OperationsError::Corrupt(
            "admission without Source proposal",
        ));
    }
    let request = intent.request;
    let receipt_id = request.acceptance_id();
    let snapshot = encode_admission_snapshot(intent, facts);
    let receipt_digest = hash_snapshot(ADMISSION_DOMAIN, &snapshot);
    let grant = build_grant(intent.identity, request, receipt_id, receipt_digest, facts)?;
    let intent_receipt = intent.receipt(false);
    let row = AdmissionRow {
        receipt: HomeAcceptanceAdmissionReceipt::from_parts(
            &intent_receipt,
            receipt_id,
            receipt_digest,
            facts.moving_profile_id,
            facts.recipient_profile_id,
            facts.recipient_participant_id,
            request.recipient().agent_id(),
            facts.recipient_session_id,
            facts.recipient_participation_id,
            facts.recipient_participation_revision,
            facts.recipient_membership_revision,
            facts.node_binding_revision,
            facts.node_binding_digest,
            facts.session_hold,
            facts.profile_pin,
            facts.authority,
            &grant,
            facts.admitted_at,
            false,
        ),
        snapshot,
    };
    persist_admission_row(transaction, intent, facts, &row)?;
    Ok(row)
}

fn persist_admission_row(
    transaction: &Transaction<'_>,
    intent: &IntentRow,
    facts: &AdmissionFacts,
    row: &AdmissionRow,
) -> Result<(), OperationsError> {
    let team_id = authority_team(facts.authority);
    let (team_membership_revision, persisted_node_revision) = match facts.authority {
        HomeHandoffWorkAuthority::WorkspaceWork { .. } => (None, facts.node_binding_revision),
        HomeHandoffWorkAuthority::HomeTeam(value) => {
            let revision = Some(value.node_binding_revision());
            if facts.node_binding_digest != *value.node_binding_digest().as_bytes()
                || facts.node_binding_revision != revision
            {
                return Err(OperationsError::Corrupt(
                    "Home Team authority Node evidence differs from admission",
                ));
            }
            (Some(value.membership_revision()), revision)
        }
        HomeHandoffWorkAuthority::TemporaryDispatch(_) => {
            return Err(OperationsError::Conflict(
                "Home acceptance Temporary Dispatch authority is unavailable without exact active-work binding",
            ));
        }
    };
    transaction.execute(
        "INSERT INTO ops_home_acceptance_admissions(
            acceptance_id,workspace_id,request_digest,issuer_id,receipt_id,receipt_digest,
            admission_snapshot,moving_profile_id,recipient_profile_id,
            recipient_participant_id,recipient_session_id,recipient_participation_id,
            recipient_participation_revision,recipient_membership_revision,
            session_revision,session_acquired_at_ms,
            profile_revision,profile_link_request_id,profile_linked_profile_revision,
            profile_linked_session_revision,profile_acquired_at_ms,home_team_id,
            team_membership_revision,node_binding_revision,node_binding_digest,affinity_digest,
            activity_id,goal_id,goal_created_at_ms,dag_revision,dag_base_goal_revision,node_id,
            root_assignment_id,successor_assignment_id,authority_kind,grant_issued_at_ms,
            grant_expires_at_ms,admitted_at_ms
         ) VALUES(
            ?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,
            ?21,?22,?23,?24,?25,?26,?27,?28,?29,?30,?31,?32,?33,?34,?35,?36,?37,?38
         )",
        params![
            intent.request.acceptance_id().as_bytes(),
            intent.request.scope().workspace_id().into_uuid().as_bytes(),
            intent.request.request_digest().as_bytes().as_slice(),
            intent.identity.issuer().into_uuid().as_bytes(),
            row.receipt.receipt_id().as_bytes(),
            row.receipt.receipt_digest().as_slice(),
            row.snapshot,
            facts.moving_profile_id.as_bytes(),
            facts.recipient_profile_id.into_uuid().as_bytes(),
            facts.recipient_participant_id.as_bytes(),
            facts.recipient_session_id.into_uuid().as_bytes(),
            facts.recipient_participation_id.into_uuid().as_bytes(),
            checked_revision(
                facts.recipient_participation_revision,
                "recipient participation"
            )?,
            checked_revision(facts.recipient_membership_revision, "recipient membership")?,
            facts.session_hold.session_revision().get(),
            facts.session_hold.acquired_at().get(),
            facts.profile_pin.profile_revision().get(),
            facts.profile_pin.logical_session_request_id().as_bytes(),
            facts.profile_pin.linked_profile_revision().get(),
            facts.profile_pin.linked_session_revision().get(),
            facts.profile_pin.acquired_at().get(),
            team_id.map(|value| value.into_bytes().to_vec()),
            team_membership_revision
                .map(|value| checked_revision(value, "team membership"))
                .transpose()?,
            persisted_node_revision
                .map(|value| checked_revision(value, "Node binding"))
                .transpose()?,
            facts.node_binding_digest.as_slice(),
            facts.affinity_digest.as_slice(),
            facts.node_scope.activity_id().into_uuid().as_bytes(),
            facts.node_scope.goal_id().into_uuid().as_bytes(),
            facts.node_scope.goal_created_at().get(),
            facts.node_scope.dag_revision().get(),
            facts.node_scope.dag_base_goal_revision().get(),
            facts.node_scope.node_id().into_uuid().as_bytes(),
            facts.root_assignment_id.into_uuid().as_bytes(),
            facts.successor_assignment_id.into_uuid().as_bytes(),
            authority_kind(facts.authority),
            facts.issued_at.get(),
            facts.expires_at.get(),
            facts.admitted_at.get(),
        ],
    )?;
    Ok(())
}

pub(crate) fn load_resolution(
    connection: &Connection,
    intent: &IntentRow,
    replayed: bool,
) -> Result<Option<ResolutionRow>, OperationsError> {
    let Some(mut raw) = connection
        .query_row(
            "SELECT admission_acceptance_id,request_digest,issuer_id,outcome,
                    source_payload_digest,source_snapshot,resolution_id,resolution_digest,
                    resolved_at_ms
             FROM ops_home_acceptance_resolutions WHERE acceptance_id=?1",
            [intent.request.acceptance_id().as_bytes()],
            RawResolution::read,
        )
        .optional()?
    else {
        return Ok(None);
    };
    let admission = load_admission(connection, intent, replayed)?;
    let stored_admission_id = raw
        .admission_acceptance_id
        .as_deref()
        .map(|value| decode_uuid(value, "Home resolution admission ID"))
        .transpose()?;
    if stored_admission_id != admission.as_ref().map(|value| value.receipt.receipt_id()) {
        return Err(OperationsError::Corrupt("Home resolution admission row"));
    }
    raw.admission_receipt_digest = admission
        .as_ref()
        .map(|value| value.receipt.receipt_digest().to_vec());
    decode_resolution(intent, raw, admission.as_ref(), replayed).map(Some)
}

pub(crate) fn insert_resolution(
    transaction: &Transaction<'_>,
    intent: &IntentRow,
    source: &HomeHandoffAcceptanceOutcomeRecord,
    admission: Option<&AdmissionRow>,
    resolved_at: TimestampMillis,
) -> Result<ResolutionRow, OperationsError> {
    let source_snapshot = source
        .to_snapshot_bytes()
        .map_err(|_| OperationsError::Conflict("Home Source terminal snapshot"))?;
    let source_digest = *source.payload_digest();
    let resolution_digest = resolution_digest(intent, &source_snapshot, admission);
    let acceptance_id = source.acceptance_id();
    transaction.execute(
        "INSERT INTO ops_home_acceptance_resolutions(
            acceptance_id,admission_acceptance_id,request_digest,issuer_id,outcome,
            source_payload_digest,source_snapshot,resolution_id,resolution_digest,resolved_at_ms
         ) VALUES(?1,?2,?3,?4,?5,?6,?7,?1,?8,?9)",
        params![
            acceptance_id.as_bytes(),
            admission.map(|_| acceptance_id.into_bytes().to_vec()),
            intent.request.request_digest().as_bytes().as_slice(),
            intent.identity.issuer().into_uuid().as_bytes(),
            encode_outcome(source.outcome_kind()),
            source_digest.as_slice(),
            source_snapshot,
            resolution_digest.as_slice(),
            resolved_at.get(),
        ],
    )?;
    load_resolution(transaction, intent, false)?.ok_or(OperationsError::Corrupt(
        "Home acceptance resolution disappeared after insert",
    ))
}

pub(crate) fn insert_work_reservation(
    transaction: &Transaction<'_>,
    admission: &HomeAcceptanceAdmissionReceipt,
    created_at: TimestampMillis,
) -> Result<HomeAcceptanceWorkReservationReceipt, OperationsError> {
    let intent = admission.intent();
    let proposal = intent
        .proposal()
        .ok_or(OperationsError::Corrupt("Home reservation has no proposal"))?;
    let acceptance_id = admission.acceptance_id();
    let workspace_id = intent.request().scope().workspace_id();
    let scope = scope_from_proposal(&proposal)?;
    let root_assignment_id = proposal.source().root_assignment_id();
    let successor_assignment_id = proposal.request().successor_id();
    let authority = admission.work_authority();
    if acceptance_id.is_nil() || scope.workspace_id() != workspace_id {
        return Err(OperationsError::Conflict("Home work reservation identity"));
    }
    if matches!(authority, HomeHandoffWorkAuthority::TemporaryDispatch(_)) {
        return Err(OperationsError::Conflict(
            "Home acceptance Temporary Dispatch authority is unavailable without exact active-work binding",
        ));
    }
    transaction.execute(
        "INSERT INTO ops_home_acceptance_work_reservations(
            acceptance_id,workspace_id,activity_id,goal_id,goal_created_at_ms,dag_revision,
            dag_base_goal_revision,node_id,root_assignment_id,successor_assignment_id,
            authority_kind,team_id,state,created_at_ms,updated_at_ms,terminal_source_digest
         ) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,0,?13,?13,NULL)",
        params![
            acceptance_id.as_bytes(),
            workspace_id.into_uuid().as_bytes(),
            scope.activity_id().into_uuid().as_bytes(),
            scope.goal_id().into_uuid().as_bytes(),
            scope.goal_created_at().get(),
            scope.dag_revision().get(),
            scope.dag_base_goal_revision().get(),
            scope.node_id().into_uuid().as_bytes(),
            root_assignment_id.into_uuid().as_bytes(),
            successor_assignment_id.into_uuid().as_bytes(),
            authority_kind(authority),
            authority_team(authority).map(|value| value.into_bytes().to_vec()),
            created_at.get(),
        ],
    )?;
    load_work_reservation(transaction, acceptance_id, false)?.ok_or(OperationsError::Corrupt(
        "Home work reservation disappeared after insert",
    ))
}

pub(crate) fn load_work_reservation(
    connection: &Connection,
    acceptance_id: Uuid,
    replayed: bool,
) -> Result<Option<HomeAcceptanceWorkReservationReceipt>, OperationsError> {
    let raw = connection
        .query_row(
            "SELECT workspace_id,activity_id,goal_id,goal_created_at_ms,dag_revision,
                    dag_base_goal_revision,node_id,root_assignment_id,successor_assignment_id,
                    authority_kind,team_id,state,created_at_ms,updated_at_ms,terminal_source_digest,
                    terminal_assignment_state,terminal_assignment_revision,
                    terminal_assignment_cancellation_requested,
                    terminal_assignment_digest,terminal_assignment_updated_at_ms
             FROM ops_home_acceptance_work_reservations WHERE acceptance_id=?1",
            [acceptance_id.as_bytes()],
            RawWorkReservation::read,
        )
        .optional()?;
    raw.map(|raw| decode_work_reservation(acceptance_id, &raw, replayed))
        .transpose()
}

/// Cross-check a persisted Home work reservation against the exact immutable
/// Source outcome and Operations admission loaded by the caller's single
/// Operations snapshot. This deliberately performs no nested catalog reads.
pub(crate) fn verify_work_reservation_source(
    intent: &IntentRow,
    admission: &HomeAcceptanceAdmissionReceipt,
    work: &HomeAcceptanceWorkReservationReceipt,
    source: &HomeHandoffAcceptanceOutcomeRecord,
) -> Result<(), OperationsError> {
    let proposal = intent
        .proposal
        .ok_or(OperationsError::Corrupt("Home work row has no proposal"))?;
    let source_pin = proposal.source();
    let expected_scope = scope_from_proposal(&proposal)?;
    let expected_team_id = authority_team(admission.work_authority());
    if admission.acceptance_id() != intent.request.acceptance_id()
        || source.acceptance_id() != intent.request.acceptance_id()
        || source.request() != intent.request
        || source.issuer() != intent.identity.issuer()
        || source.workspace_id() != work.workspace_id()
        || source.preparation_id() != proposal.request().preparation_id()
        || source.handoff_id() != proposal.request().handoff_id()
        || source.proposal_id() != proposal.request().proposal_id()
        || source.case_revision() != proposal.case_revision()
        || source.predecessor_id() != source_pin.assignment_id()
        || work.workspace_id() != intent.request.scope().workspace_id()
        || work.node_scope() != expected_scope
        || work.root_assignment_id() != source_pin.root_assignment_id()
        || work.root_assignment_id() != source.root_assignment_id()
        || work.successor_assignment_id() != proposal.request().successor_id()
        || work.team_id() != expected_team_id
    {
        return Err(OperationsError::Corrupt(
            "Home work reservation differs from exact intent/admission/Source identity",
        ));
    }
    match (work.state(), source.outcome_kind()) {
        (HomeAcceptanceWorkReservationState::Active, HomeHandoffAcceptanceOutcomeKind::Applied) => {
            verify_applied_work_source(intent, admission, work, source)?;
            ensure_work_has_no_terminal_evidence(work)
        }
        (
            HomeAcceptanceWorkReservationState::Terminal,
            HomeHandoffAcceptanceOutcomeKind::Applied,
        ) => {
            verify_applied_work_source(intent, admission, work, source)?;
            ensure_work_has_terminal_evidence(work, source)
        }
        (
            HomeAcceptanceWorkReservationState::Aborted,
            HomeHandoffAcceptanceOutcomeKind::Aborted,
        ) => ensure_aborted_work_evidence(work, source),
        _ => Err(OperationsError::Corrupt(
            "Home work reservation state differs from Source outcome",
        )),
    }
}

fn verify_applied_work_source(
    intent: &IntentRow,
    admission: &HomeAcceptanceAdmissionReceipt,
    work: &HomeAcceptanceWorkReservationReceipt,
    source: &HomeHandoffAcceptanceOutcomeRecord,
) -> Result<(), OperationsError> {
    let proposal = intent
        .proposal
        .ok_or(OperationsError::Corrupt("Applied work has no proposal"))?;
    let source_pin = proposal.source();
    let expected_scope = scope_from_proposal(&proposal)?;
    let applied = source
        .applied()
        .ok_or(OperationsError::Corrupt("Applied Home work Source payload"))?;
    let successor = applied.successor();
    if successor.assignment_id() != work.successor_assignment_id()
        || successor.origin().preparation_id() != proposal.request().preparation_id()
        || successor.origin().handoff_id() != proposal.request().handoff_id()
        || successor.origin().predecessor_id() != source_pin.assignment_id()
        || successor.origin().root_assignment_id() != work.root_assignment_id()
        || successor.workspace_id() != work.workspace_id()
        || successor.activity_id() != expected_scope.activity_id()
        || successor.goal_id() != expected_scope.goal_id()
        || successor.goal_created_at() != expected_scope.goal_created_at()
        || successor.goal_revision() != expected_scope.dag_base_goal_revision()
        || successor.dag_revision() != expected_scope.dag_revision()
        || successor.node_id() != expected_scope.node_id()
        || successor.node_revision().get() < source_pin.node_revision().get()
        || applied.grant() != admission.grant()
    {
        return Err(OperationsError::Corrupt(
            "Home work reservation differs from Applied successor",
        ));
    }
    Ok(())
}

fn ensure_work_has_no_terminal_evidence(
    work: &HomeAcceptanceWorkReservationReceipt,
) -> Result<(), OperationsError> {
    if work.terminal_source_digest().is_some()
        || work.terminal_assignment_state().is_some()
        || work.terminal_assignment_revision().is_some()
        || work.terminal_assignment_cancellation_requested().is_some()
        || work.terminal_assignment_digest().is_some()
        || work.terminal_assignment_updated_at().is_some()
    {
        return Err(OperationsError::Corrupt(
            "Active Home work reservation contains terminal evidence",
        ));
    }
    Ok(())
}

fn ensure_work_has_terminal_evidence(
    work: &HomeAcceptanceWorkReservationReceipt,
    source: &HomeHandoffAcceptanceOutcomeRecord,
) -> Result<(), OperationsError> {
    if work.terminal_source_digest() != Some(*source.payload_digest())
        || work.terminal_assignment_state().is_none()
        || work.terminal_assignment_revision().is_none()
        || work.terminal_assignment_cancellation_requested().is_none()
        || work.terminal_assignment_digest().is_none()
        || work.terminal_assignment_updated_at().is_none()
    {
        return Err(OperationsError::Corrupt(
            "Terminal Home work reservation lacks exact Source/assignment evidence",
        ));
    }
    Ok(())
}

fn ensure_aborted_work_evidence(
    work: &HomeAcceptanceWorkReservationReceipt,
    source: &HomeHandoffAcceptanceOutcomeRecord,
) -> Result<(), OperationsError> {
    if work.terminal_source_digest() != Some(*source.payload_digest())
        || work.terminal_assignment_state().is_some()
        || work.terminal_assignment_revision().is_some()
        || work.terminal_assignment_cancellation_requested().is_some()
        || work.terminal_assignment_digest().is_some()
        || work.terminal_assignment_updated_at().is_some()
    {
        return Err(OperationsError::Corrupt(
            "Aborted Home work reservation terminal evidence",
        ));
    }
    Ok(())
}

pub(crate) fn transition_work_reservation(
    transaction: &Transaction<'_>,
    acceptance_id: Uuid,
    from: HomeAcceptanceWorkReservationState,
    to: HomeAcceptanceWorkReservationState,
    source_digest: Option<[u8; DIGEST_BYTES]>,
    terminal_assignment: Option<TerminalAssignmentObservation>,
    updated_at: TimestampMillis,
) -> Result<HomeAcceptanceWorkReservationReceipt, OperationsError> {
    if !matches!(
        (from, to, source_digest, terminal_assignment),
        (
            HomeAcceptanceWorkReservationState::Pending,
            HomeAcceptanceWorkReservationState::Active,
            None,
            None
        ) | (
            HomeAcceptanceWorkReservationState::Pending,
            HomeAcceptanceWorkReservationState::Aborted,
            Some(_),
            None
        ) | (
            HomeAcceptanceWorkReservationState::Active,
            HomeAcceptanceWorkReservationState::Terminal,
            Some(_),
            Some(_)
        )
    ) {
        return Err(OperationsError::Conflict(
            "Home work reservation transition",
        ));
    }
    let terminal_assignment_state = match terminal_assignment {
        Some(value) => Some(
            encode_terminal_assignment_state(value.state)
                .ok_or(OperationsError::Conflict("Home assignment is not terminal"))?,
        ),
        None => None,
    };
    let changed = transaction.execute(
        "UPDATE ops_home_acceptance_work_reservations
         SET state=?1,updated_at_ms=?2,terminal_source_digest=?3,
             terminal_assignment_state=?4,terminal_assignment_revision=?5,
             terminal_assignment_cancellation_requested=?6,terminal_assignment_digest=?7,
             terminal_assignment_updated_at_ms=?8
         WHERE acceptance_id=?9 AND state=?10",
        params![
            to.encode(),
            updated_at.get(),
            source_digest.map(|value| value.to_vec()),
            terminal_assignment_state,
            terminal_assignment.map(|value| value.revision.get()),
            terminal_assignment.map(|value| i64::from(value.cancellation_requested)),
            terminal_assignment.map(|value| value.digest.to_vec()),
            terminal_assignment.map(|value| value.updated_at.get()),
            acceptance_id.as_bytes(),
            from.encode(),
        ],
    )?;
    if changed != 1 {
        return Err(OperationsError::Conflict(
            "Home work reservation changed before exact transition",
        ));
    }
    load_work_reservation(transaction, acceptance_id, false)?.ok_or(OperationsError::Corrupt(
        "Home work reservation disappeared after transition",
    ))
}

pub(crate) fn pending_acceptance_ids(
    connection: &Connection,
    workspace_id: WorkspaceId,
    after: Option<Uuid>,
    limit: u16,
) -> Result<(Vec<Uuid>, Option<Uuid>), OperationsError> {
    if limit == 0 || limit > 64 || after.is_some_and(|value| value.is_nil()) {
        return Err(OperationsError::Bounds("Home acceptance page"));
    }
    let limit = i64::from(limit) + 1;
    let mut statement = connection.prepare(
        "SELECT substr(i.acceptance_id,1,17) FROM ops_home_acceptance_intents i
         WHERE i.workspace_id=?1 AND (?2 IS NULL OR i.acceptance_id>?2)
           AND NOT EXISTS(
             SELECT 1 FROM ops_home_acceptance_resolutions r
             WHERE r.acceptance_id=i.acceptance_id
           )
         ORDER BY i.acceptance_id LIMIT ?3",
    )?;
    let mut rows = statement.query(params![
        workspace_id.into_uuid().as_bytes(),
        after.map(|value| value.into_bytes().to_vec()),
        limit,
    ])?;
    let mut ids = Vec::new();
    while let Some(row) = rows.next()? {
        let raw_id: Vec<u8> = row.get(0)?;
        ids.push(decode_uuid(&raw_id, "Home acceptance page ID")?);
    }
    drop(rows);
    drop(statement);
    let has_more = ids.len() > usize::try_from(limit - 1).unwrap_or(64);
    if has_more {
        ids.pop();
    }
    let next_after = has_more.then(|| ids.last().copied()).flatten();
    Ok((ids, next_after))
}

pub(crate) fn encode_admission_snapshot(intent: &IntentRow, facts: &AdmissionFacts) -> Vec<u8> {
    let request_snapshot = intent.request.canonical_snapshot();
    let proposal_digest = intent.proposal_digest.unwrap_or([0; DIGEST_BYTES]);
    let mut bytes = Vec::with_capacity(1024);
    bytes.extend_from_slice(b"OHA1");
    bytes.extend_from_slice(intent.request.acceptance_id().as_bytes());
    bytes.extend_from_slice(intent.identity.issuer().into_uuid().as_bytes());
    bytes.extend_from_slice(intent.identity.backend().into_uuid().as_bytes());
    bytes.extend_from_slice(intent.identity.destination().as_bytes());
    bytes.extend_from_slice(&intent.intent_digest);
    bytes.extend_from_slice(&proposal_digest);
    frame_bytes(&mut bytes, &request_snapshot);
    bytes.extend_from_slice(facts.moving_profile_id.as_bytes());
    bytes.extend_from_slice(facts.recipient_profile_id.into_uuid().as_bytes());
    bytes.extend_from_slice(facts.recipient_participant_id.as_bytes());
    bytes.extend_from_slice(facts.recipient_session_id.into_uuid().as_bytes());
    bytes.extend_from_slice(facts.recipient_participation_id.into_uuid().as_bytes());
    append_u64(&mut bytes, facts.recipient_participation_revision);
    append_u64(&mut bytes, facts.recipient_membership_revision);
    bytes.extend_from_slice(facts.session_hold.acceptance_id().as_bytes());
    bytes.extend_from_slice(&facts.session_hold.request_digest());
    bytes.extend_from_slice(facts.session_hold.session_id().into_uuid().as_bytes());
    // The canonical take snapshot already binds the Session principal.
    bytes.extend_from_slice(&facts.session_hold.session_revision().get().to_be_bytes());
    bytes.extend_from_slice(&facts.session_hold.acquired_at().get().to_be_bytes());
    bytes.extend_from_slice(facts.profile_pin.profile_id().into_uuid().as_bytes());
    bytes.extend_from_slice(&facts.profile_pin.profile_revision().get().to_be_bytes());
    bytes.extend_from_slice(facts.profile_pin.logical_session_request_id().as_bytes());
    bytes.extend_from_slice(
        &facts
            .profile_pin
            .linked_profile_revision()
            .get()
            .to_be_bytes(),
    );
    bytes.extend_from_slice(
        &facts
            .profile_pin
            .linked_session_revision()
            .get()
            .to_be_bytes(),
    );
    bytes.extend_from_slice(&facts.profile_pin.acquired_at().get().to_be_bytes());
    bytes.extend_from_slice(facts.root_assignment_id.into_uuid().as_bytes());
    bytes.extend_from_slice(facts.successor_assignment_id.into_uuid().as_bytes());
    append_scope(&mut bytes, facts.node_scope);
    bytes.extend_from_slice(&facts.affinity_digest);
    match facts.authority {
        HomeHandoffWorkAuthority::WorkspaceWork { .. } => bytes.push(0),
        HomeHandoffWorkAuthority::HomeTeam(value) => {
            bytes.push(1);
            bytes.extend_from_slice(value.team_id().as_bytes());
            append_u64(&mut bytes, value.membership_revision());
            append_u64(&mut bytes, value.node_binding_revision());
            bytes.extend_from_slice(value.node_binding_digest().as_bytes());
        }
        HomeHandoffWorkAuthority::TemporaryDispatch(value) => {
            bytes.push(2);
            bytes.extend_from_slice(value.dispatch_id().as_bytes());
            bytes.extend_from_slice(value.destination_team_id().as_bytes());
            append_u64(&mut bytes, value.reservation_authority_revision());
            append_u64(&mut bytes, value.node_binding_revision());
            bytes.extend_from_slice(value.node_binding_digest().as_bytes());
            bytes.extend_from_slice(&value.end_at().get().to_be_bytes());
        }
    }
    match facts.node_binding_revision {
        Some(value) => {
            bytes.push(1);
            append_u64(&mut bytes, value);
        }
        None => bytes.push(0),
    }
    bytes.extend_from_slice(&facts.node_binding_digest);
    match facts.team_membership_revision {
        Some(value) => {
            bytes.push(1);
            append_u64(&mut bytes, value);
        }
        None => bytes.push(0),
    }
    bytes.extend_from_slice(&facts.issued_at.get().to_be_bytes());
    bytes.extend_from_slice(&facts.expires_at.get().to_be_bytes());
    bytes.extend_from_slice(&facts.admitted_at.get().to_be_bytes());
    bytes
}

pub(crate) fn resolution_digest(
    intent: &IntentRow,
    source_snapshot: &[u8],
    admission: Option<&AdmissionRow>,
) -> [u8; DIGEST_BYTES] {
    let mut digest = Sha256::new();
    digest.update(RESOLUTION_DOMAIN);
    digest.update(intent.request.acceptance_id().as_bytes());
    digest.update(intent.intent_digest);
    frame(&mut digest, source_snapshot);
    match admission {
        Some(admission) => {
            digest.update([1]);
            digest.update(admission.receipt.receipt_id().as_bytes());
            digest.update(admission.receipt.receipt_digest());
        }
        None => digest.update([0]),
    }
    digest.finalize().into()
}

pub(crate) fn affinity_reference_digest(
    affinity: &crate::AssignmentAffinityReceipt,
    scope: NodeTeamBindingScope,
    node_binding_digest: Option<[u8; DIGEST_BYTES]>,
    team_membership_revision: Option<u64>,
) -> [u8; DIGEST_BYTES] {
    let mut digest = Sha256::new();
    digest.update(AFFINITY_DOMAIN);
    digest.update(affinity.workspace_id().into_uuid().as_bytes());
    digest.update(affinity.assignment_id().into_uuid().as_bytes());
    digest.update(affinity.activity_id().into_uuid().as_bytes());
    digest.update(affinity.target_agent_id().into_uuid().as_bytes());
    digest.update(affinity.target_session_id().into_uuid().as_bytes());
    digest.update(affinity.target_participation_id().into_uuid().as_bytes());
    digest.update(affinity.target_participant_id().as_bytes());
    digest.update(affinity.team_id().map_or([0; 16], Uuid::into_bytes));
    digest.update(
        affinity
            .team_membership_revision()
            .unwrap_or_default()
            .to_be_bytes(),
    );
    digest.update(
        affinity
            .binding_revision()
            .unwrap_or_default()
            .to_be_bytes(),
    );
    digest.update(affinity.assignment_payload_fingerprint());
    digest.update([match affinity.disposition() {
        crate::AssignmentAffinityDisposition::Pending => 0,
        crate::AssignmentAffinityDisposition::Active => 1,
        crate::AssignmentAffinityDisposition::Terminal => 2,
        crate::AssignmentAffinityDisposition::Quiesced => 3,
        crate::AssignmentAffinityDisposition::LegacyUnknown => 4,
        crate::AssignmentAffinityDisposition::LegacyQuiesced => 5,
    }]);
    if let Some(payload) = affinity.original_offer_payload() {
        frame(&mut digest, payload);
    } else {
        digest.update(0_u64.to_be_bytes());
    }
    append_scope_digest(&mut digest, scope);
    digest.update(node_binding_digest.unwrap_or([0; DIGEST_BYTES]));
    digest.update(team_membership_revision.unwrap_or_default().to_be_bytes());
    digest.finalize().into()
}

pub(crate) fn node_binding_digest(
    scope: NodeTeamBindingScope,
    binding: Option<&crate::NodeTeamBinding>,
) -> [u8; DIGEST_BYTES] {
    let mut digest = Sha256::new();
    digest.update(NODE_BINDING_DOMAIN);
    append_scope_digest(&mut digest, scope);
    if let Some(binding) = binding {
        digest.update([1]);
        digest.update(binding.team_id().map_or([0; 16], Uuid::into_bytes));
        digest.update(binding.bound_by().as_bytes());
        digest.update(binding.bound_at().get().to_be_bytes());
        digest.update(binding.revision().to_be_bytes());
    } else {
        digest.update([0]);
    }
    digest.finalize().into()
}

pub(crate) fn terminal_assignment_observation(
    acceptance_id: Uuid,
    source_payload_digest: [u8; DIGEST_BYTES],
    assignment: &RegisteredAgentAssignment,
) -> Result<TerminalAssignmentObservation, OperationsError> {
    match assignment.origin() {
        AgentAssignmentOrigin::HomeHandoff(_) => {}
        AgentAssignmentOrigin::CoordinatorOffer | AgentAssignmentOrigin::CutContinuation(_) => {
            return Err(OperationsError::Conflict("Home terminal assignment origin"));
        }
    }
    if !assignment.state().is_terminal() || acceptance_id.is_nil() {
        return Err(OperationsError::Conflict(
            "Home terminal assignment is not an exact terminal observation",
        ));
    }
    let cancellation_requested = assignment.cancellation_requested();
    if matches!(
        assignment.state(),
        AgentAssignmentState::Accepted | AgentAssignmentState::Rejected
    ) == cancellation_requested
    {
        return Err(OperationsError::Conflict(
            "Home terminal assignment cancellation flag differs from state",
        ));
    }
    Ok(TerminalAssignmentObservation {
        state: assignment.state(),
        revision: assignment.revision(),
        cancellation_requested,
        digest: terminal_assignment_digest(
            acceptance_id,
            source_payload_digest,
            assignment.id(),
            assignment.state(),
            assignment.revision(),
            cancellation_requested,
            assignment.updated_at(),
        ),
        updated_at: assignment.updated_at(),
    })
}

fn terminal_assignment_digest(
    acceptance_id: Uuid,
    source_payload_digest: [u8; DIGEST_BYTES],
    assignment_id: AgentAssignmentId,
    state: AgentAssignmentState,
    revision: AgentAssignmentRevision,
    cancellation_requested: bool,
    updated_at: TimestampMillis,
) -> [u8; DIGEST_BYTES] {
    let mut digest = Sha256::new();
    digest.update(b"heterarchy.orchestration.home-acceptance.terminal-assignment.v1\0");
    digest.update(acceptance_id.as_bytes());
    digest.update(source_payload_digest);
    digest.update(assignment_id.into_uuid().as_bytes());
    digest.update(assignment_state_code(state).to_be_bytes());
    digest.update(revision.get().to_be_bytes());
    digest.update([u8::from(cancellation_requested)]);
    digest.update(updated_at.get().to_be_bytes());
    digest.finalize().into()
}

fn assignment_state_code(state: AgentAssignmentState) -> i64 {
    match state {
        AgentAssignmentState::Offered => 0,
        AgentAssignmentState::Running => 1,
        AgentAssignmentState::Submitted => 2,
        AgentAssignmentState::Accepted => 3,
        AgentAssignmentState::Rejected => 4,
        AgentAssignmentState::Cancelled => 5,
        AgentAssignmentState::Interrupted => 6,
    }
}

fn build_grant(
    identity: HomeAcceptanceSourceIdentity,
    request: HomeHandoffTakeRequest,
    receipt_id: Uuid,
    receipt_digest: [u8; DIGEST_BYTES],
    facts: &AdmissionFacts,
) -> Result<HomeHandoffAcceptanceGrant, OperationsError> {
    let receipt_digest = HomeHandoffAcceptanceReceiptDigest::from_bytes(receipt_digest)
        .map_err(|_| OperationsError::Corrupt("Home acceptance receipt digest"))?;
    let participation = HomeHandoffRecipientParticipation::new(
        facts.recipient_participation_id.into_uuid(),
        facts.recipient_participation_revision,
    )
    .map_err(|_| OperationsError::Corrupt("Home recipient participation"))?;
    HomeHandoffAcceptanceGrant::new(
        identity.issuer(),
        request.request_digest(),
        receipt_id,
        receipt_digest,
        facts.moving_profile_id,
        facts.recipient_profile_id.into_uuid(),
        participation,
        facts.authority,
        facts.issued_at,
        facts.expires_at,
    )
    .map_err(|_| OperationsError::Conflict("Home acceptance grant window or identity"))
}

fn decode_intent(acceptance_id: Uuid, raw: RawIntent) -> Result<IntentRow, OperationsError> {
    let workspace_id =
        WorkspaceId::from_uuid(decode_uuid(&raw.workspace_id, "Home intent Workspace")?);
    let issuer =
        OrchestrationIssuerId::from_uuid(decode_uuid(&raw.issuer_id, "Home intent issuer")?);
    let backend = ForgeBackendId::from_uuid(decode_uuid(&raw.backend_id, "Home intent backend")?);
    let destination = decode_uuid(&raw.destination_id, "Home intent destination")?;
    let identity = HomeAcceptanceSourceIdentity::new(issuer, backend, destination)?;
    let request_digest = decode_digest(&raw.request_digest, "Home intent request digest")?;
    let request = HomeHandoffTakeRequest::from_canonical_snapshot(&raw.request_snapshot)
        .map_err(|_| OperationsError::Corrupt("Home intent request snapshot"))?;
    let proposal = raw
        .proposal_snapshot
        .as_deref()
        .map(HomeHandoffProposalRecord::from_snapshot_bytes)
        .transpose()
        .map_err(|_| OperationsError::Corrupt("Home intent proposal snapshot"))?;
    let proposal_digest = raw
        .proposal_digest
        .map(|value| decode_digest(&value, "Home intent proposal digest"))
        .transpose()?;
    let predecessor_acceptance_id = raw
        .predecessor_acceptance_id
        .map(|value| decode_uuid(&value, "Home predecessor acceptance"))
        .transpose()?;
    let predecessor_source_digest = raw
        .predecessor_source_digest
        .map(|value| decode_digest(&value, "Home predecessor Source digest"))
        .transpose()?;
    let stored_intent_digest = decode_digest(&raw.intent_digest, "Home intent digest")?;
    let recorded_at = decode_timestamp(raw.recorded_at_ms, "Home intent timestamp")?;
    if acceptance_id != request.acceptance_id()
        || workspace_id != request.scope().workspace_id()
        || request_digest != *request.request_digest().as_bytes()
        || request.scope().backend_id() != identity.backend()
        || request.scope().destination_id() != identity.destination()
        || proposal.is_some() != proposal_digest.is_some()
        || predecessor_acceptance_id.is_some() != predecessor_source_digest.is_some()
        || (proposal.is_none() && predecessor_acceptance_id.is_some())
        || proposal.is_some_and(|proposal| proposal_digest != Some(*proposal.payload_digest()))
        || stored_intent_digest
            != intent_digest(
                identity,
                &raw.request_snapshot,
                raw.proposal_snapshot.as_deref(),
                predecessor_acceptance_id,
                predecessor_source_digest,
            )
    {
        return Err(OperationsError::Corrupt("Home acceptance intent pins"));
    }
    Ok(IntentRow {
        identity,
        request,
        proposal,
        proposal_snapshot: raw.proposal_snapshot,
        proposal_digest,
        predecessor_acceptance_id,
        predecessor_source_digest,
        intent_digest: stored_intent_digest,
        recorded_at,
    })
}

fn decode_admission(
    intent: &IntentRow,
    raw: RawAdmission,
    replayed: bool,
) -> Result<AdmissionRow, OperationsError> {
    let facts = decode_admission_facts(intent, &raw)?;
    let request = intent.request;
    let workspace_id =
        WorkspaceId::from_uuid(decode_uuid(&raw.workspace_id, "Home admission Workspace")?);
    let request_digest = decode_digest(&raw.request_digest, "Home admission request digest")?;
    let issuer =
        OrchestrationIssuerId::from_uuid(decode_uuid(&raw.issuer_id, "Home admission issuer")?);
    let receipt_id = decode_uuid(&raw.receipt_id, "Home admission receipt ID")?;
    let receipt_digest = decode_digest(&raw.receipt_digest, "Home admission receipt digest")?;
    let proposal = intent.proposal.ok_or(OperationsError::Corrupt(
        "admission without staged proposal",
    ))?;

    if workspace_id != request.scope().workspace_id()
        || request_digest != *request.request_digest().as_bytes()
        || issuer != intent.identity.issuer()
        || receipt_id != request.acceptance_id()
        || facts.recipient_session_id != request.recipient().session_id()
        || facts.recipient_profile_id.into_uuid() == facts.moving_profile_id
        || facts.profile_pin.profile_id() != facts.recipient_profile_id
        || facts.session_hold.acceptance_id() != request.acceptance_id()
        || facts.session_hold.request_digest() != *request.request_digest().as_bytes()
        || facts.session_hold.session_id() != request.recipient().session_id()
        || facts.session_hold.principal() != request.recipient().principal()
        || facts.profile_pin.linked_session_revision() != facts.session_hold.session_revision()
        || facts.issued_at > facts.admitted_at
        || facts.admitted_at >= facts.expires_at
        || facts.node_scope != scope_from_proposal(&proposal)?
        || facts.root_assignment_id != proposal.source().root_assignment_id()
        || facts.successor_assignment_id != proposal.request().successor_id()
    {
        return Err(OperationsError::Corrupt("Home admission identity pins"));
    }

    let expected_snapshot = encode_admission_snapshot(intent, &facts);
    if raw.admission_snapshot != expected_snapshot
        || hash_snapshot(ADMISSION_DOMAIN, &raw.admission_snapshot) != receipt_digest
    {
        return Err(OperationsError::Corrupt("Home admission snapshot digest"));
    }
    let grant = build_grant(intent.identity, request, receipt_id, receipt_digest, &facts)?;
    if grant.issuer() != issuer
        || grant.recipient_profile_id() != facts.recipient_profile_id.into_uuid()
        || grant.moving_profile_id() != facts.moving_profile_id
        || grant.recipient_participation().id() != facts.recipient_participation_id.into_uuid()
        || grant.recipient_participation().revision() != facts.recipient_participation_revision
        || grant.work_authority() != facts.authority
    {
        return Err(OperationsError::Corrupt("Home admission grant projection"));
    }
    let intent_receipt = intent.receipt(replayed);
    let receipt = HomeAcceptanceAdmissionReceipt::from_parts(
        &intent_receipt,
        receipt_id,
        receipt_digest,
        facts.moving_profile_id,
        facts.recipient_profile_id,
        facts.recipient_participant_id,
        request.recipient().agent_id(),
        facts.recipient_session_id,
        facts.recipient_participation_id,
        facts.recipient_participation_revision,
        facts.recipient_membership_revision,
        facts.node_binding_revision,
        facts.node_binding_digest,
        facts.session_hold,
        facts.profile_pin,
        facts.authority,
        &grant,
        facts.admitted_at,
        replayed,
    );
    Ok(AdmissionRow {
        receipt,
        snapshot: raw.admission_snapshot,
    })
}

fn decode_admission_facts(
    intent: &IntentRow,
    raw: &RawAdmission,
) -> Result<AdmissionFacts, OperationsError> {
    let recipient_profile_id = AgentProfileId::from_uuid(decode_uuid(
        &raw.recipient_profile_id,
        "Home recipient Profile",
    )?)
    .map_err(|_| OperationsError::Corrupt("Home recipient Profile"))?;
    let recipient_participation_id = WorkspaceParticipationId::from_uuid(decode_uuid(
        &raw.recipient_participation_id,
        "Home recipient participation",
    )?)?;
    let session_revision = AgentSessionRevision::new(raw.session_revision)
        .map_err(|_| OperationsError::Corrupt("Home Session revision"))?;
    let session_hold = HomeAcceptanceSessionHoldObservation::from_parts(
        intent.request.acceptance_id(),
        *intent.request.request_digest().as_bytes(),
        AgentSessionId::from_uuid(decode_uuid(
            &raw.recipient_session_id,
            "Home recipient Session",
        )?),
        intent.request.recipient().principal(),
        session_revision,
        decode_timestamp(raw.session_acquired_at_ms, "Home Session acquisition time")?,
    );
    let profile_pin = decode_admission_profile_pin(recipient_profile_id, raw)?;
    let home_team_id = raw
        .home_team_id
        .as_ref()
        .map(|value| decode_uuid(value, "Home admission Team"))
        .transpose()?;
    let team_membership_revision = raw
        .team_membership_revision
        .map(|value| decode_positive_revision(value, "Home Team membership revision"))
        .transpose()?;
    let node_binding_revision = raw
        .node_binding_revision
        .map(|value| decode_positive_revision(value, "Home Node binding revision"))
        .transpose()?;
    let node_binding_digest = raw
        .node_binding_digest
        .as_ref()
        .map(|value| decode_digest(value, "Home Node binding digest"))
        .transpose()?;
    let affinity_digest = decode_digest(&raw.affinity_digest, "Home affinity digest")?;
    let authority = decode_authority(
        raw.authority_kind,
        home_team_id,
        team_membership_revision,
        node_binding_revision,
        node_binding_digest,
        affinity_digest,
    )?;
    let node_scope = decode_admission_node_scope(raw)?;
    Ok(AdmissionFacts {
        moving_profile_id: decode_uuid(&raw.moving_profile_id, "Home moving Profile")?,
        recipient_profile_id,
        recipient_participant_id: decode_uuid(
            &raw.recipient_participant_id,
            "Home recipient participant",
        )?,
        recipient_session_id: session_hold.session_id(),
        recipient_participation_id,
        recipient_participation_revision: decode_positive_revision(
            raw.recipient_participation_revision,
            "Home recipient participation revision",
        )?,
        recipient_membership_revision: decode_positive_revision(
            raw.recipient_membership_revision,
            "Home recipient membership revision",
        )?,
        session_hold,
        profile_pin,
        node_scope,
        root_assignment_id: AgentAssignmentId::from_uuid(decode_uuid(
            &raw.root_assignment_id,
            "Home root assignment",
        )?),
        successor_assignment_id: AgentAssignmentId::from_uuid(decode_uuid(
            &raw.successor_assignment_id,
            "Home successor assignment",
        )?),
        authority,
        affinity_digest,
        node_binding_revision,
        node_binding_digest: node_binding_digest
            .ok_or(OperationsError::Corrupt("Home current Node binding digest"))?,
        team_membership_revision,
        issued_at: decode_timestamp(raw.grant_issued_at_ms, "Home grant issue time")?,
        expires_at: decode_timestamp(raw.grant_expires_at_ms, "Home grant expiry")?,
        admitted_at: decode_timestamp(raw.admitted_at_ms, "Home admission time")?,
    })
}

fn decode_admission_node_scope(
    raw: &RawAdmission,
) -> Result<NodeTeamBindingScope, OperationsError> {
    let workspace_id =
        WorkspaceId::from_uuid(decode_uuid(&raw.workspace_id, "Home admission Workspace")?);
    NodeTeamBindingScope::new(
        workspace_id,
        ActivityId::from_uuid(decode_uuid(&raw.activity_id, "Home admission Activity")?),
        GoalId::from_uuid(decode_uuid(&raw.goal_id, "Home admission Goal")?),
        decode_timestamp(raw.goal_created_at_ms, "Home admission Goal creation")?,
        DagRevisionNumber::new(raw.dag_revision)
            .map_err(|_| OperationsError::Corrupt("Home admission DAG revision"))?,
        GoalRevision::new(raw.dag_base_goal_revision)
            .map_err(|_| OperationsError::Corrupt("Home admission Goal revision"))?,
        DagNodeId::from_uuid(decode_uuid(&raw.node_id, "Home admission Node")?),
    )
}

fn decode_admission_profile_pin(
    recipient_profile_id: AgentProfileId,
    raw: &RawAdmission,
) -> Result<HomeAcceptanceProfilePinObservation, OperationsError> {
    Ok(HomeAcceptanceProfilePinObservation::from_parts(
        recipient_profile_id,
        AgentProfileRevision::new(raw.profile_revision)
            .map_err(|_| OperationsError::Corrupt("Home Profile revision"))?,
        decode_uuid(&raw.profile_link_request_id, "Home Profile link request")?,
        AgentProfileRevision::new(raw.profile_linked_profile_revision)
            .map_err(|_| OperationsError::Corrupt("Home linked Profile revision"))?,
        AgentSessionRevision::new(raw.profile_linked_session_revision)
            .map_err(|_| OperationsError::Corrupt("Home linked Session revision"))?,
        decode_timestamp(raw.profile_acquired_at_ms, "Home Profile acquisition time")?,
    ))
}

fn decode_resolution(
    intent: &IntentRow,
    raw: RawResolution,
    admission: Option<&AdmissionRow>,
    replayed: bool,
) -> Result<ResolutionRow, OperationsError> {
    let request_digest = decode_digest(&raw.request_digest, "Home resolution request digest")?;
    let issuer =
        OrchestrationIssuerId::from_uuid(decode_uuid(&raw.issuer_id, "Home resolution issuer")?);
    let source_digest = decode_digest(&raw.source_payload_digest, "Home resolution Source digest")?;
    let source = HomeHandoffAcceptanceOutcomeRecord::from_snapshot_bytes(&raw.source_snapshot)
        .map_err(|_| OperationsError::Corrupt("Home resolution Source snapshot"))?;
    let resolution_id = decode_uuid(&raw.resolution_id, "Home resolution ID")?;
    let resolution_digest = decode_digest(&raw.resolution_digest, "Home resolution digest")?;
    let resolved_at = decode_timestamp(raw.resolved_at_ms, "Home resolution timestamp")?;
    if source.acceptance_id() != intent.request.acceptance_id()
        || source.request() != intent.request
        || source.issuer() != intent.identity.issuer()
        || source_digest != *source.payload_digest()
        || request_digest != *intent.request.request_digest().as_bytes()
        || issuer != intent.identity.issuer()
        || decode_outcome(raw.outcome)? != source.outcome_kind()
        || resolution_id != source.acceptance_id()
        || (source.outcome_kind() == HomeHandoffAcceptanceOutcomeKind::Applied
            && admission.is_none())
    {
        return Err(OperationsError::Corrupt("Home resolution identity"));
    }
    let expected_resolution_digest = resolution_digest_from_persisted(
        intent,
        &raw.source_snapshot,
        raw.admission_acceptance_id,
        raw.admission_receipt_digest,
    )?;
    if expected_resolution_digest != resolution_digest {
        return Err(OperationsError::Corrupt("Home resolution digest"));
    }
    let receipt = HomeAcceptanceResolutionReceipt::from_parts(
        intent.identity,
        intent.request,
        resolution_id,
        resolution_digest,
        source_digest,
        source.outcome_kind(),
        admission.map(|value| &value.receipt),
        resolved_at,
        replayed,
    );
    Ok(ResolutionRow {
        receipt,
        source,
        source_snapshot: raw.source_snapshot,
    })
}

fn decode_work_reservation(
    acceptance_id: Uuid,
    raw: &RawWorkReservation,
    replayed: bool,
) -> Result<HomeAcceptanceWorkReservationReceipt, OperationsError> {
    let workspace = WorkspaceId::from_uuid(decode_uuid(&raw.workspace_id, "Home work Workspace")?);
    let scope = NodeTeamBindingScope::new(
        workspace,
        ActivityId::from_uuid(decode_uuid(&raw.activity_id, "Home work Activity")?),
        GoalId::from_uuid(decode_uuid(&raw.goal_id, "Home work Goal")?),
        decode_timestamp(raw.goal_created_at_ms, "Home work Goal creation")?,
        DagRevisionNumber::new(raw.dag_revision)
            .map_err(|_| OperationsError::Corrupt("Home work DAG revision"))?,
        GoalRevision::new(raw.dag_base_goal_revision)
            .map_err(|_| OperationsError::Corrupt("Home work Goal revision"))?,
        DagNodeId::from_uuid(decode_uuid(&raw.node_id, "Home work Node")?),
    )?;
    let root_assignment_id = AgentAssignmentId::from_uuid(decode_uuid(
        &raw.root_assignment_id,
        "Home work root assignment",
    )?);
    let successor_assignment_id = AgentAssignmentId::from_uuid(decode_uuid(
        &raw.successor_assignment_id,
        "Home work successor assignment",
    )?);
    let authority_kind = decode_authority_kind(raw.authority_kind)?;
    let team_id = raw
        .team_id
        .as_deref()
        .map(|value| decode_uuid(value, "Home work Team"))
        .transpose()?;
    let state = HomeAcceptanceWorkReservationState::decode(raw.state)?;
    let created_at = decode_timestamp(raw.created_at_ms, "Home work creation")?;
    let updated_at = decode_timestamp(raw.updated_at_ms, "Home work update")?;
    let terminal_digest = raw
        .terminal_source_digest
        .as_deref()
        .map(|value| decode_digest(value, "Home work terminal Source digest"))
        .transpose()?;
    if updated_at < created_at
        || (authority_kind == 0 && team_id.is_some())
        || (authority_kind == 1 && team_id.is_none())
        || (matches!(
            state,
            HomeAcceptanceWorkReservationState::Pending
                | HomeAcceptanceWorkReservationState::Active
        ) && terminal_digest.is_some())
        || (matches!(
            state,
            HomeAcceptanceWorkReservationState::Aborted
                | HomeAcceptanceWorkReservationState::Terminal
        ) && terminal_digest.is_none())
    {
        return Err(OperationsError::Corrupt("Home work reservation pins"));
    }
    let terminal = decode_terminal_work_observation(
        raw,
        state,
        acceptance_id,
        successor_assignment_id,
        terminal_digest,
    )?;
    Ok(HomeAcceptanceWorkReservationReceipt::from_parts(
        acceptance_id,
        workspace,
        scope,
        root_assignment_id,
        successor_assignment_id,
        team_id,
        state,
        created_at,
        updated_at,
        terminal_digest,
        terminal.map(|value| value.state),
        terminal.map(|value| value.revision),
        terminal.map(|value| value.cancellation_requested),
        terminal.map(|value| value.digest),
        terminal.map(|value| value.updated_at),
        replayed,
    ))
}

fn decode_terminal_work_observation(
    raw: &RawWorkReservation,
    state: HomeAcceptanceWorkReservationState,
    acceptance_id: Uuid,
    successor_assignment_id: AgentAssignmentId,
    source_digest: Option<[u8; DIGEST_BYTES]>,
) -> Result<Option<TerminalAssignmentObservation>, OperationsError> {
    if state != HomeAcceptanceWorkReservationState::Terminal {
        if raw.terminal_assignment_state.is_some()
            || raw.terminal_assignment_revision.is_some()
            || raw.terminal_assignment_cancellation_requested.is_some()
            || raw.terminal_assignment_digest.is_some()
            || raw.terminal_assignment_updated_at_ms.is_some()
        {
            return Err(OperationsError::Corrupt("Home work reservation pins"));
        }
        return Ok(None);
    }
    let missing = || OperationsError::Corrupt("Home work reservation pins");
    let terminal_state =
        decode_terminal_assignment_state(raw.terminal_assignment_state.ok_or_else(missing)?)?;
    let revision =
        AgentAssignmentRevision::new(raw.terminal_assignment_revision.ok_or_else(missing)?)
            .map_err(|_| OperationsError::Corrupt("Home terminal assignment revision"))?;
    let cancellation_requested = match raw
        .terminal_assignment_cancellation_requested
        .ok_or_else(missing)?
    {
        0 => false,
        1 => true,
        _ => {
            return Err(OperationsError::Corrupt(
                "Home terminal assignment cancellation flag",
            ));
        }
    };
    let digest = decode_digest(
        raw.terminal_assignment_digest
            .as_deref()
            .ok_or_else(missing)?,
        "Home terminal assignment digest",
    )?;
    let updated_at = decode_timestamp(
        raw.terminal_assignment_updated_at_ms.ok_or_else(missing)?,
        "Home terminal assignment timestamp",
    )?;
    let source_digest = source_digest.ok_or_else(missing)?;
    let cancellation_matches = match terminal_state {
        AgentAssignmentState::Accepted | AgentAssignmentState::Rejected => !cancellation_requested,
        AgentAssignmentState::Cancelled | AgentAssignmentState::Interrupted => {
            cancellation_requested
        }
        AgentAssignmentState::Offered
        | AgentAssignmentState::Running
        | AgentAssignmentState::Submitted => false,
    };
    if !cancellation_matches
        || digest
            != terminal_assignment_digest(
                acceptance_id,
                source_digest,
                successor_assignment_id,
                terminal_state,
                revision,
                cancellation_requested,
                updated_at,
            )
    {
        return Err(OperationsError::Corrupt(
            "Home terminal assignment digest or state projection",
        ));
    }
    Ok(Some(TerminalAssignmentObservation {
        state: terminal_state,
        revision,
        cancellation_requested,
        digest,
        updated_at,
    }))
}

fn decode_authority(
    kind: i64,
    team_id: Option<Uuid>,
    team_membership_revision: Option<u64>,
    node_binding_revision: Option<u64>,
    node_binding_digest: Option<[u8; DIGEST_BYTES]>,
    affinity_digest: [u8; DIGEST_BYTES],
) -> Result<HomeHandoffWorkAuthority, OperationsError> {
    let affinity = HomeHandoffAffinityAdmissionReferenceDigest::from_bytes(affinity_digest)
        .map_err(|_| OperationsError::Corrupt("Home affinity reference digest"))?;
    match kind {
        0 => {
            if team_id.is_some() || team_membership_revision.is_some() {
                return Err(OperationsError::Corrupt("Workspace Home authority shape"));
            }
            let node_digest = node_binding_digest.ok_or(OperationsError::Corrupt(
                "Workspace Node observation digest",
            ))?;
            let _checked_node_digest = HomeHandoffNodeBindingDigest::from_bytes(node_digest)
                .map_err(|_| OperationsError::Corrupt("Workspace Node observation digest"))?;
            Ok(HomeHandoffWorkAuthority::WorkspaceWork {
                affinity_admission_reference_digest: affinity,
            })
        }
        1 => {
            let team_id = team_id.ok_or(OperationsError::Corrupt("Home Team authority ID"))?;
            let membership = team_membership_revision
                .ok_or(OperationsError::Corrupt("Home Team membership revision"))?;
            let binding_revision =
                node_binding_revision.ok_or(OperationsError::Corrupt("Home Team Node revision"))?;
            let binding_digest = HomeHandoffNodeBindingDigest::from_bytes(
                node_binding_digest.ok_or(OperationsError::Corrupt("Home Team Node digest"))?,
            )
            .map_err(|_| OperationsError::Corrupt("Home Team Node digest"))?;
            let authority = HomeHandoffHomeTeamAuthority::new(
                affinity,
                team_id,
                membership,
                binding_revision,
                binding_digest,
            )
            .map_err(|_| OperationsError::Corrupt("Home Team authority"))?;
            Ok(HomeHandoffWorkAuthority::HomeTeam(authority))
        }
        2 => Err(OperationsError::Conflict(
            "Home acceptance Temporary Dispatch authority is unavailable without exact active-work binding",
        )),
        _ => Err(OperationsError::Corrupt("Home work authority kind")),
    }
}

fn decode_authority_kind(value: i64) -> Result<i64, OperationsError> {
    if matches!(value, 0 | 1) {
        Ok(value)
    } else {
        Err(OperationsError::Corrupt("Home work authority kind"))
    }
}

fn authority_kind(value: HomeHandoffWorkAuthority) -> i64 {
    match value {
        HomeHandoffWorkAuthority::WorkspaceWork { .. } => 0,
        HomeHandoffWorkAuthority::HomeTeam(_) => 1,
        HomeHandoffWorkAuthority::TemporaryDispatch(_) => 2,
    }
}

fn authority_team(value: HomeHandoffWorkAuthority) -> Option<Uuid> {
    match value {
        HomeHandoffWorkAuthority::WorkspaceWork { .. } => None,
        HomeHandoffWorkAuthority::HomeTeam(value) => Some(value.team_id()),
        HomeHandoffWorkAuthority::TemporaryDispatch(value) => Some(value.destination_team_id()),
    }
}

pub(crate) fn scope_from_proposal(
    proposal: &HomeHandoffProposalRecord,
) -> Result<NodeTeamBindingScope, OperationsError> {
    let source = proposal.source();
    NodeTeamBindingScope::new(
        source.workspace_id(),
        source.activity_id(),
        source.goal_id(),
        source.goal_created_at(),
        source.dag_revision(),
        source.goal_revision(),
        source.node_id(),
    )
}

fn resolution_digest_from_persisted(
    intent: &IntentRow,
    source_snapshot: &[u8],
    admission_acceptance_id: Option<Vec<u8>>,
    admission_receipt_digest: Option<Vec<u8>>,
) -> Result<[u8; DIGEST_BYTES], OperationsError> {
    let mut digest = Sha256::new();
    digest.update(RESOLUTION_DOMAIN);
    digest.update(intent.request.acceptance_id().as_bytes());
    digest.update(intent.intent_digest);
    frame(&mut digest, source_snapshot);
    match (admission_acceptance_id, admission_receipt_digest) {
        (Some(id), Some(receipt_digest)) => {
            digest.update([1]);
            digest.update(decode_uuid(&id, "Home resolution admission ID")?.as_bytes());
            digest.update(decode_digest(
                &receipt_digest,
                "Home resolution admission digest",
            )?);
        }
        (None, None) => digest.update([0]),
        _ => return Err(OperationsError::Corrupt("Home resolution admission pins")),
    }
    Ok(digest.finalize().into())
}

fn hash_snapshot(domain: &[u8], snapshot: &[u8]) -> [u8; DIGEST_BYTES] {
    let mut digest = Sha256::new();
    digest.update(domain);
    frame(&mut digest, snapshot);
    digest.finalize().into()
}

fn frame(digest: &mut Sha256, bytes: &[u8]) {
    digest.update(u64::try_from(bytes.len()).unwrap_or(u64::MAX).to_be_bytes());
    digest.update(bytes);
}

fn frame_bytes(output: &mut Vec<u8>, bytes: &[u8]) {
    append_u64(output, u64::try_from(bytes.len()).unwrap_or(u64::MAX));
    output.extend_from_slice(bytes);
}

fn append_scope(output: &mut Vec<u8>, scope: NodeTeamBindingScope) {
    output.extend_from_slice(scope.workspace_id().into_uuid().as_bytes());
    output.extend_from_slice(scope.activity_id().into_uuid().as_bytes());
    output.extend_from_slice(scope.goal_id().into_uuid().as_bytes());
    output.extend_from_slice(&scope.goal_created_at().get().to_be_bytes());
    output.extend_from_slice(&scope.dag_revision().get().to_be_bytes());
    output.extend_from_slice(&scope.dag_base_goal_revision().get().to_be_bytes());
    output.extend_from_slice(scope.node_id().into_uuid().as_bytes());
}

fn append_scope_digest(digest: &mut Sha256, scope: NodeTeamBindingScope) {
    digest.update(scope.workspace_id().into_uuid().as_bytes());
    digest.update(scope.activity_id().into_uuid().as_bytes());
    digest.update(scope.goal_id().into_uuid().as_bytes());
    digest.update(scope.goal_created_at().get().to_be_bytes());
    digest.update(scope.dag_revision().get().to_be_bytes());
    digest.update(scope.dag_base_goal_revision().get().to_be_bytes());
    digest.update(scope.node_id().into_uuid().as_bytes());
}

fn append_u64(output: &mut Vec<u8>, value: u64) {
    output.extend_from_slice(&value.to_be_bytes());
}

fn checked_revision(value: u64, context: &'static str) -> Result<i64, OperationsError> {
    i64::try_from(value).map_err(|_| OperationsError::Bounds(context))
}

fn decode_positive_revision(value: i64, context: &'static str) -> Result<u64, OperationsError> {
    let value = u64::try_from(value).map_err(|_| OperationsError::Corrupt(context))?;
    if value == 0 {
        return Err(OperationsError::Corrupt(context));
    }
    Ok(value)
}

fn decode_timestamp(value: i64, context: &'static str) -> Result<TimestampMillis, OperationsError> {
    TimestampMillis::new(value).map_err(|_| OperationsError::Corrupt(context))
}

fn decode_uuid(bytes: &[u8], context: &'static str) -> Result<Uuid, OperationsError> {
    let value = Uuid::from_slice(bytes).map_err(|_| OperationsError::Corrupt(context))?;
    if value.is_nil() {
        return Err(OperationsError::Corrupt(context));
    }
    Ok(value)
}

fn decode_digest(
    bytes: &[u8],
    context: &'static str,
) -> Result<[u8; DIGEST_BYTES], OperationsError> {
    let value: [u8; DIGEST_BYTES] = bytes
        .try_into()
        .map_err(|_| OperationsError::Corrupt(context))?;
    if value == [0; DIGEST_BYTES] {
        return Err(OperationsError::Corrupt(context));
    }
    Ok(value)
}

fn decode_outcome(value: i64) -> Result<HomeHandoffAcceptanceOutcomeKind, OperationsError> {
    match value {
        1 => Ok(HomeHandoffAcceptanceOutcomeKind::Applied),
        2 => Ok(HomeHandoffAcceptanceOutcomeKind::Aborted),
        _ => Err(OperationsError::Corrupt("Home acceptance outcome")),
    }
}

fn decode_terminal_assignment_state(value: i64) -> Result<AgentAssignmentState, OperationsError> {
    match value {
        1 => Ok(AgentAssignmentState::Accepted),
        2 => Ok(AgentAssignmentState::Rejected),
        3 => Ok(AgentAssignmentState::Cancelled),
        4 => Ok(AgentAssignmentState::Interrupted),
        _ => Err(OperationsError::Corrupt("Home terminal assignment state")),
    }
}

fn encode_terminal_assignment_state(value: AgentAssignmentState) -> Option<i64> {
    match value {
        AgentAssignmentState::Accepted => Some(1),
        AgentAssignmentState::Rejected => Some(2),
        AgentAssignmentState::Cancelled => Some(3),
        AgentAssignmentState::Interrupted => Some(4),
        AgentAssignmentState::Offered
        | AgentAssignmentState::Running
        | AgentAssignmentState::Submitted => None,
    }
}

fn encode_outcome(value: HomeHandoffAcceptanceOutcomeKind) -> i64 {
    match value {
        HomeHandoffAcceptanceOutcomeKind::Applied => 1,
        HomeHandoffAcceptanceOutcomeKind::Aborted => 2,
    }
}

#[derive(Debug)]
struct RawIntent {
    workspace_id: Vec<u8>,
    issuer_id: Vec<u8>,
    backend_id: Vec<u8>,
    destination_id: Vec<u8>,
    predecessor_acceptance_id: Option<Vec<u8>>,
    predecessor_source_digest: Option<Vec<u8>>,
    request_digest: Vec<u8>,
    request_snapshot: Vec<u8>,
    proposal_snapshot: Option<Vec<u8>>,
    proposal_digest: Option<Vec<u8>>,
    intent_digest: Vec<u8>,
    recorded_at_ms: i64,
}

impl RawIntent {
    fn read(row: &Row<'_>) -> rusqlite::Result<Self> {
        Ok(Self {
            workspace_id: row.get(0)?,
            issuer_id: row.get(1)?,
            backend_id: row.get(2)?,
            destination_id: row.get(3)?,
            predecessor_acceptance_id: row.get(4)?,
            predecessor_source_digest: row.get(5)?,
            request_digest: row.get(6)?,
            request_snapshot: row.get(7)?,
            proposal_snapshot: row.get(8)?,
            proposal_digest: row.get(9)?,
            intent_digest: row.get(10)?,
            recorded_at_ms: row.get(11)?,
        })
    }
}

#[derive(Debug)]
struct RawAdmission {
    workspace_id: Vec<u8>,
    request_digest: Vec<u8>,
    issuer_id: Vec<u8>,
    receipt_id: Vec<u8>,
    receipt_digest: Vec<u8>,
    admission_snapshot: Vec<u8>,
    moving_profile_id: Vec<u8>,
    recipient_profile_id: Vec<u8>,
    recipient_participant_id: Vec<u8>,
    recipient_session_id: Vec<u8>,
    recipient_participation_id: Vec<u8>,
    recipient_participation_revision: i64,
    recipient_membership_revision: i64,
    session_revision: i64,
    session_acquired_at_ms: i64,
    profile_revision: i64,
    profile_link_request_id: Vec<u8>,
    profile_linked_profile_revision: i64,
    profile_linked_session_revision: i64,
    profile_acquired_at_ms: i64,
    home_team_id: Option<Vec<u8>>,
    team_membership_revision: Option<i64>,
    node_binding_revision: Option<i64>,
    node_binding_digest: Option<Vec<u8>>,
    affinity_digest: Vec<u8>,
    activity_id: Vec<u8>,
    goal_id: Vec<u8>,
    goal_created_at_ms: i64,
    dag_revision: i64,
    dag_base_goal_revision: i64,
    node_id: Vec<u8>,
    root_assignment_id: Vec<u8>,
    successor_assignment_id: Vec<u8>,
    authority_kind: i64,
    grant_issued_at_ms: i64,
    grant_expires_at_ms: i64,
    admitted_at_ms: i64,
}

impl RawAdmission {
    fn read(row: &Row<'_>) -> rusqlite::Result<Self> {
        Ok(Self {
            workspace_id: row.get(0)?,
            request_digest: row.get(1)?,
            issuer_id: row.get(2)?,
            receipt_id: row.get(3)?,
            receipt_digest: row.get(4)?,
            admission_snapshot: row.get(5)?,
            moving_profile_id: row.get(6)?,
            recipient_profile_id: row.get(7)?,
            recipient_participant_id: row.get(8)?,
            recipient_session_id: row.get(9)?,
            recipient_participation_id: row.get(10)?,
            recipient_participation_revision: row.get(11)?,
            recipient_membership_revision: row.get(12)?,
            session_revision: row.get(13)?,
            session_acquired_at_ms: row.get(14)?,
            profile_revision: row.get(15)?,
            profile_link_request_id: row.get(16)?,
            profile_linked_profile_revision: row.get(17)?,
            profile_linked_session_revision: row.get(18)?,
            profile_acquired_at_ms: row.get(19)?,
            home_team_id: row.get(20)?,
            team_membership_revision: row.get(21)?,
            node_binding_revision: row.get(22)?,
            node_binding_digest: row.get(23)?,
            affinity_digest: row.get(24)?,
            activity_id: row.get(25)?,
            goal_id: row.get(26)?,
            goal_created_at_ms: row.get(27)?,
            dag_revision: row.get(28)?,
            dag_base_goal_revision: row.get(29)?,
            node_id: row.get(30)?,
            root_assignment_id: row.get(31)?,
            successor_assignment_id: row.get(32)?,
            authority_kind: row.get(33)?,
            grant_issued_at_ms: row.get(34)?,
            grant_expires_at_ms: row.get(35)?,
            admitted_at_ms: row.get(36)?,
        })
    }
}

#[derive(Debug)]
struct RawResolution {
    admission_acceptance_id: Option<Vec<u8>>,
    request_digest: Vec<u8>,
    issuer_id: Vec<u8>,
    outcome: i64,
    source_payload_digest: Vec<u8>,
    source_snapshot: Vec<u8>,
    resolution_id: Vec<u8>,
    resolution_digest: Vec<u8>,
    resolved_at_ms: i64,
    admission_receipt_digest: Option<Vec<u8>>,
}

impl RawResolution {
    fn read(row: &Row<'_>) -> rusqlite::Result<Self> {
        Ok(Self {
            admission_acceptance_id: row.get(0)?,
            request_digest: row.get(1)?,
            issuer_id: row.get(2)?,
            outcome: row.get(3)?,
            source_payload_digest: row.get(4)?,
            source_snapshot: row.get(5)?,
            resolution_id: row.get(6)?,
            resolution_digest: row.get(7)?,
            resolved_at_ms: row.get(8)?,
            admission_receipt_digest: None,
        })
    }
}

#[derive(Debug)]
struct RawWorkReservation {
    workspace_id: Vec<u8>,
    activity_id: Vec<u8>,
    goal_id: Vec<u8>,
    goal_created_at_ms: i64,
    dag_revision: i64,
    dag_base_goal_revision: i64,
    node_id: Vec<u8>,
    root_assignment_id: Vec<u8>,
    successor_assignment_id: Vec<u8>,
    authority_kind: i64,
    team_id: Option<Vec<u8>>,
    state: i64,
    created_at_ms: i64,
    updated_at_ms: i64,
    terminal_source_digest: Option<Vec<u8>>,
    terminal_assignment_state: Option<i64>,
    terminal_assignment_revision: Option<i64>,
    terminal_assignment_cancellation_requested: Option<i64>,
    terminal_assignment_digest: Option<Vec<u8>>,
    terminal_assignment_updated_at_ms: Option<i64>,
}

impl RawWorkReservation {
    fn read(row: &Row<'_>) -> rusqlite::Result<Self> {
        Ok(Self {
            workspace_id: row.get(0)?,
            activity_id: row.get(1)?,
            goal_id: row.get(2)?,
            goal_created_at_ms: row.get(3)?,
            dag_revision: row.get(4)?,
            dag_base_goal_revision: row.get(5)?,
            node_id: row.get(6)?,
            root_assignment_id: row.get(7)?,
            successor_assignment_id: row.get(8)?,
            authority_kind: row.get(9)?,
            team_id: row.get(10)?,
            state: row.get(11)?,
            created_at_ms: row.get(12)?,
            updated_at_ms: row.get(13)?,
            terminal_source_digest: row.get(14)?,
            terminal_assignment_state: row.get(15)?,
            terminal_assignment_revision: row.get(16)?,
            terminal_assignment_cancellation_requested: row.get(17)?,
            terminal_assignment_digest: row.get(18)?,
            terminal_assignment_updated_at_ms: row.get(19)?,
        })
    }
}
