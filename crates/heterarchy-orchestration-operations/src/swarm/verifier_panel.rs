//! Lineage-separated adversarial verifier panels, immutable verdicts,
//! and diff-digest-bound convergence gating.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use forge_domain::WorkspaceId;
use rusqlite::{Connection, OptionalExtension, Row, Transaction, TransactionBehavior, params};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use super::board::{
    SwarmMemberRecord, SwarmRoleKind, SwarmRunRecord, SwarmWorkspaceMode, load_members_by_run,
    require_member_by_id, require_run_by_id,
};
use super::sql::{
    INSERT_SWARM_PANEL_SEAT_SQL, INSERT_SWARM_VERDICT_SQL, INSERT_SWARM_VERIFICATION_PANEL_SQL,
    SELECT_LATEST_SWARM_PANEL_BY_RUN_SQL, SELECT_SWARM_PANEL_BY_ID_SQL,
    SELECT_SWARM_PANEL_BY_REQUEST_ID_SQL, SELECT_SWARM_PANEL_SEATS_SQL,
    SELECT_SWARM_VERDICT_BY_REQUEST_ID_SQL, SELECT_SWARM_VERDICTS_BY_PANEL_SQL,
};
use super::{
    MAX_SWARM_MEMBERS_PER_RUN, MAX_SWARM_VERDICT_RATIONALE_BYTES, MAX_SWARM_VERIFIER_QUORUM,
    SwarmError, check_timestamp_non_negative, map_sqlite_conflict, validate_non_nil_uuid,
    validate_non_zero_digest, validate_workspace_id,
};
use crate::sqlite_codecs::{
    decode_bool_flag, decode_non_nil_uuid, decode_non_zero_digest, decode_positive_u32,
    decode_utf8, derive_deterministic_uuid, hash_optional_uuid, hash_str, len_u64,
};
use crate::{OperationsCatalog, OperationsError, ReviewVerdictOutcome};

const SWARM_PANEL_ID_DOMAIN: &[u8] = b"heterarchy.orchestration.swarm.panel_id.v1\0";
const SWARM_VERDICT_ID_DOMAIN: &[u8] = b"heterarchy.orchestration.swarm.verdict_id.v1\0";
const SWARM_PANEL_DIGEST_DOMAIN: &[u8] = b"heterarchy.orchestration.swarm.panel_create.v1\0";
const SWARM_VERDICT_DIGEST_DOMAIN: &[u8] = b"heterarchy.orchestration.swarm.verdict_record.v1\0";
const MAX_VERDICTS_PER_PANEL_READ: i64 = 1_024;

/// Seat classification inside a Swarm verification panel.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum SwarmPanelSeatKind {
    Author,
    Verifier,
}

impl SwarmPanelSeatKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Author => "AUTHOR",
            Self::Verifier => "VERIFIER",
        }
    }

    /// Parse a canonical uppercase seat-kind label.
    ///
    /// # Errors
    /// Rejects unknown seat-kind strings.
    pub fn parse(raw: &str) -> Result<Self, OperationsError> {
        match raw {
            "AUTHOR" => Ok(Self::Author),
            "VERIFIER" => Ok(Self::Verifier),
            _ => Err(OperationsError::Corrupt("swarm panel seat_kind")),
        }
    }
}

/// Member seat attached to a Swarm verification panel.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SwarmPanelSeatRecord {
    member_id: Uuid,
    seat_kind: SwarmPanelSeatKind,
    assigned_at_ms: i64,
}

impl SwarmPanelSeatRecord {
    #[must_use]
    pub const fn member_id(&self) -> Uuid {
        self.member_id
    }

    #[must_use]
    pub const fn seat_kind(&self) -> SwarmPanelSeatKind {
        self.seat_kind
    }

    #[must_use]
    pub const fn assigned_at_ms(&self) -> i64 {
        self.assigned_at_ms
    }
}

/// Durable Swarm verification panel specifying contributing authors, independent verifiers, and quorum.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SwarmVerificationPanelRecord {
    panel_id: Uuid,
    swarm_run_id: Uuid,
    workspace_id: WorkspaceId,
    required_quorum: u32,
    auto_created: bool,
    seats: Vec<SwarmPanelSeatRecord>,
    created_at_ms: i64,
    replayed: bool,
}

impl SwarmVerificationPanelRecord {
    #[must_use]
    pub const fn panel_id(&self) -> Uuid {
        self.panel_id
    }

    #[must_use]
    pub const fn swarm_run_id(&self) -> Uuid {
        self.swarm_run_id
    }

    #[must_use]
    pub const fn workspace_id(&self) -> WorkspaceId {
        self.workspace_id
    }

    #[must_use]
    pub const fn required_quorum(&self) -> u32 {
        self.required_quorum
    }

    #[must_use]
    pub const fn auto_created(&self) -> bool {
        self.auto_created
    }

    #[must_use]
    pub fn seats(&self) -> &[SwarmPanelSeatRecord] {
        &self.seats
    }

    #[must_use]
    pub fn author_member_ids(&self) -> Vec<Uuid> {
        self.seats
            .iter()
            .filter(|seat| seat.seat_kind == SwarmPanelSeatKind::Author)
            .map(|seat| seat.member_id)
            .collect()
    }

    #[must_use]
    pub fn verifier_member_ids(&self) -> Vec<Uuid> {
        self.seats
            .iter()
            .filter(|seat| seat.seat_kind == SwarmPanelSeatKind::Verifier)
            .map(|seat| seat.member_id)
            .collect()
    }

    fn has_verifier(&self, member_id: Uuid) -> bool {
        self.seats.iter().any(|seat| {
            seat.seat_kind == SwarmPanelSeatKind::Verifier && seat.member_id == member_id
        })
    }

    #[must_use]
    pub const fn created_at_ms(&self) -> i64 {
        self.created_at_ms
    }

    #[must_use]
    pub const fn replayed(&self) -> bool {
        self.replayed
    }
}

/// Immutable diff-digest-bound verdict recorded by a Swarm panel verifier.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SwarmVerdictRecord {
    verdict_id: Uuid,
    request_id: Uuid,
    panel_id: Uuid,
    swarm_run_id: Uuid,
    verifier_member_id: Uuid,
    outcome: ReviewVerdictOutcome,
    reviewed_diff_digest: [u8; 32],
    rationale: String,
    recorded_at_ms: i64,
    replayed: bool,
}

impl SwarmVerdictRecord {
    #[must_use]
    pub const fn verdict_id(&self) -> Uuid {
        self.verdict_id
    }

    #[must_use]
    pub const fn request_id(&self) -> Uuid {
        self.request_id
    }

    #[must_use]
    pub const fn panel_id(&self) -> Uuid {
        self.panel_id
    }

    #[must_use]
    pub const fn swarm_run_id(&self) -> Uuid {
        self.swarm_run_id
    }

    #[must_use]
    pub const fn verifier_member_id(&self) -> Uuid {
        self.verifier_member_id
    }

    #[must_use]
    pub const fn outcome(&self) -> ReviewVerdictOutcome {
        self.outcome
    }

    #[must_use]
    pub const fn reviewed_diff_digest(&self) -> [u8; 32] {
        self.reviewed_diff_digest
    }

    #[must_use]
    pub fn rationale(&self) -> &str {
        &self.rationale
    }

    #[must_use]
    pub const fn recorded_at_ms(&self) -> i64 {
        self.recorded_at_ms
    }

    #[must_use]
    pub const fn replayed(&self) -> bool {
        self.replayed
    }
}

/// Verified quorum proof returned by [`OperationsCatalog::verify_swarm_convergence_gate`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SwarmConvergenceGateOutcome {
    panel_id: Uuid,
    swarm_run_id: Uuid,
    converged_diff_digest: [u8; 32],
    required_quorum: u32,
    accepted_verdicts: Vec<SwarmVerdictRecord>,
}

impl SwarmConvergenceGateOutcome {
    #[must_use]
    pub const fn panel_id(&self) -> Uuid {
        self.panel_id
    }

    #[must_use]
    pub const fn swarm_run_id(&self) -> Uuid {
        self.swarm_run_id
    }

    #[must_use]
    pub const fn converged_diff_digest(&self) -> [u8; 32] {
        self.converged_diff_digest
    }

    #[must_use]
    pub const fn required_quorum(&self) -> u32 {
        self.required_quorum
    }

    #[must_use]
    pub fn accepted_count(&self) -> usize {
        self.accepted_verdicts.len()
    }

    #[must_use]
    pub fn accepted_verdicts(&self) -> &[SwarmVerdictRecord] {
        &self.accepted_verdicts
    }
}

/// Command to create a lineage-separated Swarm verification panel.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateSwarmVerificationPanelCommand {
    pub request_id: Uuid,
    pub workspace_id: WorkspaceId,
    pub swarm_run_id: Uuid,
    pub author_member_ids: Vec<Uuid>,
    pub verifier_member_ids: Vec<Uuid>,
    pub required_quorum: u32,
    pub created_at_ms: i64,
}

impl CreateSwarmVerificationPanelCommand {
    fn validate(&self) -> Result<(), OperationsError> {
        validate_non_nil_uuid(self.request_id, "swarm panel request_id")?;
        validate_workspace_id(self.workspace_id)?;
        validate_non_nil_uuid(self.swarm_run_id, "swarm_run_id")?;
        if self.author_member_ids.is_empty() {
            return Err(OperationsError::Bounds("swarm panel author_member_ids"));
        }
        if self.verifier_member_ids.is_empty() {
            return Err(OperationsError::Bounds("swarm panel verifier_member_ids"));
        }
        if !(1..=MAX_SWARM_VERIFIER_QUORUM).contains(&self.required_quorum)
            || self.verifier_member_ids.len() < usize::try_from(self.required_quorum).unwrap_or(1)
        {
            return Err(OperationsError::Bounds("swarm panel required_quorum"));
        }
        for &id in &self.author_member_ids {
            validate_non_nil_uuid(id, "swarm panel author_member_id")?;
        }
        for &id in &self.verifier_member_ids {
            validate_non_nil_uuid(id, "swarm panel verifier_member_id")?;
        }
        check_timestamp_non_negative(self.created_at_ms, "swarm panel created_at_ms")?;
        Ok(())
    }
}

/// Command to record an immutable diff-digest-bound verdict from a Swarm verifier.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordSwarmVerdictCommand {
    pub request_id: Uuid,
    pub workspace_id: WorkspaceId,
    pub swarm_run_id: Uuid,
    pub panel_id: Option<Uuid>,
    pub verifier_member_id: Uuid,
    pub outcome: ReviewVerdictOutcome,
    pub reviewed_diff_digest: [u8; 32],
    pub rationale: String,
    pub recorded_at_ms: i64,
}

impl RecordSwarmVerdictCommand {
    fn validate(&self) -> Result<(), OperationsError> {
        validate_non_nil_uuid(self.request_id, "swarm verdict request_id")?;
        validate_workspace_id(self.workspace_id)?;
        validate_non_nil_uuid(self.swarm_run_id, "swarm_run_id")?;
        if let Some(panel_id) = self.panel_id {
            validate_non_nil_uuid(panel_id, "swarm panel_id")?;
        }
        validate_non_nil_uuid(self.verifier_member_id, "swarm verifier_member_id")?;
        validate_non_zero_digest(self.reviewed_diff_digest, "swarm reviewed_diff_digest")?;
        if self.rationale.trim().is_empty()
            || self.rationale.len() > MAX_SWARM_VERDICT_RATIONALE_BYTES
            || self.rationale.contains('\0')
        {
            return Err(OperationsError::Bounds("swarm verdict rationale"));
        }
        check_timestamp_non_negative(self.recorded_at_ms, "swarm verdict recorded_at_ms")?;
        Ok(())
    }
}

impl OperationsCatalog {
    /// Create a Swarm verification panel while enforcing `Verifier` `Inherit` mode and strict
    /// 3-axis identity separation (`AgentProfileId`, `participant_id`, `AuthenticatedPrincipal`)
    /// against the entire author spawn lineage tree (author + ancestors + descendants) AND Coordinator.
    ///
    /// # Errors
    /// Fails closed with [`SwarmError::SybilLineageViolation`], [`SwarmError::VerifierMustInherit`],
    /// [`SwarmError::NotFound`], or [`OperationsError`].
    pub fn create_swarm_verification_panel(
        &mut self,
        command: &CreateSwarmVerificationPanelCommand,
    ) -> Result<SwarmVerificationPanelRecord, SwarmError> {
        command.validate()?;
        let mut authors = command.author_member_ids.clone();
        authors.sort_unstable();
        authors.dedup();
        let mut verifiers = command.verifier_member_ids.clone();
        verifiers.sort_unstable();
        verifiers.dedup();
        if verifiers.len() != command.verifier_member_ids.len() {
            return Err(
                OperationsError::Conflict("duplicate verifier_member_id in Swarm panel").into(),
            );
        }
        if verifiers.len() < usize::try_from(command.required_quorum).unwrap_or(1) {
            return Err(OperationsError::Bounds("swarm panel verifier count below quorum").into());
        }
        let digest = panel_create_digest(
            command.workspace_id,
            command.swarm_run_id,
            &authors,
            &verifiers,
            command.required_quorum,
        );
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(OperationsError::from)?;
        if let Some(replayed) = load_panel_replay(&tx, command.request_id, digest)? {
            tx.commit().map_err(OperationsError::from)?;
            return Ok(replayed);
        }
        let run = require_run_by_id(&tx, command.workspace_id, command.swarm_run_id)?;
        if run.state().is_terminal() {
            return Err(SwarmError::InvalidStateTransition(
                "cannot create verification panel on terminal Swarm run",
            ));
        }
        if command.created_at_ms < run.created_at_ms() {
            return Err(
                OperationsError::Bounds("swarm panel created_at_ms precedes run creation").into(),
            );
        }
        let all_members = load_members_by_run(&tx, command.swarm_run_id)?;
        enforce_lineage_separated_verifiers(&all_members, &authors, &verifiers)?;

        let panel_id =
            derive_deterministic_uuid(SWARM_PANEL_ID_DOMAIN, self.destination, command.request_id);
        insert_panel_and_seats(
            &tx,
            &PanelInsertParams {
                panel_id,
                request_id: command.request_id,
                digest,
                swarm_run_id: command.swarm_run_id,
                workspace_id: command.workspace_id,
                required_quorum: command.required_quorum,
                auto_created: false,
                created_at_ms: command.created_at_ms,
                author_ids: &authors,
                verifier_ids: &verifiers,
            },
            "swarm verification panel insert",
        )?;
        let panel = load_panel_by_id(&tx, command.swarm_run_id, panel_id)?
            .ok_or(OperationsError::Corrupt("inserted swarm panel missing"))?;
        tx.commit().map_err(OperationsError::from)?;
        Ok(panel)
    }

    /// Read the latest (or specified) Swarm verification panel for a run.
    ///
    /// # Errors
    /// Rejects invalid identities or corrupt stored rows.
    pub fn get_swarm_verification_panel(
        &self,
        workspace_id: WorkspaceId,
        swarm_run_id: Uuid,
        panel_id: Option<Uuid>,
    ) -> Result<Option<SwarmVerificationPanelRecord>, SwarmError> {
        validate_workspace_id(workspace_id)?;
        validate_non_nil_uuid(swarm_run_id, "swarm_run_id")?;
        if let Some(id) = panel_id {
            validate_non_nil_uuid(id, "swarm panel_id")?;
        }
        let tx = self
            .connection
            .unchecked_transaction()
            .map_err(OperationsError::from)?;
        let Some(run) = super::board::load_run_by_id(&tx, workspace_id, swarm_run_id)? else {
            crate::catalog::commit_read(tx, self.destination)?;
            return Ok(None);
        };
        let panel = match panel_id {
            Some(id) => load_panel_by_id(&tx, run.swarm_run_id(), id)?,
            None => load_latest_panel_by_run(&tx, run.swarm_run_id())?,
        };
        crate::catalog::commit_read(tx, self.destination)?;
        Ok(panel)
    }

    /// List immutable verdicts recorded for a verification panel.
    ///
    /// # Errors
    /// Rejects invalid identities, missing runs/panels, or corrupt rows.
    pub fn list_swarm_verdicts(
        &self,
        workspace_id: WorkspaceId,
        swarm_run_id: Uuid,
        panel_id: Uuid,
    ) -> Result<Vec<SwarmVerdictRecord>, SwarmError> {
        validate_workspace_id(workspace_id)?;
        validate_non_nil_uuid(swarm_run_id, "swarm_run_id")?;
        validate_non_nil_uuid(panel_id, "swarm panel_id")?;
        let tx = self
            .connection
            .unchecked_transaction()
            .map_err(OperationsError::from)?;
        let _run = require_run_by_id(&tx, workspace_id, swarm_run_id)?;
        let _panel = load_panel_by_id(&tx, swarm_run_id, panel_id)?
            .ok_or(SwarmError::NotFound("swarm verification panel"))?;
        let verdicts = load_verdicts_by_panel(&tx, panel_id)?;
        crate::catalog::commit_read(tx, self.destination)?;
        Ok(verdicts)
    }

    /// Record an immutable diff-digest-bound verdict from a lineage-separated `Verifier`.
    /// When `panel_id` is `None`, reuses the run's active panel (seating additional independent
    /// verifiers if the panel was auto-created) or creates a run-default panel bound to all
    /// contributing non-`Verifier`/non-`Monitor`/non-`Coordinator` members.
    ///
    /// # Errors
    /// Fails closed with [`SwarmError::SybilLineageViolation`], [`SwarmError::VerifierMustInherit`],
    /// [`SwarmError::NotFound`], or [`OperationsError`].
    pub fn record_swarm_verdict(
        &mut self,
        command: &RecordSwarmVerdictCommand,
    ) -> Result<SwarmVerdictRecord, SwarmError> {
        command.validate()?;
        let digest = verdict_record_digest(command);
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(OperationsError::from)?;
        if let Some(replayed) = load_verdict_replay(&tx, command.request_id, digest)? {
            tx.commit().map_err(OperationsError::from)?;
            return Ok(replayed);
        }
        let run = require_run_by_id(&tx, command.workspace_id, command.swarm_run_id)?;
        if run.state().is_terminal() {
            return Err(SwarmError::InvalidStateTransition(
                "cannot record verdict on terminal Swarm run",
            ));
        }
        if command.recorded_at_ms < run.created_at_ms() {
            return Err(OperationsError::Bounds(
                "swarm verdict recorded_at_ms precedes run creation",
            )
            .into());
        }
        let verifier = require_member_by_id(&tx, command.swarm_run_id, command.verifier_member_id)?;
        if verifier.workspace_id() != command.workspace_id {
            return Err(SwarmError::NotFound("swarm verifier in workspace"));
        }
        let all_members = load_members_by_run(&tx, command.swarm_run_id)?;
        let panel =
            resolve_or_seat_panel_for_verdict(&tx, self.destination, &run, &all_members, command)?;
        let author_ids = panel.author_member_ids();
        let mut verifier_ids = panel.verifier_member_ids();
        if !verifier_ids.contains(&command.verifier_member_id) {
            verifier_ids.push(command.verifier_member_id);
        }
        enforce_lineage_separated_verifiers(&all_members, &author_ids, &verifier_ids)?;

        let verdict_id = derive_deterministic_uuid(
            SWARM_VERDICT_ID_DOMAIN,
            self.destination,
            command.request_id,
        );
        tx.execute(
            INSERT_SWARM_VERDICT_SQL,
            params![
                verdict_id.as_bytes(),
                command.request_id.as_bytes(),
                digest.as_slice(),
                panel.panel_id.as_bytes(),
                command.swarm_run_id.as_bytes(),
                command.verifier_member_id.as_bytes(),
                command.outcome.as_str(),
                command.reviewed_diff_digest.as_slice(),
                command.rationale.as_str(),
                command.recorded_at_ms,
            ],
        )
        .map_err(|err| map_sqlite_conflict(err, "duplicate verifier verdict for diff digest"))?;

        let created = load_verdict_by_request_id(&tx, command.request_id)?
            .ok_or(OperationsError::Corrupt("inserted swarm verdict missing"))?;
        tx.commit().map_err(OperationsError::from)?;
        Ok(created)
    }

    /// Verify that the Swarm run holds a converged candidate diff digest and a quorum of
    /// `Accepted` verdicts from lineage-separated `Verifier`s whose `reviewed_diff_digest`
    /// equals `converged_diff_digest`.
    ///
    /// # Errors
    /// Fails closed with [`SwarmError::VerdictDiffDigestMismatch`], [`SwarmError::ReviewNotAccepted`],
    /// [`SwarmError::SybilLineageViolation`], or [`SwarmError::NotFound`].
    pub fn verify_swarm_convergence_gate(
        &self,
        workspace_id: WorkspaceId,
        swarm_run_id: Uuid,
    ) -> Result<SwarmConvergenceGateOutcome, SwarmError> {
        validate_workspace_id(workspace_id)?;
        validate_non_nil_uuid(swarm_run_id, "swarm_run_id")?;
        let tx = self
            .connection
            .unchecked_transaction()
            .map_err(OperationsError::from)?;
        let run = require_run_by_id(&tx, workspace_id, swarm_run_id)?;
        let outcome = verify_swarm_convergence_gate_in_tx(&tx, &run)?;
        crate::catalog::commit_read(tx, self.destination)?;
        Ok(outcome)
    }
}

pub(super) fn verify_swarm_convergence_gate_in_tx(
    tx: &Transaction<'_>,
    run: &SwarmRunRecord,
) -> Result<SwarmConvergenceGateOutcome, SwarmError> {
    let Some(converged_diff_digest) = run.converged_diff_digest() else {
        return Err(SwarmError::ReviewNotAccepted);
    };
    let Some(panel) = load_latest_panel_by_run(tx, run.swarm_run_id())? else {
        return Err(SwarmError::ReviewNotAccepted);
    };
    let all_members = load_members_by_run(tx, run.swarm_run_id())?;
    let author_ids = panel.author_member_ids();
    let verifier_ids = panel.verifier_member_ids();
    if author_ids.is_empty() || verifier_ids.is_empty() {
        return Err(SwarmError::ReviewNotAccepted);
    }
    enforce_lineage_separated_verifiers(&all_members, &author_ids, &verifier_ids)?;

    let verifier_set: BTreeSet<Uuid> = verifier_ids.into_iter().collect();
    let verdicts = load_verdicts_by_panel(tx, panel.panel_id)?;
    let mut matching_accepted_by_verifier: BTreeMap<Uuid, SwarmVerdictRecord> = BTreeMap::new();
    let mut has_mismatched_accepted = false;
    let mut has_blocking_rejection = false;

    for verdict in verdicts {
        if !verifier_set.contains(&verdict.verifier_member_id) {
            continue;
        }
        match verdict.outcome {
            ReviewVerdictOutcome::Accepted => {
                if verdict.reviewed_diff_digest == converged_diff_digest {
                    matching_accepted_by_verifier
                        .entry(verdict.verifier_member_id)
                        .or_insert(verdict);
                } else {
                    has_mismatched_accepted = true;
                }
            }
            ReviewVerdictOutcome::Rejected | ReviewVerdictOutcome::NeedsChanges => {
                if verdict.reviewed_diff_digest == converged_diff_digest {
                    has_blocking_rejection = true;
                }
            }
        }
    }

    if has_blocking_rejection {
        return Err(SwarmError::ReviewNotAccepted);
    }

    let required = usize::try_from(panel.required_quorum)
        .map_err(|_| OperationsError::Corrupt("swarm panel required_quorum"))?;
    if matching_accepted_by_verifier.len() >= required {
        return Ok(SwarmConvergenceGateOutcome {
            panel_id: panel.panel_id,
            swarm_run_id: run.swarm_run_id(),
            converged_diff_digest,
            required_quorum: panel.required_quorum,
            accepted_verdicts: matching_accepted_by_verifier.into_values().collect(),
        });
    }
    if has_mismatched_accepted {
        return Err(SwarmError::VerdictDiffDigestMismatch);
    }
    Err(SwarmError::ReviewNotAccepted)
}

fn resolve_or_seat_panel_for_verdict(
    tx: &Transaction<'_>,
    destination: Uuid,
    run: &SwarmRunRecord,
    all_members: &[SwarmMemberRecord],
    command: &RecordSwarmVerdictCommand,
) -> Result<SwarmVerificationPanelRecord, SwarmError> {
    if let Some(panel_id) = command.panel_id {
        let panel = load_panel_by_id(tx, run.swarm_run_id(), panel_id)?
            .ok_or(SwarmError::NotFound("swarm verification panel"))?;
        if !panel.has_verifier(command.verifier_member_id) {
            return Err(SwarmError::SybilLineageViolation {
                reason: "verifier is not seated on the specified verification panel",
            });
        }
        return Ok(panel);
    }

    if let Some(existing) = load_latest_panel_by_run(tx, run.swarm_run_id())? {
        if !existing.has_verifier(command.verifier_member_id) {
            if !existing.auto_created {
                return Err(SwarmError::SybilLineageViolation {
                    reason: "verifier is not seated on the explicit verification panel",
                });
            }
            let author_ids = existing.author_member_ids();
            let mut candidate_verifiers = existing.verifier_member_ids();
            candidate_verifiers.push(command.verifier_member_id);
            enforce_lineage_separated_verifiers(all_members, &author_ids, &candidate_verifiers)?;
            tx.execute(
                INSERT_SWARM_PANEL_SEAT_SQL,
                params![
                    existing.panel_id.as_bytes(),
                    run.swarm_run_id().as_bytes(),
                    command.verifier_member_id.as_bytes(),
                    SwarmPanelSeatKind::Verifier.as_str(),
                    command.recorded_at_ms,
                ],
            )
            .map_err(OperationsError::from)?;
            return load_panel_by_id(tx, run.swarm_run_id(), existing.panel_id)?
                .ok_or(OperationsError::Corrupt("swarm auto panel reload").into());
        }
        return Ok(existing);
    }

    create_auto_panel_for_verdict(tx, destination, run, all_members, command)
}

fn create_auto_panel_for_verdict(
    tx: &Transaction<'_>,
    destination: Uuid,
    run: &SwarmRunRecord,
    all_members: &[SwarmMemberRecord],
    command: &RecordSwarmVerdictCommand,
) -> Result<SwarmVerificationPanelRecord, SwarmError> {
    let mut author_ids: Vec<Uuid> = all_members
        .iter()
        .filter(|member| {
            matches!(
                member.role_kind(),
                SwarmRoleKind::Worker | SwarmRoleKind::Helper
            )
        })
        .map(SwarmMemberRecord::member_id)
        .collect();
    if author_ids.is_empty() {
        author_ids = all_members
            .iter()
            .filter(|member| member.role_kind() == SwarmRoleKind::Coordinator)
            .map(SwarmMemberRecord::member_id)
            .collect();
    }
    if author_ids.is_empty() {
        return Err(SwarmError::NotFound("swarm author member for panel"));
    }
    let verifiers = vec![command.verifier_member_id];
    enforce_lineage_separated_verifiers(all_members, &author_ids, &verifiers)?;

    let panel_id =
        derive_deterministic_uuid(SWARM_PANEL_ID_DOMAIN, destination, command.request_id);
    let digest = panel_create_digest(
        command.workspace_id,
        run.swarm_run_id(),
        &author_ids,
        &verifiers,
        run.required_verifier_quorum(),
    );
    insert_panel_and_seats(
        tx,
        &PanelInsertParams {
            panel_id,
            request_id: panel_id,
            digest,
            swarm_run_id: run.swarm_run_id(),
            workspace_id: command.workspace_id,
            required_quorum: run.required_verifier_quorum(),
            auto_created: true,
            created_at_ms: command.recorded_at_ms,
            author_ids: &author_ids,
            verifier_ids: &verifiers,
        },
        "swarm auto verification panel insert",
    )?;
    load_panel_by_id(tx, run.swarm_run_id(), panel_id)?
        .ok_or(OperationsError::Corrupt("inserted auto swarm panel missing").into())
}

struct PanelInsertParams<'a> {
    panel_id: Uuid,
    request_id: Uuid,
    digest: [u8; 32],
    swarm_run_id: Uuid,
    workspace_id: WorkspaceId,
    required_quorum: u32,
    auto_created: bool,
    created_at_ms: i64,
    author_ids: &'a [Uuid],
    verifier_ids: &'a [Uuid],
}

fn insert_panel_and_seats(
    tx: &Transaction<'_>,
    input: &PanelInsertParams<'_>,
    conflict_label: &'static str,
) -> Result<(), SwarmError> {
    tx.execute(
        INSERT_SWARM_VERIFICATION_PANEL_SQL,
        params![
            input.panel_id.as_bytes(),
            input.request_id.as_bytes(),
            input.digest.as_slice(),
            input.swarm_run_id.as_bytes(),
            input.workspace_id.into_uuid().as_bytes(),
            i64::from(input.required_quorum),
            i64::from(input.auto_created),
            input.created_at_ms,
        ],
    )
    .map_err(|err| map_sqlite_conflict(err, conflict_label))?;
    for (seat_kind, member_ids) in [
        (SwarmPanelSeatKind::Author.as_str(), input.author_ids),
        (SwarmPanelSeatKind::Verifier.as_str(), input.verifier_ids),
    ] {
        for member_id in member_ids {
            tx.execute(
                INSERT_SWARM_PANEL_SEAT_SQL,
                params![
                    input.panel_id.as_bytes(),
                    input.swarm_run_id.as_bytes(),
                    member_id.as_bytes(),
                    seat_kind,
                    input.created_at_ms,
                ],
            )
            .map_err(OperationsError::from)?;
        }
    }
    Ok(())
}

/// Enforce `Verifier` role, `Inherit` workspace mode, pairwise verifier 3-axis distinctness,
/// and 3-axis identity separation (`AgentProfileId`, `participant_id`, `AuthenticatedPrincipal`)
/// against EVERY member in the entire author spawn lineage tree (author + ancestors + descendants)
/// AND the `Coordinator`.
fn enforce_lineage_separated_verifiers(
    all_members: &[SwarmMemberRecord],
    author_ids: &[Uuid],
    verifier_ids: &[Uuid],
) -> Result<(), SwarmError> {
    let by_id: BTreeMap<Uuid, &SwarmMemberRecord> =
        all_members.iter().map(|m| (m.member_id(), m)).collect();
    let blocked_ids = collect_blocked_lineage_ids(all_members, &by_id, author_ids)?;

    let mut resolved_verifiers: Vec<&SwarmMemberRecord> = Vec::with_capacity(verifier_ids.len());
    for &verifier_id in verifier_ids {
        let Some(&verifier) = by_id.get(&verifier_id) else {
            return Err(SwarmError::NotFound("swarm verifier member"));
        };
        if verifier.role_kind() != SwarmRoleKind::Verifier {
            return Err(SwarmError::SybilLineageViolation {
                reason: "member is not a Verifier role",
            });
        }
        if verifier.workspace_mode() != SwarmWorkspaceMode::Inherit {
            return Err(SwarmError::VerifierMustInherit);
        }
        if blocked_ids.contains(&verifier_id) {
            return Err(SwarmError::SybilLineageViolation {
                reason: "verifier belongs to author spawn lineage tree or Coordinator",
            });
        }
        for &blocked_id in &blocked_ids {
            let Some(&blocked) = by_id.get(&blocked_id) else {
                continue;
            };
            check_three_axis_distinct(
                verifier,
                blocked,
                "verifier shares AgentProfileId with author spawn lineage or Coordinator",
                "verifier shares participant_id with author spawn lineage or Coordinator",
                "verifier shares AuthenticatedPrincipal with author spawn lineage or Coordinator",
            )?;
        }
        for &prior_verifier in &resolved_verifiers {
            if prior_verifier.member_id() == verifier.member_id() {
                return Err(SwarmError::SybilLineageViolation {
                    reason: "duplicate verifier member in panel",
                });
            }
            check_three_axis_distinct(
                verifier,
                prior_verifier,
                "panel verifiers share AgentProfileId",
                "panel verifiers share participant_id",
                "panel verifiers share AuthenticatedPrincipal",
            )?;
        }
        resolved_verifiers.push(verifier);
    }
    Ok(())
}

fn collect_blocked_lineage_ids(
    all_members: &[SwarmMemberRecord],
    by_id: &BTreeMap<Uuid, &SwarmMemberRecord>,
    author_ids: &[Uuid],
) -> Result<BTreeSet<Uuid>, SwarmError> {
    let mut children_by_parent: BTreeMap<Uuid, Vec<Uuid>> = BTreeMap::new();
    for member in all_members {
        if let Some(parent_id) = member.parent_member_id() {
            children_by_parent
                .entry(parent_id)
                .or_default()
                .push(member.member_id());
        }
    }

    let mut blocked_ids = BTreeSet::new();
    for &author_id in author_ids {
        if !by_id.contains_key(&author_id) {
            return Err(SwarmError::NotFound("swarm author member"));
        }
        blocked_ids.insert(author_id);

        // Walk upward through all ancestors (without branching down into unrelated siblings).
        let mut cursor = author_id;
        let mut visited_up = BTreeSet::new();
        visited_up.insert(author_id);
        while let Some(member) = by_id.get(&cursor) {
            let Some(parent_id) = member.parent_member_id() else {
                break;
            };
            blocked_ids.insert(parent_id);
            if !visited_up.insert(parent_id) {
                break;
            }
            cursor = parent_id;
        }

        // Walk downward through all descendant helpers spawned by the author or its helpers.
        let mut queue = VecDeque::new();
        queue.push_back(author_id);
        let mut visited_down = BTreeSet::new();
        visited_down.insert(author_id);
        while let Some(current) = queue.pop_front() {
            if let Some(children) = children_by_parent.get(&current) {
                for &child_id in children {
                    blocked_ids.insert(child_id);
                    if visited_down.insert(child_id) {
                        queue.push_back(child_id);
                    }
                }
            }
        }
    }

    // Also include the Coordinator in the exclusion set.
    for member in all_members {
        if member.role_kind() == SwarmRoleKind::Coordinator {
            blocked_ids.insert(member.member_id());
        }
    }
    Ok(blocked_ids)
}

fn check_three_axis_distinct(
    verifier: &SwarmMemberRecord,
    other: &SwarmMemberRecord,
    profile_reason: &'static str,
    participant_reason: &'static str,
    principal_reason: &'static str,
) -> Result<(), SwarmError> {
    if verifier.identity().profile_id() == other.identity().profile_id() {
        return Err(SwarmError::SybilLineageViolation {
            reason: profile_reason,
        });
    }
    if verifier.identity().participant_id() == other.identity().participant_id() {
        return Err(SwarmError::SybilLineageViolation {
            reason: participant_reason,
        });
    }
    if verifier.identity().principal() == other.identity().principal() {
        return Err(SwarmError::SybilLineageViolation {
            reason: principal_reason,
        });
    }
    Ok(())
}

fn panel_create_digest(
    workspace_id: WorkspaceId,
    swarm_run_id: Uuid,
    authors: &[Uuid],
    verifiers: &[Uuid],
    required_quorum: u32,
) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(SWARM_PANEL_DIGEST_DOMAIN);
    hasher.update(workspace_id.into_uuid().as_bytes());
    hasher.update(swarm_run_id.as_bytes());
    hasher.update(len_u64(authors.len()));
    for id in authors {
        hasher.update(id.as_bytes());
    }
    hasher.update(len_u64(verifiers.len()));
    for id in verifiers {
        hasher.update(id.as_bytes());
    }
    hasher.update(required_quorum.to_le_bytes());
    hasher.finalize().into()
}

fn verdict_record_digest(command: &RecordSwarmVerdictCommand) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(SWARM_VERDICT_DIGEST_DOMAIN);
    hasher.update(command.workspace_id.into_uuid().as_bytes());
    hasher.update(command.swarm_run_id.as_bytes());
    hash_optional_uuid(&mut hasher, command.panel_id);
    hasher.update(command.verifier_member_id.as_bytes());
    hash_str(&mut hasher, command.outcome.as_str());
    hasher.update(command.reviewed_diff_digest);
    hash_str(&mut hasher, &command.rationale);
    hasher.finalize().into()
}

struct RawPanelRow {
    panel_id: Vec<u8>,
    _request_id: Vec<u8>,
    payload_digest: Vec<u8>,
    swarm_run_id: Vec<u8>,
    workspace_id: Vec<u8>,
    required_quorum: i64,
    auto_created: i64,
    created_at_ms: i64,
}

fn read_raw_panel_row(row: &Row<'_>) -> rusqlite::Result<RawPanelRow> {
    Ok(RawPanelRow {
        panel_id: row.get(0)?,
        _request_id: row.get(1)?,
        payload_digest: row.get(2)?,
        swarm_run_id: row.get(3)?,
        workspace_id: row.get(4)?,
        required_quorum: row.get(5)?,
        auto_created: row.get(6)?,
        created_at_ms: row.get(7)?,
    })
}

fn decode_panel_row(
    conn: &Connection,
    raw: &RawPanelRow,
) -> Result<(SwarmVerificationPanelRecord, [u8; 32]), OperationsError> {
    let panel_id = decode_non_nil_uuid(&raw.panel_id, "swarm panel_id")?;
    let payload_digest = decode_non_zero_digest(&raw.payload_digest, "swarm panel payload_digest")?;
    let swarm_run_id = decode_non_nil_uuid(&raw.swarm_run_id, "swarm_run_id")?;
    let workspace_id = WorkspaceId::from_uuid(decode_non_nil_uuid(
        &raw.workspace_id,
        "swarm workspace_id",
    )?);
    let required_quorum = decode_positive_u32(raw.required_quorum, "swarm panel required_quorum")?;
    let auto_created = decode_bool_flag(raw.auto_created, "swarm panel auto_created")?;
    if raw.created_at_ms < 0 {
        return Err(OperationsError::Corrupt("swarm panel created_at_ms"));
    }
    let seats = load_panel_seats(conn, panel_id)?;
    Ok((
        SwarmVerificationPanelRecord {
            panel_id,
            swarm_run_id,
            workspace_id,
            required_quorum,
            auto_created,
            seats,
            created_at_ms: raw.created_at_ms,
            replayed: false,
        },
        payload_digest,
    ))
}

fn load_panel_seats(
    conn: &Connection,
    panel_id: Uuid,
) -> Result<Vec<SwarmPanelSeatRecord>, OperationsError> {
    let limit = i64::try_from(MAX_SWARM_MEMBERS_PER_RUN * 2)
        .map_err(|_| OperationsError::Bounds("swarm panel seats limit"))?;
    let mut stmt = conn.prepare(SELECT_SWARM_PANEL_SEATS_SQL)?;
    let raw_rows = stmt
        .query_map(params![panel_id.as_bytes(), limit], |row| {
            Ok((
                row.get::<_, Vec<u8>>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    drop(stmt);
    raw_rows
        .into_iter()
        .map(|(member_bytes, kind_raw, assigned_at_ms)| {
            if assigned_at_ms < 0 {
                return Err(OperationsError::Corrupt("swarm panel seat assigned_at_ms"));
            }
            Ok(SwarmPanelSeatRecord {
                member_id: decode_non_nil_uuid(&member_bytes, "swarm panel seat member_id")?,
                seat_kind: SwarmPanelSeatKind::parse(&kind_raw)?,
                assigned_at_ms,
            })
        })
        .collect()
}

fn load_panel_by_id(
    conn: &Connection,
    swarm_run_id: Uuid,
    panel_id: Uuid,
) -> Result<Option<SwarmVerificationPanelRecord>, OperationsError> {
    let raw = conn
        .query_row(
            SELECT_SWARM_PANEL_BY_ID_SQL,
            params![swarm_run_id.as_bytes(), panel_id.as_bytes()],
            read_raw_panel_row,
        )
        .optional()?;
    raw.map(|r| decode_panel_row(conn, &r).map(|(panel, _)| panel))
        .transpose()
}

fn load_latest_panel_by_run(
    conn: &Connection,
    swarm_run_id: Uuid,
) -> Result<Option<SwarmVerificationPanelRecord>, OperationsError> {
    let raw = conn
        .query_row(
            SELECT_LATEST_SWARM_PANEL_BY_RUN_SQL,
            params![swarm_run_id.as_bytes()],
            read_raw_panel_row,
        )
        .optional()?;
    raw.map(|r| decode_panel_row(conn, &r).map(|(panel, _)| panel))
        .transpose()
}

fn load_panel_replay(
    conn: &Connection,
    request_id: Uuid,
    expected_digest: [u8; 32],
) -> Result<Option<SwarmVerificationPanelRecord>, SwarmError> {
    let raw = conn
        .query_row(
            SELECT_SWARM_PANEL_BY_REQUEST_ID_SQL,
            params![request_id.as_bytes()],
            read_raw_panel_row,
        )
        .optional()
        .map_err(OperationsError::from)?;
    let Some(raw) = raw else {
        return Ok(None);
    };
    let (mut panel, stored_digest) = decode_panel_row(conn, &raw)?;
    if stored_digest != expected_digest {
        return Err(OperationsError::Conflict(
            "swarm verification panel request_id reused with different payload",
        )
        .into());
    }
    panel.replayed = true;
    Ok(Some(panel))
}

struct RawVerdictRow {
    verdict_id: Vec<u8>,
    request_id: Vec<u8>,
    payload_digest: Vec<u8>,
    panel_id: Vec<u8>,
    swarm_run_id: Vec<u8>,
    verifier_member_id: Vec<u8>,
    outcome: String,
    reviewed_diff_digest: Vec<u8>,
    rationale_bytes: Vec<u8>,
    recorded_at_ms: i64,
}

fn read_raw_verdict_row(row: &Row<'_>) -> rusqlite::Result<RawVerdictRow> {
    Ok(RawVerdictRow {
        verdict_id: row.get(0)?,
        request_id: row.get(1)?,
        payload_digest: row.get(2)?,
        panel_id: row.get(3)?,
        swarm_run_id: row.get(4)?,
        verifier_member_id: row.get(5)?,
        outcome: row.get(6)?,
        reviewed_diff_digest: row.get(7)?,
        rationale_bytes: row.get(8)?,
        recorded_at_ms: row.get(9)?,
    })
}

fn decode_verdict_row(
    raw: RawVerdictRow,
) -> Result<(SwarmVerdictRecord, [u8; 32]), OperationsError> {
    let verdict_id = decode_non_nil_uuid(&raw.verdict_id, "swarm verdict_id")?;
    let request_id = decode_non_nil_uuid(&raw.request_id, "swarm verdict request_id")?;
    let payload_digest =
        decode_non_zero_digest(&raw.payload_digest, "swarm verdict payload_digest")?;
    let panel_id = decode_non_nil_uuid(&raw.panel_id, "swarm verdict panel_id")?;
    let swarm_run_id = decode_non_nil_uuid(&raw.swarm_run_id, "swarm_run_id")?;
    let verifier_member_id =
        decode_non_nil_uuid(&raw.verifier_member_id, "swarm verifier_member_id")?;
    let outcome = match raw.outcome.as_str() {
        "ACCEPTED" => ReviewVerdictOutcome::Accepted,
        "REJECTED" => ReviewVerdictOutcome::Rejected,
        "NEEDS_CHANGES" => ReviewVerdictOutcome::NeedsChanges,
        _ => return Err(OperationsError::Corrupt("swarm verdict outcome")),
    };
    let reviewed_diff_digest =
        decode_non_zero_digest(&raw.reviewed_diff_digest, "swarm reviewed_diff_digest")?;
    let rationale = decode_utf8(raw.rationale_bytes, "swarm verdict rationale")?;
    if rationale.trim().is_empty() || rationale.len() > MAX_SWARM_VERDICT_RATIONALE_BYTES {
        return Err(OperationsError::Corrupt("swarm verdict rationale bounds"));
    }
    if raw.recorded_at_ms < 0 {
        return Err(OperationsError::Corrupt("swarm verdict recorded_at_ms"));
    }
    Ok((
        SwarmVerdictRecord {
            verdict_id,
            request_id,
            panel_id,
            swarm_run_id,
            verifier_member_id,
            outcome,
            reviewed_diff_digest,
            rationale,
            recorded_at_ms: raw.recorded_at_ms,
            replayed: false,
        },
        payload_digest,
    ))
}

fn load_verdict_by_request_id(
    conn: &Connection,
    request_id: Uuid,
) -> Result<Option<SwarmVerdictRecord>, OperationsError> {
    let raw = conn
        .query_row(
            SELECT_SWARM_VERDICT_BY_REQUEST_ID_SQL,
            params![request_id.as_bytes()],
            read_raw_verdict_row,
        )
        .optional()?;
    raw.map(|row| decode_verdict_row(row).map(|(verdict, _)| verdict))
        .transpose()
}

fn load_verdict_replay(
    conn: &Connection,
    request_id: Uuid,
    expected_digest: [u8; 32],
) -> Result<Option<SwarmVerdictRecord>, SwarmError> {
    let raw = conn
        .query_row(
            SELECT_SWARM_VERDICT_BY_REQUEST_ID_SQL,
            params![request_id.as_bytes()],
            read_raw_verdict_row,
        )
        .optional()
        .map_err(OperationsError::from)?;
    let Some(raw) = raw else {
        return Ok(None);
    };
    let (mut verdict, stored_digest) = decode_verdict_row(raw)?;
    if stored_digest != expected_digest {
        return Err(OperationsError::Conflict(
            "swarm verdict request_id reused with different payload",
        )
        .into());
    }
    verdict.replayed = true;
    Ok(Some(verdict))
}

fn load_verdicts_by_panel(
    conn: &Connection,
    panel_id: Uuid,
) -> Result<Vec<SwarmVerdictRecord>, OperationsError> {
    let mut stmt = conn.prepare(SELECT_SWARM_VERDICTS_BY_PANEL_SQL)?;
    let raw_rows = stmt
        .query_map(
            params![panel_id.as_bytes(), MAX_VERDICTS_PER_PANEL_READ],
            read_raw_verdict_row,
        )?
        .collect::<Result<Vec<_>, _>>()?;
    drop(stmt);
    raw_rows
        .into_iter()
        .map(|row| decode_verdict_row(row).map(|(verdict, _)| verdict))
        .collect()
}
