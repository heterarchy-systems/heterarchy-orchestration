use forge_domain::{ActivityId, AgentInstanceId, AgentSessionId, TimestampMillis, WorkspaceId};
use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use uuid::Uuid;

use crate::{
    CurrentLeadershipAuthority, LeadershipScope, OperationsCatalog, OperationsError, catalog,
    operational_actor::{OperationalActor, resolve_actor},
    teams,
};

use super::super::resolve_actor_provenance;
use super::{
    TemporaryDispatchAdmissionBinding, TemporaryDispatchConsentCommand,
    TemporaryDispatchConsentKind, TemporaryDispatchConsentReceipt,
    TemporaryDispatchConsentWithdrawalCommand, TemporaryDispatchConsentWithdrawalReceipt,
    same_actor, validate_command_identity,
};

impl OperationsCatalog {
    /// Read the immutable admission binding for an authorized consent actor.
    ///
    /// This lets the target, current home-Team Leader, or Workspace
    /// Orchestrator construct a typed consent request without accepting a
    /// caller-fabricated binding. The caller role is checked against the
    /// current admission state; the subsequent write rechecks that state in
    /// its own IMMEDIATE transaction.
    ///
    /// # Errors
    /// Rejects invalid identities, unverified or foreign Workspaces, missing
    /// requests, stale roles or bindings, and corrupt durable rows.
    pub fn temporary_dispatch_consent_binding(
        &mut self,
        workspace_id: WorkspaceId,
        request_id: Uuid,
        kind: TemporaryDispatchConsentKind,
        actor: OperationalActor,
    ) -> Result<TemporaryDispatchAdmissionBinding, OperationsError> {
        if request_id.is_nil() || !super::actor_input_is_valid(actor) {
            return Err(OperationsError::Conflict(
                "temporary dispatch consent binding identity",
            ));
        }
        let transaction = self.connection.unchecked_transaction()?;
        teams::ensure_verified_workspace(&transaction, workspace_id)?;
        let resolved_actor = resolve_actor(&transaction, workspace_id, actor)?;
        let actor = resolve_actor_provenance(actor, &resolved_actor)?;
        let record = super::super::load_request(&transaction, request_id)?.ok_or(
            OperationsError::Conflict("temporary dispatch admission missing"),
        )?;
        if record.workspace_id() != workspace_id {
            return Err(OperationsError::Conflict(
                "temporary dispatch admission Workspace",
            ));
        }
        let binding = TemporaryDispatchAdmissionBinding::from_record(&record);
        authorize_consent(&transaction, &binding, kind, &actor)?;
        catalog::commit_read(transaction, self.destination)?;
        Ok(binding)
    }

    /// Recover a consent's admission binding as its original logical author,
    /// even when that role is no longer current. This grants historical read
    /// access only; it does not keep the consent valid for reservation.
    ///
    /// # Errors
    /// Rejects invalid identities, unverified Workspace, foreign/closed
    /// Session, or corrupt durable rows. A non-author receives `None`.
    pub fn temporary_dispatch_consent_readback_binding(
        &mut self,
        workspace_id: WorkspaceId,
        request_id: Uuid,
        receipt_id: Uuid,
        kind: TemporaryDispatchConsentKind,
        actor: OperationalActor,
    ) -> Result<Option<TemporaryDispatchAdmissionBinding>, OperationsError> {
        if request_id.is_nil() || receipt_id.is_nil() || !super::actor_input_is_valid(actor) {
            return Err(OperationsError::Conflict(
                "temporary dispatch consent readback binding identity",
            ));
        }
        let transaction = self.connection.unchecked_transaction()?;
        teams::ensure_verified_workspace(&transaction, workspace_id)?;
        let resolved_actor = resolve_actor(&transaction, workspace_id, actor)?;
        let reader = resolve_actor_provenance(actor, &resolved_actor)?;
        let record = super::super::load_request(&transaction, request_id)?;
        let binding = record
            .filter(|record| record.workspace_id() == workspace_id)
            .map(|record| TemporaryDispatchAdmissionBinding::from_record(&record));
        let result = if let Some(binding) = binding {
            let receipt = load_consent_receipt(&transaction, receipt_id)?;
            receipt
                .filter(|receipt| {
                    receipt.request_id() == request_id
                        && receipt.kind() == kind
                        && receipt.binding == binding
                        && same_logical_actor(&receipt.actor, &reader)
                })
                .map(|_| binding)
        } else {
            None
        };
        catalog::commit_read(transaction, self.destination)?;
        Ok(result)
    }

    /// Read the reservation binding as its original requester or the current
    /// Workspace Orchestrator.
    ///
    /// # Errors
    /// Rejects invalid identity, unverified/foreign Workspace, missing request,
    /// or actors outside the existing reservation readback policy.
    pub fn temporary_dispatch_reservation_binding(
        &mut self,
        workspace_id: WorkspaceId,
        request_id: Uuid,
        actor: OperationalActor,
    ) -> Result<TemporaryDispatchAdmissionBinding, OperationsError> {
        if request_id.is_nil() || !super::actor_input_is_valid(actor) {
            return Err(OperationsError::Conflict(
                "temporary dispatch reservation binding identity",
            ));
        }
        let transaction = self.connection.unchecked_transaction()?;
        teams::ensure_verified_workspace(&transaction, workspace_id)?;
        let resolved = resolve_actor(&transaction, workspace_id, actor)?;
        let reader = resolve_actor_provenance(actor, &resolved)?;
        let record = super::super::load_request(&transaction, request_id)?.ok_or(
            OperationsError::Conflict("temporary dispatch admission missing"),
        )?;
        if record.workspace_id() != workspace_id {
            return Err(OperationsError::Conflict(
                "temporary dispatch admission Workspace",
            ));
        }
        let binding = TemporaryDispatchAdmissionBinding::from_record(&record);
        let is_requester = same_logical_actor(binding.actor(), &reader);
        let is_current_orchestrator = if is_requester {
            false
        } else {
            current_authority(&transaction, workspace_id, None)?
                .map(|authority| {
                    participant_identity(&transaction, workspace_id, authority.participant_id())
                        .map(|(participation, profile)| {
                            participation == reader.participation_id().into_uuid()
                                && profile == reader.profile_id()
                        })
                })
                .transpose()?
                .unwrap_or(false)
        };
        if !is_requester && !is_current_orchestrator {
            return Err(OperationsError::Conflict(
                "temporary dispatch reservation readback actor",
            ));
        }
        catalog::commit_read(transaction, self.destination)?;
        Ok(binding)
    }

    /// Record the authenticated Agent, home-Leader, or Workspace-Orchestrator consent.
    ///
    /// A replay with the same receipt ID and exact binding is read back before
    /// current-role CAS checks. New receipts capture the current authority epoch
    /// and replace only the current consent pointer; prior receipts remain immutable.
    ///
    /// # Errors
    /// Rejects foreign/stale actors, absent roles, revision conflicts, corrupt
    /// receipts, clock errors, and SQLite failures.
    pub fn record_temporary_dispatch_consent(
        &mut self,
        command: &TemporaryDispatchConsentCommand,
        trusted_now: impl FnOnce() -> Result<TimestampMillis, OperationsError>,
    ) -> Result<TemporaryDispatchConsentReceipt, OperationsError> {
        validate_command_identity(
            command.receipt_id,
            &command.binding,
            command.expected_revision,
            command.actor,
        )?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        teams::ensure_verified_workspace(&transaction, command.binding.workspace_id)?;
        let resolved_actor =
            resolve_actor(&transaction, command.binding.workspace_id, command.actor)?;
        let actor = resolve_actor_provenance(command.actor, &resolved_actor)?;
        let admitted = load_bound_request(&transaction, &command.binding)?;

        if let Some(existing) = load_consent_receipt(&transaction, command.receipt_id)? {
            if !consent_matches_command(&existing, command, &actor) {
                return Err(OperationsError::Conflict(
                    "temporary dispatch consent receipt replay",
                ));
            }
            transaction.commit()?;
            return Ok(with_consent_replay(existing));
        }
        if lifecycle_receipt_id_exists(&transaction, command.receipt_id)? {
            return Err(OperationsError::Conflict(
                "temporary dispatch lifecycle receipt identity",
            ));
        }

        let current_revision =
            current_consent_revision(&transaction, command.binding.request_id, command.kind)?;
        if current_revision != command.expected_revision {
            return Err(OperationsError::Conflict(
                "temporary dispatch consent revision",
            ));
        }
        let authority_revision =
            authorize_consent(&transaction, &command.binding, command.kind, &actor)?;
        let revision = next_revision(current_revision, "temporary dispatch consent revision")?;
        let recorded_at = trusted_now()?;
        let receipt = TemporaryDispatchConsentReceipt {
            receipt_id: command.receipt_id,
            binding: command.binding.clone(),
            kind: command.kind,
            revision,
            actor: actor.clone(),
            authority_revision,
            recorded_at,
            replayed: false,
        };
        insert_consent_receipt(&transaction, &receipt)?;
        let changed = transaction.execute(
            "INSERT INTO ops_temporary_dispatch_current_consents(request_id,consent_kind,receipt_id,revision) VALUES(?1,?2,?3,?4) ON CONFLICT(request_id,consent_kind) DO UPDATE SET receipt_id=excluded.receipt_id,revision=excluded.revision WHERE ops_temporary_dispatch_current_consents.revision=?5",
            params![
                command.binding.request_id.as_bytes(),
                command.kind.as_str(),
                command.receipt_id.as_bytes(),
                to_i64(revision, "temporary dispatch consent revision")?,
                to_i64(current_revision, "temporary dispatch consent revision")?,
            ],
        )?;
        if changed != 1 {
            return Err(OperationsError::Conflict(
                "temporary dispatch consent revision",
            ));
        }
        let commit_result = transaction.commit();
        commit_consent_write(
            commit_result,
            &self.connection,
            receipt,
            command,
            &admitted,
            &actor,
        )
    }

    /// Withdraw one exact consent authored by the same logical participant.
    ///
    /// A current role epoch is required to withdraw a live consent. If that role
    /// epoch has changed, the old receipt is already invalid at reservation time.
    ///
    /// # Errors
    /// Rejects foreign or stale consent, changed authority, revision conflicts,
    /// corrupt receipts, clock errors, and SQLite failures.
    pub fn withdraw_temporary_dispatch_consent(
        &mut self,
        command: &TemporaryDispatchConsentWithdrawalCommand,
        trusted_now: impl FnOnce() -> Result<TimestampMillis, OperationsError>,
    ) -> Result<TemporaryDispatchConsentWithdrawalReceipt, OperationsError> {
        validate_command_identity(
            command.receipt_id,
            &command.binding,
            command.expected_revision,
            command.actor,
        )?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        teams::ensure_verified_workspace(&transaction, command.binding.workspace_id)?;
        let resolved_actor =
            resolve_actor(&transaction, command.binding.workspace_id, command.actor)?;
        let actor = resolve_actor_provenance(command.actor, &resolved_actor)?;
        let admitted = load_bound_request(&transaction, &command.binding)?;

        if let Some(existing) = load_withdrawal_receipt(&transaction, command.receipt_id)? {
            if !withdrawal_matches_command(&existing, command, &actor) {
                return Err(OperationsError::Conflict(
                    "temporary dispatch withdrawal receipt replay",
                ));
            }
            transaction.commit()?;
            return Ok(with_withdrawal_replay(existing));
        }
        if lifecycle_receipt_id_exists(&transaction, command.receipt_id)? {
            return Err(OperationsError::Conflict(
                "temporary dispatch lifecycle receipt identity",
            ));
        }

        let consent = load_consent_receipt(&transaction, command.consent_receipt_id)?.ok_or(
            OperationsError::Conflict("temporary dispatch consent receipt missing"),
        )?;
        if consent.request_id() != command.binding.request_id
            || consent.intent_digest() != command.binding.intent_digest
            || !same_logical_actor(&consent.actor, &actor)
        {
            return Err(OperationsError::Conflict(
                "temporary dispatch consent withdrawal authority",
            ));
        }
        if let Some(existing) =
            load_withdrawal_for_consent(&transaction, command.consent_receipt_id)?
        {
            if existing.receipt_id == command.receipt_id
                && withdrawal_matches_command(&existing, command, &actor)
            {
                transaction.commit()?;
                return Ok(with_withdrawal_replay(existing));
            }
            return Err(OperationsError::Conflict(
                "temporary dispatch consent already withdrawn",
            ));
        }

        let (current_receipt_id, current_revision) =
            current_consent_pointer(&transaction, command.binding.request_id, consent.kind)?
                .ok_or(OperationsError::Conflict(
                    "temporary dispatch current consent missing",
                ))?;
        if current_receipt_id != Some(command.consent_receipt_id)
            || current_revision != command.expected_revision
        {
            return Err(OperationsError::Conflict(
                "temporary dispatch consent withdrawal revision",
            ));
        }
        let authority_revision =
            authorize_consent(&transaction, &command.binding, consent.kind, &actor)?;
        if authority_revision != consent.authority_revision {
            return Err(OperationsError::Conflict(
                "temporary dispatch consent authority changed",
            ));
        }
        let revision = next_revision(current_revision, "temporary dispatch consent revision")?;
        let recorded_at = trusted_now()?;
        let receipt = TemporaryDispatchConsentWithdrawalReceipt {
            receipt_id: command.receipt_id,
            consent_receipt_id: command.consent_receipt_id,
            binding: command.binding.clone(),
            kind: consent.kind,
            revision,
            actor: actor.clone(),
            authority_revision,
            recorded_at,
            replayed: false,
        };
        persist_withdrawal_receipt(&transaction, command, &receipt, current_revision)?;
        let commit_result = transaction.commit();
        commit_withdrawal_write(
            commit_result,
            &self.connection,
            receipt,
            command,
            &admitted,
            &actor,
        )
    }

    /// Read an exact consent receipt as its authenticated logical author.
    ///
    /// Readback permits the author's current Session after a rebind; it does not
    /// infer the original consent remains valid under the current authority.
    ///
    /// # Errors
    /// Rejects an invalid Workspace actor, foreign request binding, or corrupt data.
    pub fn read_temporary_dispatch_consent(
        &mut self,
        binding: &TemporaryDispatchAdmissionBinding,
        receipt_id: Uuid,
        actor: OperationalActor,
    ) -> Result<Option<TemporaryDispatchConsentReceipt>, OperationsError> {
        if receipt_id.is_nil() || !super::actor_input_is_valid(actor) {
            return Err(OperationsError::Conflict(
                "temporary dispatch consent readback identity",
            ));
        }
        let transaction = self.connection.unchecked_transaction()?;
        teams::ensure_verified_workspace(&transaction, binding.workspace_id)?;
        let resolved_actor = resolve_actor(&transaction, binding.workspace_id, actor)?;
        let reader = resolve_actor_provenance(actor, &resolved_actor)?;
        let _admitted = load_bound_request(&transaction, binding)?;
        let receipt = load_consent_receipt(&transaction, receipt_id)?;
        let receipt = receipt.filter(|receipt| {
            receipt.binding == *binding && same_logical_actor(&receipt.actor, &reader)
        });
        catalog::commit_read(transaction, self.destination)?;
        Ok(receipt)
    }

    /// Read an exact withdrawal receipt as its authenticated logical author.
    ///
    /// # Errors
    /// Rejects an invalid Workspace actor, foreign request binding, or corrupt data.
    pub fn read_temporary_dispatch_consent_withdrawal(
        &mut self,
        binding: &TemporaryDispatchAdmissionBinding,
        receipt_id: Uuid,
        consent_receipt_id: Uuid,
        actor: OperationalActor,
    ) -> Result<Option<TemporaryDispatchConsentWithdrawalReceipt>, OperationsError> {
        if receipt_id.is_nil() || consent_receipt_id.is_nil() || !super::actor_input_is_valid(actor)
        {
            return Err(OperationsError::Conflict(
                "temporary dispatch withdrawal readback identity",
            ));
        }
        let transaction = self.connection.unchecked_transaction()?;
        teams::ensure_verified_workspace(&transaction, binding.workspace_id)?;
        let resolved_actor = resolve_actor(&transaction, binding.workspace_id, actor)?;
        let reader = resolve_actor_provenance(actor, &resolved_actor)?;
        let _admitted = load_bound_request(&transaction, binding)?;
        let receipt = load_withdrawal_receipt(&transaction, receipt_id)?;
        let receipt = receipt.filter(|receipt| {
            receipt.receipt_id == receipt_id
                && receipt.consent_receipt_id == consent_receipt_id
                && receipt.binding == *binding
                && same_logical_actor(&receipt.actor, &reader)
        });
        catalog::commit_read(transaction, self.destination)?;
        Ok(receipt)
    }

    /// Recover a withdrawal's admission binding as its original logical author
    /// after a role change. This read never preserves the consent's current
    /// authority or changes an admitted task.
    ///
    /// # Errors
    /// Rejects invalid identities, unverified Workspace, foreign/closed
    /// Session, or corrupt durable rows. A non-author receives `None`.
    pub fn temporary_dispatch_consent_withdrawal_readback_binding(
        &mut self,
        workspace_id: WorkspaceId,
        request_id: Uuid,
        receipt_id: Uuid,
        consent_receipt_id: Uuid,
        kind: TemporaryDispatchConsentKind,
        actor: OperationalActor,
    ) -> Result<Option<TemporaryDispatchAdmissionBinding>, OperationsError> {
        if request_id.is_nil()
            || receipt_id.is_nil()
            || consent_receipt_id.is_nil()
            || !super::actor_input_is_valid(actor)
        {
            return Err(OperationsError::Conflict(
                "temporary dispatch withdrawal readback binding identity",
            ));
        }
        let transaction = self.connection.unchecked_transaction()?;
        teams::ensure_verified_workspace(&transaction, workspace_id)?;
        let resolved_actor = resolve_actor(&transaction, workspace_id, actor)?;
        let reader = resolve_actor_provenance(actor, &resolved_actor)?;
        let record = super::super::load_request(&transaction, request_id)?;
        let binding = record
            .filter(|record| record.workspace_id() == workspace_id)
            .map(|record| TemporaryDispatchAdmissionBinding::from_record(&record));
        let result = if let Some(binding) = binding {
            let receipt = load_withdrawal_receipt(&transaction, receipt_id)?;
            receipt
                .filter(|receipt| {
                    receipt.request_id() == request_id
                        && receipt.consent_receipt_id() == consent_receipt_id
                        && receipt.kind() == kind
                        && receipt.binding == binding
                        && same_logical_actor(&receipt.actor, &reader)
                })
                .map(|_| binding)
        } else {
            None
        };
        catalog::commit_read(transaction, self.destination)?;
        Ok(result)
    }
}

fn authorize_consent(
    transaction: &Transaction<'_>,
    binding: &TemporaryDispatchAdmissionBinding,
    kind: TemporaryDispatchConsentKind,
    actor: &super::TemporaryDispatchActorProvenance,
) -> Result<u64, OperationsError> {
    let current_home = super::super::resolve_target_home_team(
        transaction,
        binding.workspace_id,
        binding.requesting_team_id,
        binding.target_participant_id,
    )?;
    if current_home != binding.target_home_team_id {
        return Err(OperationsError::Conflict(
            "temporary dispatch target home Team changed",
        ));
    }
    match kind {
        TemporaryDispatchConsentKind::TargetAcceptance => {
            let membership = target_membership_authority(transaction, binding)?;
            if actor.participation_id().into_uuid() != membership.participation_id
                || actor.profile_id() != membership.profile_id
            {
                return Err(OperationsError::Conflict(
                    "temporary dispatch target participation identity changed",
                ));
            }
            if authority_has_identity(
                transaction,
                binding.workspace_id,
                Some(binding.target_home_team_id),
                membership.profile_id,
                membership.participation_id,
            )? || authority_has_identity(
                transaction,
                binding.workspace_id,
                None,
                membership.profile_id,
                membership.participation_id,
            )? {
                return Err(OperationsError::Conflict(
                    "temporary dispatch target is an organization leader",
                ));
            }
            Ok(membership.revision)
        }
        TemporaryDispatchConsentKind::HomeLeaderConsent => {
            let authority = current_authority(
                transaction,
                binding.workspace_id,
                Some(binding.target_home_team_id),
            )?
            .ok_or(OperationsError::Conflict(
                "temporary dispatch home Team has no current Leader",
            ))?;
            let (leader_participation, leader_profile) = participant_identity(
                transaction,
                binding.workspace_id,
                authority.participant_id(),
            )?;
            if actor.participation_id().into_uuid() != leader_participation
                || actor.profile_id() != leader_profile
            {
                return Err(OperationsError::Conflict(
                    "temporary dispatch home Leader actor",
                ));
            }
            positive_revision(
                authority.leadership_revision(),
                "temporary dispatch home leadership revision",
            )
        }
        TemporaryDispatchConsentKind::WorkspaceOrchestratorApproval => {
            let authority = current_authority(transaction, binding.workspace_id, None)?.ok_or(
                OperationsError::Conflict("temporary dispatch Workspace Orchestrator missing"),
            )?;
            let (leader_participation, leader_profile) = participant_identity(
                transaction,
                binding.workspace_id,
                authority.participant_id(),
            )?;
            if actor.participation_id().into_uuid() != leader_participation
                || actor.profile_id() != leader_profile
            {
                return Err(OperationsError::Conflict(
                    "temporary dispatch Workspace Orchestrator actor",
                ));
            }
            positive_revision(
                authority.leadership_revision(),
                "temporary dispatch Workspace leadership revision",
            )
        }
    }
}

fn current_authority(
    transaction: &Transaction<'_>,
    workspace_id: WorkspaceId,
    team_id: Option<Uuid>,
) -> Result<Option<CurrentLeadershipAuthority>, OperationsError> {
    let scope = team_id.map_or_else(
        || LeadershipScope::workspace(workspace_id),
        |team| LeadershipScope::team(workspace_id, team),
    );
    crate::leadership::current_leadership_authority_in_transaction(transaction, scope)
}

pub(super) fn current_consent(
    transaction: &Transaction<'_>,
    binding: &TemporaryDispatchAdmissionBinding,
    kind: TemporaryDispatchConsentKind,
) -> Result<TemporaryDispatchConsentReceipt, OperationsError> {
    let (receipt_id, revision) = current_consent_pointer(transaction, binding.request_id, kind)?
        .and_then(|(receipt_id, revision)| receipt_id.map(|id| (id, revision)))
        .ok_or(OperationsError::Conflict(
            "temporary dispatch required consent missing",
        ))?;
    let receipt = load_consent_receipt(transaction, receipt_id)?.ok_or(
        OperationsError::Corrupt("temporary dispatch current consent receipt"),
    )?;
    if receipt.revision != revision
        || receipt.kind != kind
        || receipt.binding != *binding
        || receipt.binding.intent_digest != binding.intent_digest
        || load_withdrawal_for_consent(transaction, receipt_id)?.is_some()
    {
        return Err(OperationsError::Conflict(
            "temporary dispatch current consent invalidated",
        ));
    }
    let current_actor = current_logical_actor(
        transaction,
        binding.workspace_id,
        receipt.actor.profile_id(),
        receipt.actor.participation_id().into_uuid(),
    )?;
    if !same_logical_actor(&receipt.actor, &current_actor) {
        return Err(OperationsError::Conflict(
            "temporary dispatch consent logical actor changed",
        ));
    }
    let authority_revision = authorize_consent(transaction, binding, kind, &receipt.actor)?;
    if authority_revision != receipt.authority_revision {
        return Err(OperationsError::Conflict(
            "temporary dispatch consent authority changed",
        ));
    }
    Ok(receipt)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct TargetMembershipAuthority {
    pub(super) revision: u64,
    pub(super) participation_id: Uuid,
    pub(super) profile_id: Uuid,
}

pub(super) fn target_membership_authority(
    transaction: &Transaction<'_>,
    binding: &TemporaryDispatchAdmissionBinding,
) -> Result<TargetMembershipAuthority, OperationsError> {
    let (count, revision, participation, profile):
        (i64, i64, Option<Vec<u8>>, Option<Vec<u8>>) = transaction.query_row(
        "SELECT COUNT(*),COALESCE(MIN(member.revision),0),MIN(member.participation_id),MIN(participation.agent_profile_id) FROM ops_team_members AS member JOIN ops_workspace_participations AS participation ON participation.workspace_id=member.workspace_id AND participation.participation_id=member.participation_id WHERE member.workspace_id=?1 AND member.team_id=?2 AND member.state='JOINED' AND (SELECT COUNT(*) FROM ops_workspace_participation_mappings AS target_mapping WHERE target_mapping.workspace_id=member.workspace_id AND target_mapping.participant_id=?3)=1 AND EXISTS(SELECT 1 FROM ops_workspace_participation_mappings AS target_mapping WHERE target_mapping.workspace_id=member.workspace_id AND target_mapping.participant_id=?3 AND target_mapping.participation_id=member.participation_id AND target_mapping.agent_profile_id=participation.agent_profile_id)",
        params![
            binding.workspace_id.into_uuid().as_bytes(),
            binding.target_home_team_id.as_bytes(),
            binding.target_participant_id.as_bytes(),
        ],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
    )?;
    if count != 1 {
        return Err(OperationsError::Conflict(
            "temporary dispatch target home membership identity",
        ));
    }
    Ok(TargetMembershipAuthority {
        revision: positive_revision(revision, "temporary dispatch target membership revision")?,
        participation_id: uuid_from_bytes(
            participation.as_deref().ok_or(OperationsError::Corrupt(
                "temporary dispatch target membership participation",
            ))?,
            "temporary dispatch target membership participation",
        )?,
        profile_id: uuid_from_bytes(
            profile.as_deref().ok_or(OperationsError::Corrupt(
                "temporary dispatch target membership Profile",
            ))?,
            "temporary dispatch target membership Profile",
        )?,
    })
}

pub(super) fn load_bound_request(
    connection: &Connection,
    binding: &TemporaryDispatchAdmissionBinding,
) -> Result<super::TemporaryDispatchRequestRecord, OperationsError> {
    let request = super::super::load_request(connection, binding.request_id)?.ok_or(
        OperationsError::Conflict("temporary dispatch admission receipt missing"),
    )?;
    if !binding.matches_record(&request) {
        return Err(OperationsError::Conflict(
            "temporary dispatch admission binding",
        ));
    }
    Ok(request)
}

pub(super) fn current_logical_actor(
    connection: &Connection,
    workspace_id: WorkspaceId,
    profile_id: Uuid,
    participation_id: Uuid,
) -> Result<super::TemporaryDispatchActorProvenance, OperationsError> {
    let mut statement = connection.prepare(
        "WITH current_sessions AS (SELECT mapping.participant_id,mapping.session_id FROM ops_workspace_participation_mappings AS mapping WHERE mapping.workspace_id=?1 AND mapping.participation_id=?2 AND mapping.agent_profile_id=?3 UNION SELECT rebind.participant_id,rebind.new_session_id AS session_id FROM ops_workspace_participation_session_rebinds AS rebind JOIN ops_workspace_participations AS participation ON participation.workspace_id=rebind.workspace_id AND participation.participation_id=rebind.participation_id AND participation.agent_profile_id=rebind.agent_profile_id AND participation.revision=rebind.result_participation_revision WHERE rebind.workspace_id=?1 AND rebind.participation_id=?2 AND rebind.agent_profile_id=?3) SELECT binding.activity_id,binding.agent_id,binding.session_id FROM current_sessions AS current JOIN ops_logical_members AS member ON member.workspace_id=?1 AND member.participant_id=current.participant_id AND member.session_id=current.session_id JOIN ops_current_effect_bindings AS binding ON binding.workspace_id=member.workspace_id AND binding.participant_id=member.participant_id AND binding.session_id=member.session_id ORDER BY binding.activity_id,binding.agent_id LIMIT 65",
    )?;
    let mut rows = statement.query(params![
        workspace_id.into_uuid().as_bytes(),
        participation_id.as_bytes(),
        profile_id.as_bytes(),
    ])?;
    let mut resolved = None;
    let mut candidates = 0_usize;
    while let Some(row) = rows.next()? {
        candidates += 1;
        if candidates > 64 {
            return Err(OperationsError::Bounds(
                "temporary dispatch current Profile bindings",
            ));
        }
        let actor_input = OperationalActor::Participant {
            activity_id: ActivityId::from_uuid(uuid_from_bytes(
                &row.get::<_, Vec<u8>>(0)?,
                "temporary dispatch current Activity",
            )?),
            agent_id: AgentInstanceId::from_uuid(uuid_from_bytes(
                &row.get::<_, Vec<u8>>(1)?,
                "temporary dispatch current Agent",
            )?),
            session_id: AgentSessionId::from_uuid(uuid_from_bytes(
                &row.get::<_, Vec<u8>>(2)?,
                "temporary dispatch current Session",
            )?),
        };
        let actor = match resolve_actor(connection, workspace_id, actor_input)
            .and_then(|value| resolve_actor_provenance(actor_input, &value))
        {
            Ok(actor)
                if actor.profile_id() == profile_id
                    && actor.participation_id().into_uuid() == participation_id =>
            {
                actor
            }
            Ok(_) | Err(OperationsError::Conflict(_)) => continue,
            Err(error) => return Err(error),
        };
        if resolved.is_none() {
            resolved = Some(actor);
        }
    }
    resolved.ok_or(OperationsError::Conflict(
        "temporary dispatch logical actor has no current Profile binding",
    ))
}

pub(super) fn participant_identity(
    connection: &Connection,
    workspace_id: WorkspaceId,
    participant_id: Uuid,
) -> Result<(Uuid, Uuid), OperationsError> {
    let (count, participation, profile): (i64, Option<Vec<u8>>, Option<Vec<u8>>) =
        connection.query_row(
            "SELECT COUNT(*),MIN(mapping.participation_id),MIN(mapping.agent_profile_id) FROM ops_workspace_participation_mappings AS mapping JOIN ops_workspace_participations AS participation ON participation.workspace_id=mapping.workspace_id AND participation.participation_id=mapping.participation_id AND participation.agent_profile_id=mapping.agent_profile_id WHERE mapping.workspace_id=?1 AND mapping.participant_id=?2",
            params![workspace_id.into_uuid().as_bytes(), participant_id.as_bytes()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )?;
    if count != 1 {
        return Err(OperationsError::Conflict(
            "temporary dispatch participant Profile identity is missing or ambiguous",
        ));
    }
    Ok((
        uuid_from_bytes(
            participation.as_deref().ok_or(OperationsError::Corrupt(
                "temporary dispatch participant participation identity",
            ))?,
            "temporary dispatch participant participation identity",
        )?,
        uuid_from_bytes(
            profile.as_deref().ok_or(OperationsError::Corrupt(
                "temporary dispatch participant Profile identity",
            ))?,
            "temporary dispatch participant Profile identity",
        )?,
    ))
}

fn authority_has_identity(
    transaction: &Transaction<'_>,
    workspace_id: WorkspaceId,
    team_id: Option<Uuid>,
    profile_id: Uuid,
    participation_id: Uuid,
) -> Result<bool, OperationsError> {
    let Some(authority) = current_authority(transaction, workspace_id, team_id)? else {
        return Ok(false);
    };
    let (leader_participation, leader_profile) =
        participant_identity(transaction, workspace_id, authority.participant_id())?;
    Ok(leader_profile == profile_id && leader_participation == participation_id)
}

fn same_logical_actor(
    left: &super::TemporaryDispatchActorProvenance,
    right: &super::TemporaryDispatchActorProvenance,
) -> bool {
    left.participation_id() == right.participation_id() && left.profile_id() == right.profile_id()
}

fn current_consent_revision(
    transaction: &Transaction<'_>,
    request_id: Uuid,
    kind: TemporaryDispatchConsentKind,
) -> Result<u64, OperationsError> {
    let revision = transaction
        .query_row(
            "SELECT revision FROM ops_temporary_dispatch_current_consents WHERE request_id=?1 AND consent_kind=?2",
            params![request_id.as_bytes(), kind.as_str()],
            |row| row.get::<_, i64>(0),
        )
        .optional()?;
    revision.map_or(Ok(0), |revision| {
        nonnegative_revision(revision, "temporary dispatch consent revision")
    })
}

fn current_consent_pointer(
    transaction: &Transaction<'_>,
    request_id: Uuid,
    kind: TemporaryDispatchConsentKind,
) -> Result<Option<(Option<Uuid>, u64)>, OperationsError> {
    transaction
        .query_row(
            "SELECT receipt_id,revision FROM ops_temporary_dispatch_current_consents WHERE request_id=?1 AND consent_kind=?2",
            params![request_id.as_bytes(), kind.as_str()],
            |row| Ok((row.get::<_, Option<Vec<u8>>>(0)?, row.get::<_, i64>(1)?)),
        )
        .optional()?
        .map(|(receipt_id, revision)| {
            let receipt_id = receipt_id
                .map(|bytes| uuid_from_bytes(&bytes, "temporary dispatch consent pointer"))
                .transpose()?;
            Ok((
                receipt_id,
                positive_revision(revision, "temporary dispatch consent revision")?,
            ))
        })
        .transpose()
}

pub(super) fn lifecycle_receipt_id_exists(
    transaction: &Transaction<'_>,
    receipt_id: Uuid,
) -> Result<bool, OperationsError> {
    transaction
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM ops_temporary_dispatch_consent_receipts WHERE receipt_id=?1 UNION ALL SELECT 1 FROM ops_temporary_dispatch_consent_withdrawals WHERE receipt_id=?1 UNION ALL SELECT 1 FROM ops_temporary_dispatch_reservation_receipts WHERE receipt_id=?1)",
            params![receipt_id.as_bytes()],
            |row| row.get(0),
        )
        .map_err(OperationsError::from)
}

pub(super) fn load_consent_receipt(
    connection: &Connection,
    receipt_id: Uuid,
) -> Result<Option<TemporaryDispatchConsentReceipt>, OperationsError> {
    let raw = connection
        .query_row(
            "SELECT request_id,consent_kind,revision,intent_digest,actor_activity_id,actor_agent_id,actor_session_id,actor_participation_id,actor_profile_id,actor_participant_id,actor_display_name,authority_revision,recorded_at_ms FROM ops_temporary_dispatch_consent_receipts WHERE receipt_id=?1",
            params![receipt_id.as_bytes()],
            |row| {
                Ok((
                    row.get::<_, Vec<u8>>(0)?, row.get::<_, String>(1)?, row.get::<_, i64>(2)?,
                    row.get::<_, Vec<u8>>(3)?, row.get::<_, Vec<u8>>(4)?, row.get::<_, Vec<u8>>(5)?,
                    row.get::<_, Vec<u8>>(6)?, row.get::<_, Vec<u8>>(7)?, row.get::<_, Vec<u8>>(8)?,
                    row.get::<_, Vec<u8>>(9)?, row.get::<_, String>(10)?, row.get::<_, i64>(11)?,
                    row.get::<_, i64>(12)?,
                ))
            },
        )
        .optional()?;
    let Some((
        request_id,
        kind,
        revision,
        intent_digest,
        activity,
        agent,
        session,
        participation,
        profile,
        participant,
        display_name,
        authority_revision,
        recorded_at,
    )) = raw
    else {
        return Ok(None);
    };
    let request_id = uuid_from_bytes(&request_id, "temporary dispatch consent request")?;
    let request = super::super::load_request(connection, request_id)?.ok_or(
        OperationsError::Corrupt("temporary dispatch consent admission receipt"),
    )?;
    let binding = TemporaryDispatchAdmissionBinding::from_record(&request);
    if intent_digest.as_slice() != binding.intent_digest.as_slice() {
        return Err(OperationsError::Corrupt(
            "temporary dispatch consent digest",
        ));
    }
    Ok(Some(TemporaryDispatchConsentReceipt {
        receipt_id,
        binding,
        kind: TemporaryDispatchConsentKind::decode(&kind)?,
        revision: positive_revision(revision, "temporary dispatch consent revision")?,
        actor: decode_actor(
            &activity,
            &agent,
            &session,
            &participation,
            &profile,
            &participant,
            display_name,
        )?,
        authority_revision: positive_revision(
            authority_revision,
            "temporary dispatch consent authority revision",
        )?,
        recorded_at: decode_timestamp(recorded_at, "temporary dispatch consent time")?,
        replayed: false,
    }))
}

fn load_withdrawal_receipt(
    connection: &Connection,
    receipt_id: Uuid,
) -> Result<Option<TemporaryDispatchConsentWithdrawalReceipt>, OperationsError> {
    let raw = connection
        .query_row(
            "SELECT consent_receipt_id,request_id,consent_kind,revision,intent_digest,actor_activity_id,actor_agent_id,actor_session_id,actor_participation_id,actor_profile_id,actor_participant_id,actor_display_name,authority_revision,recorded_at_ms FROM ops_temporary_dispatch_consent_withdrawals WHERE receipt_id=?1",
            params![receipt_id.as_bytes()],
            |row| {
                Ok((
                    row.get::<_, Vec<u8>>(0)?, row.get::<_, Vec<u8>>(1)?, row.get::<_, String>(2)?,
                    row.get::<_, i64>(3)?, row.get::<_, Vec<u8>>(4)?, row.get::<_, Vec<u8>>(5)?,
                    row.get::<_, Vec<u8>>(6)?, row.get::<_, Vec<u8>>(7)?, row.get::<_, Vec<u8>>(8)?,
                    row.get::<_, Vec<u8>>(9)?, row.get::<_, Vec<u8>>(10)?, row.get::<_, String>(11)?,
                    row.get::<_, i64>(12)?, row.get::<_, i64>(13)?,
                ))
            },
        )
        .optional()?;
    let Some((
        consent_receipt_id,
        request_id,
        kind,
        revision,
        intent_digest,
        activity,
        agent,
        session,
        participation,
        profile,
        participant,
        display_name,
        authority_revision,
        recorded_at,
    )) = raw
    else {
        return Ok(None);
    };
    let request_id = uuid_from_bytes(&request_id, "temporary dispatch withdrawal request")?;
    let request = super::super::load_request(connection, request_id)?.ok_or(
        OperationsError::Corrupt("temporary dispatch withdrawal admission receipt"),
    )?;
    let binding = TemporaryDispatchAdmissionBinding::from_record(&request);
    if intent_digest.as_slice() != binding.intent_digest.as_slice() {
        return Err(OperationsError::Corrupt(
            "temporary dispatch withdrawal digest",
        ));
    }
    Ok(Some(TemporaryDispatchConsentWithdrawalReceipt {
        receipt_id,
        consent_receipt_id: uuid_from_bytes(
            &consent_receipt_id,
            "temporary dispatch withdrawn consent",
        )?,
        binding,
        kind: TemporaryDispatchConsentKind::decode(&kind)?,
        revision: positive_revision(revision, "temporary dispatch withdrawal revision")?,
        actor: decode_actor(
            &activity,
            &agent,
            &session,
            &participation,
            &profile,
            &participant,
            display_name,
        )?,
        authority_revision: positive_revision(
            authority_revision,
            "temporary dispatch withdrawal authority revision",
        )?,
        recorded_at: decode_timestamp(recorded_at, "temporary dispatch withdrawal time")?,
        replayed: false,
    }))
}

fn load_withdrawal_for_consent(
    connection: &Connection,
    consent_receipt_id: Uuid,
) -> Result<Option<TemporaryDispatchConsentWithdrawalReceipt>, OperationsError> {
    let receipt_id = connection
        .query_row(
            "SELECT receipt_id FROM ops_temporary_dispatch_consent_withdrawals WHERE consent_receipt_id=?1",
            params![consent_receipt_id.as_bytes()],
            |row| row.get::<_, Vec<u8>>(0),
        )
        .optional()?;
    receipt_id
        .map(|bytes| {
            let id = uuid_from_bytes(&bytes, "temporary dispatch withdrawal receipt")?;
            load_withdrawal_receipt(connection, id)?.ok_or(OperationsError::Corrupt(
                "temporary dispatch withdrawal receipt index",
            ))
        })
        .transpose()
}

fn insert_consent_receipt(
    transaction: &Transaction<'_>,
    receipt: &TemporaryDispatchConsentReceipt,
) -> Result<(), OperationsError> {
    let actor = &receipt.actor;
    transaction.execute(
        "INSERT INTO ops_temporary_dispatch_consent_receipts(receipt_id,request_id,consent_kind,revision,intent_digest,actor_activity_id,actor_agent_id,actor_session_id,actor_participation_id,actor_profile_id,actor_participant_id,actor_display_name,authority_revision,recorded_at_ms) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14)",
        params![
            receipt.receipt_id.as_bytes(), receipt.binding.request_id.as_bytes(), receipt.kind.as_str(),
            to_i64(receipt.revision, "temporary dispatch consent revision")?,
            receipt.binding.intent_digest.as_slice(), actor.activity_id().into_uuid().as_bytes(),
            actor.agent_id().into_uuid().as_bytes(), actor.session_id().into_uuid().as_bytes(),
            actor.participation_id().into_uuid().as_bytes(), actor.profile_id().as_bytes(),
            actor.participant_id().as_bytes(), actor.display_name(),
            to_i64(receipt.authority_revision, "temporary dispatch consent authority revision")?,
            receipt.recorded_at.get(),
        ],
    )?;
    Ok(())
}

fn insert_withdrawal_receipt(
    transaction: &Transaction<'_>,
    receipt: &TemporaryDispatchConsentWithdrawalReceipt,
) -> Result<(), OperationsError> {
    let actor = &receipt.actor;
    transaction.execute(
        "INSERT INTO ops_temporary_dispatch_consent_withdrawals(receipt_id,consent_receipt_id,request_id,consent_kind,revision,intent_digest,actor_activity_id,actor_agent_id,actor_session_id,actor_participation_id,actor_profile_id,actor_participant_id,actor_display_name,authority_revision,recorded_at_ms) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15)",
        params![
            receipt.receipt_id.as_bytes(), receipt.consent_receipt_id.as_bytes(),
            receipt.binding.request_id.as_bytes(), receipt.kind.as_str(),
            to_i64(receipt.revision, "temporary dispatch withdrawal revision")?,
            receipt.binding.intent_digest.as_slice(), actor.activity_id().into_uuid().as_bytes(),
            actor.agent_id().into_uuid().as_bytes(), actor.session_id().into_uuid().as_bytes(),
            actor.participation_id().into_uuid().as_bytes(), actor.profile_id().as_bytes(),
            actor.participant_id().as_bytes(), actor.display_name(),
            to_i64(receipt.authority_revision, "temporary dispatch withdrawal authority revision")?,
            receipt.recorded_at.get(),
        ],
    )?;
    Ok(())
}

fn persist_withdrawal_receipt(
    transaction: &Transaction<'_>,
    command: &TemporaryDispatchConsentWithdrawalCommand,
    receipt: &TemporaryDispatchConsentWithdrawalReceipt,
    expected_revision: u64,
) -> Result<(), OperationsError> {
    insert_withdrawal_receipt(transaction, receipt)?;
    let changed = transaction.execute(
        "UPDATE ops_temporary_dispatch_current_consents SET receipt_id=NULL,revision=?1 WHERE request_id=?2 AND consent_kind=?3 AND receipt_id=?4 AND revision=?5",
        params![
            to_i64(receipt.revision, "temporary dispatch consent revision")?,
            command.binding.request_id.as_bytes(),
            receipt.kind.as_str(),
            command.consent_receipt_id.as_bytes(),
            to_i64(expected_revision, "temporary dispatch consent revision")?,
        ],
    )?;
    if changed != 1 {
        return Err(OperationsError::Conflict(
            "temporary dispatch consent withdrawal revision",
        ));
    }
    Ok(())
}

fn commit_consent_write(
    commit_result: Result<(), rusqlite::Error>,
    connection: &Connection,
    receipt: TemporaryDispatchConsentReceipt,
    command: &TemporaryDispatchConsentCommand,
    admitted: &super::TemporaryDispatchRequestRecord,
    actor: &super::TemporaryDispatchActorProvenance,
) -> Result<TemporaryDispatchConsentReceipt, OperationsError> {
    match commit_result {
        Ok(()) => Ok(receipt),
        Err(source) => match load_consent_receipt(connection, command.receipt_id) {
            Ok(Some(existing)) if consent_matches_command(&existing, command, actor) => {
                Ok(with_consent_replay(existing))
            }
            Ok(Some(_)) => Err(OperationsError::Conflict(
                "temporary dispatch consent replay after uncertain commit",
            )),
            Ok(None) | Err(_) => Err(OperationsError::OperationCommitUnconfirmed {
                workspace_id: admitted.workspace_id(),
                request_id: command.receipt_id,
                source,
            }),
        },
    }
}

fn commit_withdrawal_write(
    commit_result: Result<(), rusqlite::Error>,
    connection: &Connection,
    receipt: TemporaryDispatchConsentWithdrawalReceipt,
    command: &TemporaryDispatchConsentWithdrawalCommand,
    admitted: &super::TemporaryDispatchRequestRecord,
    actor: &super::TemporaryDispatchActorProvenance,
) -> Result<TemporaryDispatchConsentWithdrawalReceipt, OperationsError> {
    match commit_result {
        Ok(()) => Ok(receipt),
        Err(source) => match load_withdrawal_receipt(connection, command.receipt_id) {
            Ok(Some(existing)) if withdrawal_matches_command(&existing, command, actor) => {
                Ok(with_withdrawal_replay(existing))
            }
            Ok(Some(_)) => Err(OperationsError::Conflict(
                "temporary dispatch withdrawal replay after uncertain commit",
            )),
            Ok(None) | Err(_) => Err(OperationsError::OperationCommitUnconfirmed {
                workspace_id: admitted.workspace_id(),
                request_id: command.receipt_id,
                source,
            }),
        },
    }
}

fn consent_matches_command(
    receipt: &TemporaryDispatchConsentReceipt,
    command: &TemporaryDispatchConsentCommand,
    actor: &super::TemporaryDispatchActorProvenance,
) -> bool {
    receipt.receipt_id == command.receipt_id
        && receipt.binding == command.binding
        && receipt.kind == command.kind
        && command.expected_revision.checked_add(1) == Some(receipt.revision)
        && same_actor(&receipt.actor, actor)
}

fn withdrawal_matches_command(
    receipt: &TemporaryDispatchConsentWithdrawalReceipt,
    command: &TemporaryDispatchConsentWithdrawalCommand,
    actor: &super::TemporaryDispatchActorProvenance,
) -> bool {
    receipt.receipt_id == command.receipt_id
        && receipt.consent_receipt_id == command.consent_receipt_id
        && receipt.binding == command.binding
        && command.expected_revision.checked_add(1) == Some(receipt.revision)
        && same_actor(&receipt.actor, actor)
}

fn with_consent_replay(
    mut receipt: TemporaryDispatchConsentReceipt,
) -> TemporaryDispatchConsentReceipt {
    receipt.replayed = true;
    receipt
}

fn with_withdrawal_replay(
    mut receipt: TemporaryDispatchConsentWithdrawalReceipt,
) -> TemporaryDispatchConsentWithdrawalReceipt {
    receipt.replayed = true;
    receipt
}

pub(super) fn decode_actor(
    activity: &[u8],
    agent: &[u8],
    session: &[u8],
    participation: &[u8],
    profile: &[u8],
    participant: &[u8],
    display_name: String,
) -> Result<super::TemporaryDispatchActorProvenance, OperationsError> {
    if display_name.is_empty() || display_name.len() > 128 {
        return Err(OperationsError::Corrupt(
            "temporary dispatch lifecycle actor display name",
        ));
    }
    Ok(super::TemporaryDispatchActorProvenance {
        activity_id: ActivityId::from_uuid(uuid_from_bytes(
            activity,
            "temporary dispatch lifecycle actor Activity",
        )?),
        agent_id: AgentInstanceId::from_uuid(uuid_from_bytes(
            agent,
            "temporary dispatch lifecycle actor Agent",
        )?),
        session_id: AgentSessionId::from_uuid(uuid_from_bytes(
            session,
            "temporary dispatch lifecycle actor Session",
        )?),
        participation_id: crate::WorkspaceParticipationId::from_uuid(uuid_from_bytes(
            participation,
            "temporary dispatch lifecycle actor participation",
        )?)
        .map_err(|_| {
            OperationsError::Corrupt("temporary dispatch lifecycle actor participation")
        })?,
        profile_id: uuid_from_bytes(profile, "temporary dispatch lifecycle actor Profile")?,
        participant_id: uuid_from_bytes(
            participant,
            "temporary dispatch lifecycle actor participant",
        )?,
        display_name,
    })
}

fn positive_revision(value: i64, label: &'static str) -> Result<u64, OperationsError> {
    u64::try_from(value)
        .ok()
        .filter(|revision| *revision > 0)
        .ok_or(OperationsError::Corrupt(label))
}

fn nonnegative_revision(value: i64, label: &'static str) -> Result<u64, OperationsError> {
    u64::try_from(value).map_err(|_| OperationsError::Corrupt(label))
}

fn next_revision(value: u64, label: &'static str) -> Result<u64, OperationsError> {
    value.checked_add(1).ok_or(OperationsError::Bounds(label))
}

fn to_i64(value: u64, label: &'static str) -> Result<i64, OperationsError> {
    i64::try_from(value).map_err(|_| OperationsError::Bounds(label))
}

fn decode_timestamp(value: i64, label: &'static str) -> Result<TimestampMillis, OperationsError> {
    TimestampMillis::new(value).map_err(|_| OperationsError::Corrupt(label))
}

fn uuid_from_bytes(value: &[u8], label: &'static str) -> Result<Uuid, OperationsError> {
    Uuid::from_slice(value).map_err(|_| OperationsError::Corrupt(label))
}
