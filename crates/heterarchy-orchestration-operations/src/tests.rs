use std::error::Error;
use std::fs;
use std::path::{Path, PathBuf};

use forge_domain::{
    ActivityCoordinatorActor, ActivityCoordinatorRevision, ActivityId, AgentBindingRejectionReason,
    AgentDeclaredRole, AgentDisplayName, AgentEventId, AgentHandoff, AgentInstanceId,
    AgentInstanceRevision, AgentPresenceRevision, AgentSessionId, AgentSessionOpenRequestId,
    AgentTransportKind, AuthenticatedPrincipal, BootstrapActivity, ClaimActivityCoordinator,
    CoordinationSnapshot, DetachAgent, DirectTokenId, EstablishWorkspace, InboxReceiptRevision,
    LocationId, OpenAgentSession, RegisteredAgentSession, RuntimeInstanceId, TimestampMillis,
    WorkspaceId, WorkspaceRevision, WorkspaceRoot, WorkspaceRootFingerprint,
};
use forge_persistence::{
    AgentBindingActivationRequest, AgentBindingReservationRequest, AgentDepartureCommand,
    AgentDepartureReconciliationRequest, AgentOperationsDelegation,
    AgentOperationsExportCursor as Cursor, AgentOperationsExportPage,
    AgentOperationsExportRecord as Record, AgentOperationsFreeze, AgentOperationsFreezeRequest,
    WorkspaceStore,
};
use forge_workspace::WorkspaceStateLayout;
use heterarchy_orchestration_sessions::{
    AgentProfileCatalog, AgentProfileCreationOrigin, AgentProfileId,
    AgentProfileLogicalSessionLink, AgentProfileProviderFamily, AgentSessionCatalog,
    CreateAgentProfile, link_existing_logical_session,
};
use rusqlite::{Connection, OptionalExtension, params};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::{
    BindingActivationObservation, BindingRejectionAttemptDisposition,
    BindingRejectionAttemptRequest, BindingRejectionObservation,
    BindingRejectionSourceRequestPolicy, BindingReservationObservation, ImportPhase,
    ImportProgress, LegacyBindingRejectionEnvelopePolicy, LogicalDepartureKind,
    LogicalDetachCommand, LogicalHandoffCommand, LogicalInboxAck, LogicalInboxEntry,
    LogicalInboxQuery, LogicalInboxTarget, LogicalPresenceTarget, LogicalPresenceTouch,
    OperationsCatalog, OperationsError, RegistrationPhase, RegistrationRequest,
    ResolveBindingRejectionAttempt, TrustedLegacyBindingRejectionDeployment,
    WorkspaceParticipationId, WorkspaceParticipationReplacementActivationReceipt,
    WorkspaceParticipationReplacementCommitReceipt, WorkspaceParticipationSessionRebindCommand,
};

#[test]
#[allow(
    clippy::too_many_lines,
    reason = "Keeps attempt custody, terminal replay, activation exclusion, and reopen evidence in one end-to-end regression."
)]
fn binding_rejection_attempt_is_readback_only_and_preserves_membership_history()
-> Result<(), Box<dyn Error>> {
    let destination = Uuid::from_u128(0xEB00);
    let seed = 0xEB10_u128;
    let mut source = source_fixture(seed, 1, false, destination)?;
    let activity_id = ActivityId::from_uuid(Uuid::from_u128(seed + 2));
    let (catalog_root, catalog_path) = catalog_root(seed)?;
    let mut catalog = OperationsCatalog::open(&catalog_path, destination)?;
    let _prepared = catalog.prepare(&source.freeze)?;
    import_all(&mut catalog, &source)?;
    verify_all(&mut catalog, &source)?;

    let principal =
        AuthenticatedPrincipal::DirectToken(DirectTokenId::from_uuid(Uuid::from_u128(0xEB20)));
    let binding_request_id = Uuid::parse_str("00000000-0000-8000-8000-00000000eb21")?;
    let binding = AgentBindingReservationRequest::new(
        binding_request_id,
        activity_id,
        AgentInstanceId::from_uuid(Uuid::from_u128(0xEB22)),
        AgentSessionId::from_uuid(Uuid::from_u128(0xEB23)),
        principal,
        timestamp(1_000)?,
    )?;
    let registration = RegistrationRequest::new(
        Uuid::from_u128(0xEB24),
        binding.clone(),
        AgentDisplayName::new("terminal worker".to_owned())?,
        AgentDeclaredRole::new("reviewer".to_owned())?,
        AgentTransportKind::Other,
    )?;
    let _pending = catalog.prepare_registration(source.migration, &registration)?;
    let reservation =
        source
            .store
            .reserve_agent_binding(&source.freeze, &binding, timestamp(1_100)?)?;
    let _reserved = catalog.record_registration_reservation(&reservation)?;
    let commit =
        catalog.commit_registration_membership(binding.request_id(), &[], timestamp(1_200)?)?;
    let import = catalog
        .progress(source.migration)?
        .ok_or("import missing")?;
    let attempt = BindingRejectionAttemptRequest::for_registration(
        source.workspace,
        Uuid::from_u128(0xEB25),
        destination,
        source.migration,
        source.freeze.revision(),
        import.digest(),
        &registration,
        commit.commit_digest(),
        timestamp(1_300)?,
    )?;
    assert_eq!(
        catalog.prepare_registration_binding_rejection_attempt(&attempt)?,
        BindingRejectionAttemptDisposition::DispatchAllowed
    );
    let later_replay = BindingRejectionAttemptRequest::for_registration(
        source.workspace,
        attempt.issuer_id(),
        destination,
        source.migration,
        source.freeze.revision(),
        import.digest(),
        &registration,
        commit.commit_digest(),
        timestamp(1_350)?,
    )?;
    assert_eq!(
        catalog.prepare_registration_binding_rejection_attempt(&attempt)?,
        BindingRejectionAttemptDisposition::ReadbackRequired,
        "exact replay after a possible dispatch must remain readback-only"
    );
    assert!(matches!(
        catalog.prepare_registration_binding_rejection_attempt(&later_replay),
        Err(OperationsError::Conflict(
            "binding rejection attempt replay"
        ))
    ));
    let attempt_before_resolution: Vec<rusqlite::types::Value> = catalog.connection.query_row(
        "SELECT * FROM ops_binding_rejection_attempts WHERE request_id=?1",
        params![binding.request_id().as_bytes()],
        |row| {
            (0..row.as_ref().column_count())
                .map(|index| row.get(index))
                .collect()
        },
    )?;
    drop(catalog);
    let legacy = Connection::open(&catalog_path)?;
    legacy.execute_batch(
        "DROP TABLE ops_agent_provision_uncertainty_retirements;
         DROP TABLE ops_binding_rejection_attempt_resolutions;",
    )?;
    crate::catalog::restore_v46_message_schema_for_legacy_fixture(&legacy)?;
    crate::catalog::restore_v47_assignment_schema_for_legacy_fixture(&legacy)?;
    legacy.pragma_update(None, "user_version", 44_i64)?;
    drop(legacy);
    let mut catalog = OperationsCatalog::open(&catalog_path, destination)?;
    assert_eq!(
        catalog
            .connection
            .pragma_query_value::<i64, _>(None, "user_version", |row| row.get(0))?,
        crate::catalog::VERSION
    );
    let migrated_attempt: Vec<rusqlite::types::Value> = catalog.connection.query_row(
        "SELECT * FROM ops_binding_rejection_attempts WHERE request_id=?1",
        params![binding.request_id().as_bytes()],
        |row| {
            (0..row.as_ref().column_count())
                .map(|index| row.get(index))
                .collect()
        },
    )?;
    assert_eq!(migrated_attempt, attempt_before_resolution);
    assert!(
        catalog
            .registration_binding_rejection_attempt_resolution(binding.request_id())?
            .is_none(),
        "a migrated uncertain attempt without an explicit resolution remains readback-only"
    );
    assert_eq!(
        catalog.prepare_registration_binding_rejection_attempt(&attempt)?,
        BindingRejectionAttemptDisposition::ReadbackRequired
    );
    let resolution_request_id = Uuid::from_u128(0xEB26);
    let resolution = ResolveBindingRejectionAttempt {
        request_id: binding.request_id(),
        resolution_request_id,
        expected_attempt_request_digest: attempt.request_digest(),
        expected_legacy_client_artifact_digest: [0x94; 32],
        source_get_observed_at: timestamp(1_360)?,
        operator_principal: principal,
        resolved_at: timestamp(1_361)?,
    };
    let trusted_deployment = TrustedLegacyBindingRejectionDeployment::new(
        [0x94; 32],
        LegacyBindingRejectionEnvelopePolicy::GeneratedUuidV7,
        8,
        BindingRejectionSourceRequestPolicy::BodyEqualsEnvelope,
    )?;
    let untrusted_deployment = TrustedLegacyBindingRejectionDeployment::new(
        [0x95; 32],
        LegacyBindingRejectionEnvelopePolicy::GeneratedUuidV7,
        8,
        BindingRejectionSourceRequestPolicy::BodyEqualsEnvelope,
    )?;
    assert!(matches!(
        catalog.resolve_registration_binding_rejection_attempt(&resolution, untrusted_deployment),
        Err(OperationsError::Conflict(
            "binding rejection resolution identity"
        ))
    ));
    let proof =
        catalog.resolve_registration_binding_rejection_attempt(&resolution, trusted_deployment)?;
    assert_eq!(
        proof.envelope_policy(),
        LegacyBindingRejectionEnvelopePolicy::GeneratedUuidV7
    );
    assert_eq!(proof.durable_uuid_version(), 8);
    assert_eq!(
        proof.source_request_policy(),
        BindingRejectionSourceRequestPolicy::BodyEqualsEnvelope
    );
    assert_eq!(
        catalog.resolve_registration_binding_rejection_attempt(&resolution, trusted_deployment)?,
        proof
    );
    let mut changed_resolution = resolution.clone();
    changed_resolution.expected_legacy_client_artifact_digest = [0x95; 32];
    assert!(matches!(
        catalog.resolve_registration_binding_rejection_attempt(
            &changed_resolution,
            trusted_deployment
        ),
        Err(OperationsError::Conflict(_))
    ));
    drop(catalog);
    let mut reopened = OperationsCatalog::open(&catalog_path, destination)?;
    assert_eq!(
        reopened.resolve_registration_binding_rejection_attempt(&resolution, trusted_deployment)?,
        proof
    );
    for (column, corrupt, original) in [
        (
            "attempt_request_digest",
            vec![0x96_u8; 32],
            attempt.request_digest().to_vec(),
        ),
        (
            "legacy_client_artifact_digest",
            vec![0x97_u8; 32],
            trusted_deployment.artifact_digest().to_vec(),
        ),
        (
            "operator_principal_id",
            Uuid::from_u128(0xEB27).as_bytes().to_vec(),
            Uuid::from_u128(0xEB20).as_bytes().to_vec(),
        ),
    ] {
        reopened.connection.execute(
            &format!("UPDATE ops_binding_rejection_attempt_resolutions SET {column}=?1 WHERE request_id=?2"),
            params![corrupt, binding.request_id().as_bytes()],
        )?;
        assert!(matches!(
            reopened.consume_registration_binding_rejection_resolution(
                binding.request_id(),
                principal,
                trusted_deployment,
                timestamp(1_362)?,
            ),
            Err(OperationsError::Corrupt(_))
        ));
        reopened.connection.execute(
            &format!("UPDATE ops_binding_rejection_attempt_resolutions SET {column}=?1 WHERE request_id=?2"),
            params![original, binding.request_id().as_bytes()],
        )?;
    }
    reopened
        .connection
        .pragma_update(None, "ignore_check_constraints", true)?;
    reopened.connection.execute(
        "UPDATE ops_binding_rejection_attempt_resolutions SET resolved_at_ms=1359 WHERE request_id=?1",
        params![binding.request_id().as_bytes()],
    )?;
    assert!(matches!(
        reopened.consume_registration_binding_rejection_resolution(
            binding.request_id(),
            principal,
            trusted_deployment,
            timestamp(1_362)?,
        ),
        Err(OperationsError::Corrupt(_))
    ));
    reopened.connection.execute(
        "UPDATE ops_binding_rejection_attempt_resolutions SET resolved_at_ms=1361,consume_request_id=?1,consumed_at_ms=1360 WHERE request_id=?2",
        params![resolution_request_id.as_bytes(), binding.request_id().as_bytes()],
    )?;
    assert!(matches!(
        reopened.consume_registration_binding_rejection_resolution(
            binding.request_id(),
            principal,
            trusted_deployment,
            timestamp(1_362)?,
        ),
        Err(OperationsError::Corrupt(_))
    ));
    reopened.connection.execute(
        "UPDATE ops_binding_rejection_attempt_resolutions SET consume_request_id=NULL,consumed_at_ms=NULL WHERE request_id=?1",
        params![binding.request_id().as_bytes()],
    )?;
    reopened.connection.execute(
        "UPDATE ops_binding_rejection_attempt_resolutions SET source_get_observed_at_ms=1299 WHERE request_id=?1",
        params![binding.request_id().as_bytes()],
    )?;
    assert!(matches!(
        reopened.consume_registration_binding_rejection_resolution(
            binding.request_id(),
            principal,
            trusted_deployment,
            timestamp(1_362)?,
        ),
        Err(OperationsError::Corrupt(_))
    ));
    reopened.connection.execute(
        "UPDATE ops_binding_rejection_attempt_resolutions SET source_get_observed_at_ms=1360,consume_request_id=?1,consumed_at_ms=1362 WHERE request_id=?2",
        params![Uuid::from_u128(0xEB28).as_bytes(), binding.request_id().as_bytes()],
    )?;
    assert!(matches!(
        reopened.consume_registration_binding_rejection_resolution(
            binding.request_id(),
            principal,
            trusted_deployment,
            timestamp(1_362)?,
        ),
        Err(OperationsError::Corrupt(_))
    ));
    reopened.connection.execute(
        "UPDATE ops_binding_rejection_attempt_resolutions SET consume_request_id=NULL,consumed_at_ms=NULL WHERE request_id=?1",
        params![binding.request_id().as_bytes()],
    )?;
    reopened
        .connection
        .pragma_update(None, "ignore_check_constraints", false)?;
    drop(reopened);

    let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
    let consumers = [1_362_i64, 1_363_i64].map(|at| {
        let path = catalog_path.clone();
        let barrier = std::sync::Arc::clone(&barrier);
        std::thread::spawn(move || -> Result<_, String> {
            let mut catalog =
                OperationsCatalog::open(&path, destination).map_err(|error| error.to_string())?;
            barrier.wait();
            catalog
                .consume_registration_binding_rejection_resolution(
                    binding_request_id,
                    principal,
                    trusted_deployment,
                    TimestampMillis::new(at).map_err(|error| error.to_string())?,
                )
                .map_err(|error| error.to_string())
        })
    });
    let mut dispositions = Vec::with_capacity(consumers.len());
    for consumer in consumers {
        dispositions.push(consumer.join().map_err(|_| "consumer panicked")??);
    }
    dispositions.sort_by_key(|disposition| match disposition {
        BindingRejectionAttemptDisposition::DispatchAllowed => 0,
        BindingRejectionAttemptDisposition::ReadbackRequired => 1,
        BindingRejectionAttemptDisposition::Recorded(_) => 2,
    });
    assert!(matches!(
        dispositions.as_slice(),
        [
            BindingRejectionAttemptDisposition::DispatchAllowed,
            BindingRejectionAttemptDisposition::ReadbackRequired
        ]
    ));
    let mut catalog = OperationsCatalog::open(&catalog_path, destination)?;
    catalog
        .connection
        .pragma_update(None, "ignore_check_constraints", true)?;
    for (column, corrupt, original) in [
        ("envelope_policy", 9_i64, 0_i64),
        ("durable_uuid_version", 7_i64, 8_i64),
        ("source_contract", 9_i64, 0_i64),
    ] {
        catalog.connection.execute(
            &format!("UPDATE ops_binding_rejection_attempt_resolutions SET {column}=?1 WHERE request_id=?2"),
            params![corrupt, binding.request_id().as_bytes()],
        )?;
        assert!(matches!(
            catalog.resolve_registration_binding_rejection_attempt(&resolution, trusted_deployment),
            Err(OperationsError::Corrupt(_))
        ));
        catalog.connection.execute(
            &format!("UPDATE ops_binding_rejection_attempt_resolutions SET {column}=?1 WHERE request_id=?2"),
            params![original, binding.request_id().as_bytes()],
        )?;
    }
    catalog
        .connection
        .pragma_update(None, "ignore_check_constraints", false)?;
    let attempt_after_resolution: Vec<rusqlite::types::Value> = catalog.connection.query_row(
        "SELECT * FROM ops_binding_rejection_attempts WHERE request_id=?1",
        params![binding.request_id().as_bytes()],
        |row| {
            (0..row.as_ref().column_count())
                .map(|index| row.get(index))
                .collect()
        },
    )?;
    assert_eq!(attempt_after_resolution, attempt_before_resolution);

    let root_fingerprint = WorkspaceRootFingerprint::from_bytes([0x31; 16]);
    let binding_digest = [0x32; 32];
    let delegation_digest = [0x33; 32];
    let observed_at = timestamp(1_400)?;
    let rejected_at = timestamp(1_401)?;
    let mut receipt_hasher = Sha256::new();
    receipt_hasher.update(b"forge.agent.binding.rejection.receipt.v1\0");
    receipt_hasher.update(attempt.request_digest());
    receipt_hasher.update(binding.request_id().as_bytes());
    receipt_hasher.update(source.workspace.into_uuid().as_bytes());
    receipt_hasher.update(binding.activity_id().into_uuid().as_bytes());
    receipt_hasher.update(binding.agent_id().into_uuid().as_bytes());
    receipt_hasher.update(binding.session_id().into_uuid().as_bytes());
    receipt_hasher.update([1]);
    let AuthenticatedPrincipal::DirectToken(token_id) = principal else {
        return Err("expected DirectToken".into());
    };
    receipt_hasher.update(token_id.into_uuid().as_bytes());
    receipt_hasher.update(source.migration.as_bytes());
    receipt_hasher.update(destination.as_bytes());
    receipt_hasher.update(source.freeze.revision().to_le_bytes());
    receipt_hasher.update(import.digest());
    receipt_hasher.update(attempt.issuer_id().as_bytes());
    receipt_hasher.update(root_fingerprint.as_bytes());
    receipt_hasher.update(binding_digest);
    receipt_hasher.update(delegation_digest);
    receipt_hasher.update(commit.commit_digest());
    for text in [
        registration.display_name().as_str(),
        registration.declared_role().as_str(),
    ] {
        receipt_hasher.update((text.len() as u64).to_le_bytes());
        receipt_hasher.update(text.as_bytes());
    }
    receipt_hasher.update([3]);
    receipt_hasher.update(reservation.reserved_at().get().to_le_bytes());
    receipt_hasher.update(binding.requested_at().get().to_le_bytes());
    receipt_hasher.update(observed_at.get().to_le_bytes());
    receipt_hasher.update(rejected_at.get().to_le_bytes());
    let receipt_digest: [u8; 32] = receipt_hasher.finalize().into();
    let observation = BindingRejectionObservation::new(
        binding.request_id(),
        source.workspace,
        binding.activity_id(),
        binding.agent_id(),
        binding.session_id(),
        binding.principal(),
        source.migration,
        destination,
        source.freeze.revision(),
        import.digest(),
        attempt.issuer_id(),
        root_fingerprint,
        binding_digest,
        delegation_digest,
        commit.commit_digest(),
        registration.display_name().clone(),
        registration.declared_role().clone(),
        registration.transport(),
        AgentBindingRejectionReason::PrincipalTransportMismatch,
        reservation.reserved_at(),
        binding.requested_at(),
        observed_at,
        rejected_at,
        attempt.request_digest(),
        receipt_digest,
    )?;
    let receipt = catalog.record_registration_binding_rejection_observation(&observation)?;
    assert_eq!(receipt.membership_digest(), commit.commit_digest());
    assert_eq!(receipt.binding(), &binding);
    assert_eq!(
        catalog.record_registration_binding_rejection_observation(&observation)?,
        receipt
    );
    assert_eq!(
        catalog.prepare_registration_binding_rejection_attempt(&attempt)?,
        BindingRejectionAttemptDisposition::Recorded(Box::new(receipt.clone()))
    );
    assert_eq!(
        catalog
            .registration_progress(binding.request_id())?
            .ok_or("registration missing")?
            .phase(),
        RegistrationPhase::MembershipCommitted
    );
    assert!(
        catalog
            .registration_activation(binding.request_id())?
            .is_none()
    );

    let activation = BindingActivationObservation::new(
        BindingReservationObservation::from_native(&reservation)?,
        commit.commit_digest(),
        registration.display_name().clone(),
        registration.declared_role().clone(),
        registration.transport(),
        timestamp(1_500)?,
    )?;
    assert!(matches!(
        catalog.record_registration_activation_observation(&activation),
        Err(OperationsError::Conflict(
            "binding rejection already recorded"
        ))
    ));

    drop(catalog);
    let reopened = OperationsCatalog::open(&catalog_path, destination)?;
    assert_eq!(
        reopened.registration_binding_rejection(binding.request_id())?,
        Some(receipt.clone())
    );
    drop(reopened);

    for (column, corrupt, original) in [
        (
            "request_digest",
            vec![0x91_u8; 32],
            receipt.request_digest().to_vec(),
        ),
        (
            "receipt_digest",
            vec![0x92_u8; 32],
            receipt.receipt_digest().to_vec(),
        ),
        (
            "binding_digest",
            vec![0x93_u8; 32],
            receipt.binding_digest().to_vec(),
        ),
    ] {
        let connection = Connection::open(&catalog_path)?;
        connection.execute(
            &format!("UPDATE ops_binding_rejections SET {column}=?1 WHERE request_id=?2"),
            params![&corrupt, binding.request_id().as_bytes()],
        )?;
        if column == "request_digest" {
            connection.execute(
                "UPDATE ops_binding_rejection_attempts SET request_digest=?1 WHERE request_id=?2",
                params![&corrupt, binding.request_id().as_bytes()],
            )?;
        }
        drop(connection);
        let corrupted = OperationsCatalog::open(&catalog_path, destination)?;
        assert!(matches!(
            corrupted.registration_binding_rejection(binding.request_id()),
            Err(OperationsError::Corrupt(_))
        ));
        drop(corrupted);
        let connection = Connection::open(&catalog_path)?;
        connection.execute(
            &format!("UPDATE ops_binding_rejections SET {column}=?1 WHERE request_id=?2"),
            params![&original, binding.request_id().as_bytes()],
        )?;
        if column == "request_digest" {
            connection.execute(
                "UPDATE ops_binding_rejection_attempts SET request_digest=?1 WHERE request_id=?2",
                params![&original, binding.request_id().as_bytes()],
            )?;
        }
    }
    let restored = OperationsCatalog::open(&catalog_path, destination)?;
    assert_eq!(
        restored.registration_binding_rejection(binding.request_id())?,
        Some(receipt)
    );
    drop(restored);
    source.cleanup()?;
    fs::remove_dir_all(catalog_root)?;
    Ok(())
}

struct SourceFixture {
    root: PathBuf,
    layout: WorkspaceStateLayout,
    store: WorkspaceStore,
    workspace: WorkspaceId,
    migration: Uuid,
    freeze: AgentOperationsFreeze,
}

impl SourceFixture {
    fn reopen_store(&mut self) -> Result<(), Box<dyn Error>> {
        let existing = WorkspaceStateLayout::inspect_existing(&self.root)?
            .ok_or("source layout missing while reopening")?;
        self.store = WorkspaceStore::open_existing(&existing)?;
        Ok(())
    }

    fn cleanup(self) -> Result<(), Box<dyn Error>> {
        let root = self.root.clone();
        drop(self.store);
        fs::remove_dir_all(root)?;
        Ok(())
    }
}

fn timestamp(value: i64) -> Result<TimestampMillis, Box<dyn Error>> {
    Ok(TimestampMillis::new(value)?)
}

/// Seed the exact legacy attach projection (`agent_instances`, initial focus
/// fact, `agent_presence`, JOINED participation event, and — when a current
/// Coordinator is already bound — that Coordinator's inbox receipt) with raw
/// SQL. The Forge attach writers are retired, so test sources are seeded
/// directly against the legacy tables on a short-lived connection.
#[allow(clippy::too_many_lines)]
fn insert_legacy_attached_agent(
    layout: &WorkspaceStateLayout,
    workspace: WorkspaceId,
    activity: ActivityId,
    agent_id: AgentInstanceId,
    session: AgentSessionId,
    display_name: &str,
    observed_at: TimestampMillis,
) -> Result<(), Box<dyn Error>> {
    let connection = Connection::open(layout.database_path())?;
    let observed = observed_at.get();
    connection.execute(
        "INSERT INTO agent_instances (
            agent_id, workspace_id, activity_id, display_name, declared_role,
            transport_kind, principal_kind, principal_id, attachment_state,
            focus_goal_id, focus_goal_revision, focus_dag_revision, focus_node_id,
            focus_node_revision, created_at_ms, updated_at_ms, revision, session_id
        ) VALUES (?1, ?2, ?3, ?4, 'worker', 1, 0, NULL, 0, NULL, NULL, NULL, NULL, NULL, ?5, ?5, 1, ?6)",
        params![
            agent_id.into_uuid().as_bytes(),
            workspace.into_uuid().as_bytes(),
            activity.into_uuid().as_bytes(),
            display_name,
            observed,
            session.into_uuid().as_bytes(),
        ],
    )?;
    connection.execute(
        "INSERT INTO agent_focus_events (
            agent_id, workspace_id, activity_id, event_sequence, event_kind,
            focus_goal_id, focus_goal_revision, focus_dag_revision, focus_node_id,
            focus_node_revision, observed_at_ms
        ) VALUES (?1, ?2, ?3, 1, 0, NULL, NULL, NULL, NULL, NULL, ?4)",
        params![
            agent_id.into_uuid().as_bytes(),
            workspace.into_uuid().as_bytes(),
            activity.into_uuid().as_bytes(),
            observed,
        ],
    )?;
    connection.execute(
        "INSERT INTO agent_presence (
            agent_id, workspace_id, activity_id, session_id,
            last_authenticated_at_ms, presence_revision
        ) VALUES (?1, ?2, ?3, ?4, ?5, 1)",
        params![
            agent_id.into_uuid().as_bytes(),
            workspace.into_uuid().as_bytes(),
            activity.into_uuid().as_bytes(),
            session.into_uuid().as_bytes(),
            observed,
        ],
    )?;
    let membership_revision: i64 = connection.query_row(
        "SELECT COALESCE(MAX(membership_revision), 0) FROM agent_events WHERE activity_id = ?1",
        params![activity.into_uuid().as_bytes()],
        |row| row.get(0),
    )?;
    let coordinator: Option<(Vec<u8>, Option<Vec<u8>>, i64)> = connection
        .query_row(
            "SELECT c.agent_id, ai.session_id, c.revision FROM activity_coordinators AS c
             JOIN agent_instances AS ai ON ai.agent_id = c.agent_id WHERE c.activity_id = ?1",
            params![activity.into_uuid().as_bytes()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()?;
    let (bound_agent, bound_session, bound_revision): (
        Option<Vec<u8>>,
        Option<Vec<u8>>,
        Option<i64>,
    ) = match &coordinator {
        Some((agent, Some(session), revision)) => {
            (Some(agent.clone()), Some(session.clone()), Some(*revision))
        }
        _ => (None, None, None),
    };
    let event_id = AgentEventId::for_transition(
        agent_id,
        AgentInstanceRevision::INITIAL,
        forge_domain::AgentEventKind::Joined,
    );
    connection.execute(
        "INSERT INTO agent_events (
            event_id, workspace_id, activity_id, membership_revision, event_kind,
            from_agent_id, from_session_id, from_principal_kind, from_principal_id,
            from_display_name, to_agent_id, to_session_id, to_principal_kind,
            to_principal_id, to_display_name, before_goal_id, before_goal_revision,
            before_dag_revision, before_dag_head_revision, before_head_id,
            before_head_revision, after_goal_id, after_goal_revision, after_dag_revision,
            after_dag_head_revision, after_head_id, after_head_revision,
            before_coordinator_agent_id, before_coordinator_session_id,
            before_coordinator_revision, after_coordinator_agent_id,
            after_coordinator_session_id, after_coordinator_revision, observed_at_ms
        ) VALUES (?1, ?2, ?3, ?4, 0, NULL, NULL, NULL, NULL, NULL, ?5, ?6, 0, NULL, ?7,
            NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
            ?8, ?9, ?10, ?8, ?9, ?10, ?11)",
        params![
            event_id.into_uuid().as_bytes(),
            workspace.into_uuid().as_bytes(),
            activity.into_uuid().as_bytes(),
            membership_revision + 1,
            agent_id.into_uuid().as_bytes(),
            session.into_uuid().as_bytes(),
            display_name,
            bound_agent,
            bound_session,
            bound_revision,
            observed,
        ],
    )?;
    if let Some((coordinator_agent, Some(coordinator_session), _)) = &coordinator {
        connection.execute(
            "INSERT INTO agent_inbox (
                recipient_agent_id, recipient_session_id, event_id,
                receipt_revision, acknowledged_at_ms
            ) VALUES (?1, ?2, ?3, 1, NULL)",
            params![
                coordinator_agent.as_slice(),
                coordinator_session.as_slice(),
                event_id.into_uuid().as_bytes()
            ],
        )?;
    }
    Ok(())
}

/// Acknowledge the coordinator's newest unread legacy inbox receipt directly,
/// preserving the imported-acknowledged-receipt fixture semantics without the
/// retired Forge `ack_agent_inbox` writer.
fn acknowledge_first_legacy_inbox_receipt(
    layout: &WorkspaceStateLayout,
    recipient: AgentInstanceId,
    session: AgentSessionId,
    acknowledged_at: TimestampMillis,
) -> Result<(), Box<dyn Error>> {
    let connection = Connection::open(layout.database_path())?;
    let event_id: Vec<u8> = connection.query_row(
        "SELECT event_id FROM agent_inbox WHERE recipient_agent_id = ?1
         AND recipient_session_id = ?2 AND acknowledged_at_ms IS NULL
         ORDER BY receipt_revision DESC, event_id DESC LIMIT 1",
        params![
            recipient.into_uuid().as_bytes(),
            session.into_uuid().as_bytes(),
        ],
        |row| row.get(0),
    )?;
    let acknowledged = connection.execute(
        "UPDATE agent_inbox SET acknowledged_at_ms = ?1
         WHERE recipient_agent_id = ?2 AND recipient_session_id = ?3 AND event_id = ?4",
        params![
            acknowledged_at.get(),
            recipient.into_uuid().as_bytes(),
            session.into_uuid().as_bytes(),
            event_id,
        ],
    )?;
    assert_eq!(acknowledged, 1, "legacy inbox receipt to acknowledge");
    Ok(())
}

fn insert_legacy_null_session(
    layout: &WorkspaceStateLayout,
    workspace: WorkspaceId,
    activity: ActivityId,
    agent_id: AgentInstanceId,
) -> Result<(), Box<dyn Error>> {
    let connection = Connection::open(layout.database_path())?;
    connection.execute(
        "INSERT INTO agent_instances (
            agent_id, workspace_id, activity_id, display_name, declared_role,
            transport_kind, principal_kind, principal_id, attachment_state,
            focus_goal_id, focus_goal_revision, focus_dag_revision, focus_node_id,
            focus_node_revision, created_at_ms, updated_at_ms, revision, session_id
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, NULL, ?8, NULL, NULL, NULL, NULL, NULL, ?9, ?10, ?11, NULL)",
        params![
            agent_id.into_uuid().as_bytes(),
            workspace.into_uuid().as_bytes(),
            activity.into_uuid().as_bytes(),
            "legacy null-session Agent",
            "legacy",
            1_i64,
            0_i64,
            0_i64,
            3_i64,
            4_i64,
            2_i64,
        ],
    )?;
    Ok(())
}

fn source_fixture(
    seed: u128,
    participant_count: u32,
    include_legacy: bool,
    destination: Uuid,
) -> Result<SourceFixture, Box<dyn Error>> {
    let root = std::env::temp_dir().join(format!(
        "heterarchy-orchestration-operations-source-{}-{}",
        std::process::id(),
        Uuid::now_v7()
    ));
    fs::create_dir_all(&root)?;
    let layout = WorkspaceStateLayout::initialize(&root)?;
    let workspace = WorkspaceId::from_uuid(Uuid::from_u128(seed));
    let location = LocationId::from_uuid(Uuid::from_u128(seed + 1));
    let activity = ActivityId::from_uuid(Uuid::from_u128(seed + 2));
    let runtime = RuntimeInstanceId::from_uuid(Uuid::from_u128(seed + 3));
    let command = EstablishWorkspace::new(
        workspace,
        location,
        WorkspaceRoot::from_canonical(layout.workspace_root().to_path_buf())?,
        layout.root_fingerprint()?,
        timestamp(1)?,
    );
    let mut store = WorkspaceStore::open(&layout, &command)?;
    store.bootstrap_activity(&BootstrapActivity::new(
        activity,
        workspace,
        WorkspaceRevision::INITIAL,
        runtime,
        timestamp(100_000)?,
        timestamp(2)?,
    ))?;

    let coordinator_id = AgentInstanceId::from_uuid(Uuid::from_u128(seed + 0x10));
    let coordinator_session = AgentSessionId::from_uuid(Uuid::from_u128(seed + 0x10 + 0x100_000));
    insert_legacy_attached_agent(
        &layout,
        workspace,
        activity,
        coordinator_id,
        coordinator_session,
        &format!("operations test Agent {}", seed + 0x10),
        timestamp(3)?,
    )?;
    let _ = store.claim_activity_coordinator(
        ClaimActivityCoordinator::new(
            workspace,
            activity,
            coordinator_id,
            AuthenticatedPrincipal::TrustedStdio,
            timestamp(4)?,
            coordinator_session,
        ),
        timestamp(4)?,
    )?;

    for ordinal in 0..participant_count {
        let participant_seed = seed + 0x100 + u128::from(ordinal);
        insert_legacy_attached_agent(
            &layout,
            workspace,
            activity,
            AgentInstanceId::from_uuid(Uuid::from_u128(participant_seed)),
            AgentSessionId::from_uuid(Uuid::from_u128(participant_seed + 0x100_000)),
            &format!("operations test Agent {participant_seed}"),
            timestamp(10 + i64::from(ordinal))?,
        )?;
    }

    acknowledge_first_legacy_inbox_receipt(
        &layout,
        coordinator_id,
        coordinator_session,
        timestamp(500)?,
    )?;
    drop(store);

    if include_legacy {
        insert_legacy_null_session(
            &layout,
            workspace,
            activity,
            AgentInstanceId::from_uuid(Uuid::from_u128(seed + 0x8000)),
        )?;
    }
    let existing = WorkspaceStateLayout::inspect_existing(&root)?.ok_or("source layout missing")?;
    let mut store = WorkspaceStore::open_existing(&existing)?;
    let migration = Uuid::from_u128(seed + 0x7000);
    let freeze_request =
        AgentOperationsFreezeRequest::new(migration, destination, timestamp(1_000)?)?;
    let freeze = store.freeze_agent_operations(&freeze_request)?;
    Ok(SourceFixture {
        root,
        layout,
        store,
        workspace,
        migration,
        freeze,
    })
}

fn catalog_root(seed: u128) -> Result<(PathBuf, PathBuf), Box<dyn Error>> {
    let root = std::env::temp_dir().join(format!(
        "heterarchy-orchestration-operations-catalog-{}-{}",
        std::process::id(),
        Uuid::now_v7()
    ));
    fs::create_dir_all(&root)?;
    let database = root.join(format!("catalog-{seed:x}.sqlite3"));
    Ok((root, database))
}

fn import_all(
    catalog: &mut OperationsCatalog,
    source: &SourceFixture,
) -> Result<ImportProgress, OperationsError> {
    loop {
        let progress = catalog
            .progress(source.migration)?
            .ok_or(OperationsError::Corrupt("missing test import"))?;
        if progress.phase() != ImportPhase::Importing {
            return Ok(progress);
        }
        catalog.import_next_page(&source.store, source.migration)?;
    }
}

fn verify_all(
    catalog: &mut OperationsCatalog,
    source: &SourceFixture,
) -> Result<ImportProgress, OperationsError> {
    loop {
        let progress = catalog
            .progress(source.migration)?
            .ok_or(OperationsError::Corrupt("missing test import"))?;
        if progress.phase() != ImportPhase::Imported {
            return Ok(progress);
        }
        catalog.verify_next_page(&source.store, source.migration)?;
    }
}

fn count_workspace_rows(
    connection: &Connection,
    table: &str,
    workspace: WorkspaceId,
) -> Result<i64, Box<dyn Error>> {
    let query = if table == "agent_inbox" {
        "SELECT COUNT(*) FROM agent_inbox i JOIN agent_events e ON e.event_id=i.event_id WHERE e.workspace_id=?1".to_owned()
    } else {
        format!("SELECT COUNT(*) FROM {table} WHERE workspace_id = ?1")
    };
    Ok(
        connection.query_row(&query, params![workspace.into_uuid().as_bytes()], |row| {
            row.get(0)
        })?,
    )
}

fn source_legacy_operational_counts(source: &SourceFixture) -> Result<[i64; 4], Box<dyn Error>> {
    let connection = Connection::open(source.layout.database_path())?;
    Ok([
        count_workspace_rows(&connection, "agent_presence", source.workspace)?,
        count_workspace_rows(&connection, "agent_events", source.workspace)?,
        count_workspace_rows(&connection, "agent_inbox", source.workspace)?,
        count_workspace_rows(&connection, "agent_effect_outbox_entries", source.workspace)?,
    ])
}

#[test]
fn effect_fact_reconciliation_requires_verified_matching_delegation_and_no_source_ack()
-> Result<(), Box<dyn Error>> {
    let destination = Uuid::from_u128(0xD0F0);
    let mut source = source_fixture(0xA0F0, 2, false, destination)?;
    let (catalog_root, catalog_path) = catalog_root(0xA0F0)?;
    let mut catalog = OperationsCatalog::open(&catalog_path, destination)?;
    let _prepared = catalog.prepare(&source.freeze)?;
    let imported = import_all(&mut catalog, &source)?;
    assert_eq!(imported.phase(), ImportPhase::Imported);
    let verified = verify_all(&mut catalog, &source)?;
    assert_eq!(verified.phase(), ImportPhase::Verified);

    assert!(matches!(
        catalog.reconcile_effect_facts(&source.store, source.migration),
        Err(OperationsError::Conflict(
            "effect fact source delegation missing"
        ))
    ));
    assert!(catalog.effect_fact_progress(source.migration)?.is_none());

    let delegation =
        source
            .store
            .delegate_agent_operations(&source.freeze, [0x5F; 32], timestamp(2_500)?)?;
    assert_eq!(delegation.migration_id(), source.migration);
    let observed = catalog.reconcile_effect_facts(&source.store, source.migration)?;
    assert_eq!(observed.migration_id(), source.migration);
    assert_eq!(observed.workspace_id(), source.workspace);
    assert_eq!(observed.destination_id(), destination);
    assert_eq!(observed.last_sequence(), 0);
    assert_eq!(observed.applied_count(), 0);
    assert!(
        catalog.effect_fact_progress(source.migration)?.is_none(),
        "an empty Forge page must not manufacture a consumer acknowledgement"
    );

    let mut foreign = source_fixture(0xA1F0, 1, false, destination)?;
    let _foreign_delegation =
        foreign
            .store
            .delegate_agent_operations(&foreign.freeze, [0x6F; 32], timestamp(2_500)?)?;
    assert!(matches!(
        catalog.reconcile_effect_facts(&foreign.store, source.migration),
        Err(OperationsError::Conflict(
            "effect fact source delegation identity"
        ))
    ));
    assert!(catalog.effect_fact_progress(source.migration)?.is_none());

    drop(catalog);
    source.cleanup()?;
    foreign.cleanup()?;
    fs::remove_dir_all(catalog_root)?;
    Ok(())
}

#[test]
#[allow(clippy::too_many_lines)]
fn import_resume_replay_and_verify_preserves_bounded_history() -> Result<(), Box<dyn Error>> {
    let destination = Uuid::from_u128(0xD001);
    let source = source_fixture(0xA001, 70, true, destination)?;
    let (catalog_root, catalog_path) = catalog_root(0xA001)?;
    let mut catalog = OperationsCatalog::open(&catalog_path, destination)?;
    let prepared = catalog.prepare(&source.freeze)?;
    assert_eq!(prepared.phase(), ImportPhase::Importing);
    let first = source
        .store
        .export_agent_operations(source.migration, Cursor::default())?;
    assert_eq!(first.records().len(), 64);
    assert!(matches!(
        first.next_cursor(),
        Some(Cursor::Participants(Some(_)))
    ));
    let after_first = catalog.import_page(&first)?;
    assert_eq!(after_first.page_count(), 1);
    assert_eq!(catalog.import_page(&first)?, after_first);

    drop(catalog);
    let mut catalog = OperationsCatalog::open(&catalog_path, destination)?;
    assert_eq!(
        catalog
            .progress(source.migration)?
            .ok_or("progress missing")?,
        after_first
    );
    let imported = import_all(&mut catalog, &source)?;
    assert_eq!(imported.phase(), ImportPhase::Imported);
    assert!(imported.page_count() > 1);
    assert!(imported.record_count() > 64);
    let verified = verify_all(&mut catalog, &source)?;
    assert_eq!(verified.phase(), ImportPhase::Verified);
    assert_eq!(verified.verified_pages(), verified.page_count());

    drop(catalog);
    let source_connection = Connection::open(source.layout.database_path())?;
    let destination_connection = Connection::open(&catalog_path)?;
    for (source_table, destination_table) in [
        ("agent_instances", "ops_members"),
        ("agent_presence", "ops_presence"),
        ("agent_events", "ops_events"),
        ("agent_inbox", "ops_inbox"),
        ("activity_coordinators", "ops_activity_coordinators"),
    ] {
        assert_eq!(
            count_workspace_rows(&source_connection, source_table, source.workspace)?,
            count_workspace_rows(&destination_connection, destination_table, source.workspace)?,
            "row count mismatch for {source_table}/{destination_table}"
        );
    }
    let null_sessions: i64 = destination_connection.query_row(
        "SELECT COUNT(*) FROM ops_members WHERE workspace_id=?1 AND session_id IS NULL",
        params![source.workspace.into_uuid().as_bytes()],
        |row| row.get(0),
    )?;
    assert_eq!(null_sessions, 1);
    let acknowledged: i64 = destination_connection.query_row(
        "SELECT COUNT(*) FROM ops_inbox WHERE workspace_id=?1 AND acknowledged_at_ms IS NOT NULL",
        params![source.workspace.into_uuid().as_bytes()],
        |row| row.get(0),
    )?;
    assert!(acknowledged >= 1);

    drop(source_connection);
    drop(destination_connection);
    source.cleanup()?;
    fs::remove_dir_all(catalog_root)?;
    Ok(())
}

#[test]
fn explicit_page_verification_matches_native_source_fetch_progress() -> Result<(), Box<dyn Error>> {
    let destination = Uuid::from_u128(0xD050);
    let source = source_fixture(0xA050, 70, false, destination)?;
    let (native_root, native_path) = catalog_root(0xA050)?;
    let (explicit_root, explicit_path) = catalog_root(0xA051)?;
    let mut native = OperationsCatalog::open(&native_path, destination)?;
    let mut explicit = OperationsCatalog::open(&explicit_path, destination)?;
    native.prepare(&source.freeze)?;
    explicit.prepare(&source.freeze)?;
    assert!(matches!(
        native.verification_target(source.migration),
        Err(OperationsError::Conflict("import not ready to verify"))
    ));
    import_all(&mut native, &source)?;
    import_all(&mut explicit, &source)?;
    let target = explicit.verification_target(source.migration)?;
    assert_eq!(target.progress().phase(), ImportPhase::Imported);
    assert_eq!(target.requested_cursor(), Some(Cursor::default()));

    let native_verified = verify_all(&mut native, &source)?;
    let mut cursor = Cursor::default();
    let explicit_verified = loop {
        let page = source
            .store
            .export_agent_operations(source.migration, cursor)?;
        let progress = explicit.verify_page(&page)?;
        match page.next_cursor() {
            Some(next) => cursor = next,
            None => break progress,
        }
    };
    assert_eq!(explicit_verified, native_verified);
    assert_eq!(explicit_verified.phase(), ImportPhase::Verified);
    let verified_target = explicit.verification_target(source.migration)?;
    assert_eq!(verified_target.progress(), &explicit_verified);
    assert_eq!(verified_target.requested_cursor(), None);

    let first_page = source
        .store
        .export_agent_operations(source.migration, Cursor::default())?;
    let participants = first_page
        .records()
        .map(|record| match record {
            Record::Participant(value) => Ok(value.clone()),
            _ => Err("expected the first export page to contain participants"),
        })
        .collect::<Result<Vec<_>, _>>()?;
    let wrong_header = AgentOperationsExportPage::from_participants(
        source.workspace,
        Uuid::from_u128(0xD051),
        source.migration,
        source.freeze.revision(),
        Cursor::default(),
        first_page.next_cursor(),
        participants,
    )?;
    assert!(matches!(
        explicit.verify_page(&wrong_header),
        Err(OperationsError::Conflict("source page header"))
    ));

    drop(native);
    drop(explicit);
    source.cleanup()?;
    fs::remove_dir_all(native_root)?;
    fs::remove_dir_all(explicit_root)?;
    Ok(())
}

#[test]
fn expected_progress_fences_stale_page_verify_and_abort() -> Result<(), Box<dyn Error>> {
    let destination = Uuid::from_u128(0xD060);
    let source = source_fixture(0xA060, 70, false, destination)?;
    let (catalog_root, catalog_path) = catalog_root(0xA060)?;
    let mut catalog = OperationsCatalog::open(&catalog_path, destination)?;
    catalog.prepare(&source.freeze)?;
    let initial = catalog
        .progress(source.migration)?
        .ok_or("initial import progress missing")?;
    let first = source
        .store
        .export_agent_operations(source.migration, Cursor::default())?;
    let after_first = catalog.import_page_expected(&first, &initial)?;
    let second_cursor = first.next_cursor().ok_or("second page cursor missing")?;
    let second = source
        .store
        .export_agent_operations(source.migration, second_cursor)?;
    assert!(matches!(
        catalog.import_page_expected(&second, &initial),
        Err(OperationsError::Conflict("import progress changed"))
    ));
    assert_eq!(
        catalog
            .progress(source.migration)?
            .ok_or("progress missing after stale import")?,
        after_first
    );
    let imported = import_all(&mut catalog, &source)?;
    let first_verify = source
        .store
        .export_agent_operations(source.migration, Cursor::default())?;
    let after_verify = catalog.verify_page_expected(&first_verify, &imported)?;
    let second_verify_cursor = first_verify
        .next_cursor()
        .ok_or("second verification cursor missing")?;
    let second_verify = source
        .store
        .export_agent_operations(source.migration, second_verify_cursor)?;
    assert!(matches!(
        catalog.verify_page_expected(&second_verify, &imported),
        Err(OperationsError::Conflict("import progress changed"))
    ));
    assert_eq!(
        catalog
            .progress(source.migration)?
            .ok_or("progress missing after stale verify")?,
        after_verify
    );
    assert!(matches!(
        catalog.abort_import_expected(source.migration, &imported),
        Err(OperationsError::Conflict("import progress changed"))
    ));
    assert_eq!(
        catalog
            .abort_import_expected(source.migration, &after_verify)?
            .phase(),
        ImportPhase::Aborted
    );

    drop(catalog);
    source.cleanup()?;
    fs::remove_dir_all(catalog_root)?;
    Ok(())
}

#[test]
fn verified_page_replay_still_requires_the_exact_progress_fence() -> Result<(), Box<dyn Error>> {
    let destination = Uuid::from_u128(0xD062);
    let source = source_fixture(0xA062, 70, false, destination)?;
    let (root, path) = catalog_root(0xA062)?;
    let mut catalog = OperationsCatalog::open(&path, destination)?;
    catalog.prepare(&source.freeze)?;
    let imported = import_all(&mut catalog, &source)?;
    let verified = verify_all(&mut catalog, &source)?;
    let page = source
        .store
        .export_agent_operations(source.migration, Cursor::default())?;
    assert!(matches!(
        catalog.verify_page_expected(&page, &imported),
        Err(OperationsError::Conflict("import progress changed"))
    ));
    assert_eq!(catalog.verify_page_expected(&page, &verified)?, verified);
    drop(catalog);
    source.cleanup()?;
    fs::remove_dir_all(root)?;
    Ok(())
}

#[test]
fn foreign_destination_and_out_of_order_pages_are_rejected() -> Result<(), Box<dyn Error>> {
    let destination = Uuid::from_u128(0xD101);
    let source = source_fixture(0xA101, 70, false, destination)?;
    let (foreign_root, foreign_path) = catalog_root(0xF101)?;
    let mut foreign = OperationsCatalog::open(&foreign_path, Uuid::from_u128(0xD102))?;
    assert!(matches!(
        foreign.prepare(&source.freeze),
        Err(OperationsError::Conflict("source freeze/destination"))
    ));
    drop(foreign);

    let (catalog_root, catalog_path) = catalog_root(0xA101)?;
    let mut catalog = OperationsCatalog::open(&catalog_path, destination)?;
    catalog.prepare(&source.freeze)?;
    let first = source
        .store
        .export_agent_operations(source.migration, Cursor::default())?;
    let next = first
        .next_cursor()
        .ok_or("test source did not cross page bound")?;
    let second = source
        .store
        .export_agent_operations(source.migration, next)?;
    assert!(matches!(
        catalog.import_page(&second),
        Err(OperationsError::Conflict("page order"))
    ));
    import_all(&mut catalog, &source)?;
    drop(catalog);

    let connection = Connection::open(&catalog_path)?;
    let agent_id: Vec<u8> = connection.query_row(
        "SELECT agent_id FROM ops_members WHERE workspace_id=?1 ORDER BY agent_id LIMIT 1",
        params![source.workspace.into_uuid().as_bytes()],
        |row| row.get(0),
    )?;
    connection.execute(
        "UPDATE ops_members SET display_name='tampered' WHERE workspace_id=?1 AND agent_id=?2",
        params![source.workspace.into_uuid().as_bytes(), agent_id],
    )?;
    drop(connection);

    let mut catalog = OperationsCatalog::open(&catalog_path, destination)?;
    let error = catalog.verify_next_page(&source.store, source.migration);
    assert!(matches!(
        error,
        Err(OperationsError::Corrupt(
            "destination differs from frozen source"
        ))
    ));
    catalog.connection.execute(
        "UPDATE ops_import_pages SET table_kind=1 WHERE migration_id=?1 AND ordinal=0",
        params![source.migration.as_bytes()],
    )?;
    assert!(matches!(
        catalog.verify_next_page(&source.store, source.migration),
        Err(OperationsError::Corrupt("page table kind"))
    ));
    drop(catalog);

    source.cleanup()?;
    fs::remove_dir_all(catalog_root)?;
    fs::remove_dir_all(foreign_root)?;
    Ok(())
}

#[test]
fn abort_removes_only_the_owned_workspace_rows() -> Result<(), Box<dyn Error>> {
    let destination = Uuid::from_u128(0xD201);
    let source = source_fixture(0xA201, 70, false, destination)?;
    let other = source_fixture(0xA301, 1, false, destination)?;
    let (catalog_root, catalog_path) = catalog_root(0xA201)?;
    let mut catalog = OperationsCatalog::open(&catalog_path, destination)?;
    catalog.prepare(&source.freeze)?;
    catalog.prepare(&other.freeze)?;
    catalog.import_next_page(&source.store, source.migration)?;
    let other_imported = import_all(&mut catalog, &other)?;
    assert_eq!(other_imported.phase(), ImportPhase::Imported);

    let aborted = catalog.abort_import(source.migration)?;
    assert_eq!(aborted.phase(), ImportPhase::Aborted);
    let late = source
        .store
        .export_agent_operations(source.migration, Cursor::default())?;
    assert!(matches!(
        catalog.import_page(&late),
        Err(OperationsError::Conflict("import aborted"))
    ));
    assert_eq!(
        catalog
            .progress(other.migration)?
            .ok_or("other progress missing")?
            .phase(),
        ImportPhase::Imported
    );
    drop(catalog);

    let connection = Connection::open(&catalog_path)?;
    assert_eq!(
        count_workspace_rows(&connection, "ops_members", source.workspace)?,
        0
    );
    assert!(
        count_workspace_rows(&connection, "ops_members", other.workspace)? > 0,
        "abort removed another Workspace's imported members"
    );
    drop(connection);

    source.cleanup()?;
    other.cleanup()?;
    fs::remove_dir_all(catalog_root)?;
    Ok(())
}

#[test]
fn historical_event_snapshots_preserve_distinct_revisions_without_live_members()
-> Result<(), Box<dyn Error>> {
    use forge_domain::{
        ActivityCoordinatorRevision, AgentEvent, AgentEventKind, AgentFocusNode,
        AgentMembershipRevision, AgentParticipantRef, CoordinationSnapshot, CoordinatorSnapshot,
        DagHeadRevision, DagNodeId, DagNodeRevision, DagRevisionNumber, DirectTokenId,
        GoalAttribution, GoalId, GoalRevision,
    };
    let destination = Uuid::from_u128(0xF900);
    let (root, path) = catalog_root(0xF900)?;
    let mut catalog = OperationsCatalog::open(&path, destination)?;
    let workspace = WorkspaceId::from_uuid(Uuid::from_u128(0xF901));
    let activity = ActivityId::from_uuid(Uuid::from_u128(0xF902));
    let before = CoordinationSnapshot::new(
        Some(GoalAttribution::new(
            GoalId::from_uuid(Uuid::from_u128(0xF910)),
            GoalRevision::new(7)?,
        )),
        Some(DagRevisionNumber::new(11)?),
        Some(DagHeadRevision::new(13)?),
        Some(AgentFocusNode::new(
            DagNodeId::from_uuid(Uuid::from_u128(0xF911)),
            DagNodeRevision::new(17)?,
        )),
    )?;
    let after = CoordinationSnapshot::new(
        Some(GoalAttribution::new(
            GoalId::from_uuid(Uuid::from_u128(0xF920)),
            GoalRevision::new(19)?,
        )),
        Some(DagRevisionNumber::new(23)?),
        Some(DagHeadRevision::new(29)?),
        Some(AgentFocusNode::new(
            DagNodeId::from_uuid(Uuid::from_u128(0xF921)),
            DagNodeRevision::new(31)?,
        )),
    )?;
    let from_id = AgentInstanceId::from_uuid(Uuid::from_u128(0xF930));
    let to_id = AgentInstanceId::from_uuid(Uuid::from_u128(0xF931));
    let session = AgentSessionId::from_uuid(Uuid::from_u128(0xF932));
    let event = AgentEvent::new(
        AgentEventId::from_uuid(Uuid::from_u128(0xF933)),
        workspace,
        activity,
        AgentMembershipRevision::new(43)?,
        AgentEventKind::HandoffCompleted,
        Some(AgentParticipantRef::new(
            from_id,
            None,
            AuthenticatedPrincipal::TrustedStdio,
            AgentDisplayName::new("과거 담당".to_owned())?,
        )),
        Some(AgentParticipantRef::new(
            to_id,
            Some(session),
            AuthenticatedPrincipal::DirectToken(DirectTokenId::from_uuid(Uuid::from_u128(0xF934))),
            AgentDisplayName::new("다음 담당".to_owned())?,
        )),
        before,
        after,
        CoordinatorSnapshot::bound(from_id, None, ActivityCoordinatorRevision::new(37)?),
        CoordinatorSnapshot::bound(to_id, Some(session), ActivityCoordinatorRevision::new(41)?),
        timestamp(47)?,
    );
    let expected = crate::events::event_digest(&event)?;
    let tx = catalog.connection.transaction()?;
    crate::members::scope(
        &tx,
        workspace.into_uuid().into_bytes(),
        activity.into_uuid().into_bytes(),
    )?;
    crate::events::put_event(&tx, &event)?;
    assert_eq!(
        crate::events::get_event(&tx, workspace, event.id())?,
        Some(event.clone())
    );
    tx.commit()?;
    catalog.connection.execute(
        "UPDATE ops_events SET before_dag_head_revision=53 WHERE workspace_id=?1 AND event_id=?2",
        params![
            workspace.into_uuid().as_bytes(),
            event.id().into_uuid().as_bytes()
        ],
    )?;
    let changed = crate::events::get_event(&catalog.connection, workspace, event.id())?
        .ok_or("event missing")?;
    assert_ne!(crate::events::event_digest(&changed)?, expected);
    drop(catalog);
    fs::remove_dir_all(root)?;
    Ok(())
}

#[test]
#[allow(
    clippy::too_many_lines,
    reason = "Keeps the real Source import, native registration activation, append, replay, and reopen evidence together."
)]
fn ready_native_participant_can_append_enqueue_replay_and_read_back_node_message()
-> Result<(), Box<dyn Error>> {
    let destination = Uuid::from_u128(0xFC00);
    let seed = 0xFC01;
    let mut source = source_fixture(seed, 1, false, destination)?;
    let (catalog_root, catalog_path) = catalog_root(seed)?;
    let mut catalog = OperationsCatalog::open(&catalog_path, destination)?;
    let _prepared = catalog.prepare(&source.freeze)?;
    import_all(&mut catalog, &source)?;
    assert_eq!(
        verify_all(&mut catalog, &source)?.phase(),
        ImportPhase::Verified
    );

    let workspace = source.workspace;
    let activity = ActivityId::from_uuid(Uuid::from_u128(seed + 2));
    let session = AgentSessionId::from_uuid(Uuid::from_u128(seed + 0x100));
    let agent = AgentInstanceId::from_uuid(Uuid::from_u128(seed + 0x101));
    let participant_id =
        crate::native_session_participation_id(workspace.into_uuid(), session.into_uuid());
    let principal = AuthenticatedPrincipal::DirectToken(DirectTokenId::from_uuid(Uuid::from_u128(
        seed + 0x103,
    )));
    let binding = AgentBindingReservationRequest::new(
        Uuid::from_u128(seed + 0x104),
        activity,
        agent,
        session,
        principal,
        timestamp(1_001)?,
    )?;
    let request = RegistrationRequest::new(
        participant_id,
        binding.clone(),
        AgentDisplayName::new("Native Main".to_owned())?,
        AgentDeclaredRole::new("coordinator".to_owned())?,
        AgentTransportKind::StreamableHttp,
    )?;
    let pending = catalog.prepare_registration(source.migration, &request)?;
    assert_eq!(pending.participant_id(), participant_id);
    assert_eq!(
        catalog.registration_profile_scope(binding.request_id())?,
        None
    );

    let committed = reserve_and_commit_registration(
        &mut source,
        &mut catalog,
        &binding,
        &[],
        timestamp(1_002)?,
        timestamp(2_000)?,
    )?;
    let delegation =
        source
            .store
            .delegate_agent_operations(&source.freeze, [0xFC; 32], timestamp(2_500)?)?;
    activate_registration(
        &mut source,
        &mut catalog,
        &delegation,
        &binding,
        &committed,
        timestamp(3_000)?,
    )?;
    assert_eq!(
        catalog.ready_participant_for_agent(workspace, activity, agent)?,
        Some(participant_id)
    );
    let logical = catalog
        .logical_identity_for_session(workspace, session)?
        .ok_or("native logical Session missing")?;
    assert_eq!(logical.participant_id(), participant_id);
    assert_eq!(logical.session_id(), session);

    let message_id = Uuid::from_u128(seed + 0x105);
    let command = crate::AppendWorkspaceMessageCommand {
        message_id,
        workspace_id: workspace,
        channel: crate::MessageChannel::Workspace,
        actor: crate::OperationalActor::Participant {
            activity_id: activity,
            agent_id: agent,
            session_id: session,
        },
        body: "Native Main joined the workspace discussion".to_owned(),
        submitted_at: timestamp(3_001)?,
        context: crate::MessageContext::default(),
    };
    let appended = catalog.append_workspace_message(command.clone())?;
    assert!(appended.created());
    assert_eq!(appended.message().author_display_name(), "Native Main");
    assert!(matches!(
        appended.message().author(),
        crate::MessageAuthor::NativeParticipant {
            participant_id: stored_participant,
            session_id: stored_session,
        } if *stored_participant == participant_id && *stored_session == session
    ));

    drop(catalog);
    let mut reopened = OperationsCatalog::open(&catalog_path, destination)?;
    let stored = reopened
        .workspace_message(message_id)?
        .ok_or("native message missing after reopen")?;
    assert_eq!(stored.author_display_name(), "Native Main");
    assert_eq!(stored.body(), "Native Main joined the workspace discussion");
    let replay = reopened.append_workspace_message(command.clone())?;
    assert!(!replay.created());
    assert_eq!(replay.message(), &stored);

    let node_context = crate::MessageContext::new(
        Some(activity),
        Some(forge_domain::GoalId::from_uuid(Uuid::from_u128(
            seed + 0x109,
        ))),
        Some(timestamp(3_002)?),
        Some(forge_domain::DagRevisionNumber::INITIAL),
        Some(forge_domain::GoalRevision::INITIAL),
        Some(forge_domain::DagNodeId::from_uuid(Uuid::from_u128(
            seed + 0x10A,
        ))),
    )?;
    let (target, node_witness, leadership_revision) = seed_native_node_route(
        &mut reopened,
        &source,
        destination,
        command.actor,
        participant_id,
        node_context,
        seed,
    )?;
    let node_command = crate::AppendWorkspaceMessageCommand {
        message_id: Uuid::from_u128(seed + 0x10B),
        workspace_id: workspace,
        channel: crate::MessageChannel::Direct(target.participation_id()),
        actor: command.actor,
        body: "Native Main node conversation".to_owned(),
        submitted_at: timestamp(5_002)?,
        context: node_context,
    };
    let node_first = reopened.append_and_enqueue_provider_message(
        node_command.clone(),
        target.clone(),
        Some(node_witness.clone()),
    )?;
    assert!(node_first.created());
    assert_eq!(
        node_first.outbox().state(),
        crate::ProviderMessageState::Queued
    );
    assert_eq!(
        node_first.outbox().node_route_witness(),
        Some(&node_witness)
    );
    let node_message = reopened
        .workspace_message(node_command.message_id)?
        .ok_or("native node message missing after append")?;
    assert!(matches!(
        node_message.author(),
        crate::MessageAuthor::NativeParticipant {
            participant_id: stored_participant,
            session_id: stored_session,
        } if *stored_participant == participant_id && *stored_session == session
    ));

    let mut stale_command = node_command.clone();
    stale_command.message_id = Uuid::from_u128(seed + 0x120);
    let stale_leadership_revision = leadership_revision
        .checked_add(1)
        .ok_or("leadership revision overflow")?;
    let stale_witness = crate::NodeMessageRouteWitness::new(
        node_witness.operation_id(),
        node_witness.participation_revision(),
        node_witness.runtime_revision(),
        stale_command.actor,
        Some(stale_leadership_revision),
        node_witness.target(),
    )?;
    assert!(matches!(
        reopened.append_and_enqueue_provider_message(
            stale_command.clone(),
            target.clone(),
            Some(stale_witness),
        ),
        Err(OperationsError::Conflict("node message sender leadership"))
    ));
    assert!(
        reopened
            .workspace_message(stale_command.message_id)?
            .is_none()
    );
    assert!(
        reopened
            .provider_message_outbox(stale_command.message_id)?
            .is_none()
    );

    let replay = reopened.append_and_enqueue_provider_message(
        node_command.clone(),
        target.clone(),
        Some(node_witness.clone()),
    )?;
    assert!(!replay.created());
    assert_eq!(replay.outbox(), node_first.outbox());
    assert_eq!(
        reopened
            .workspace_message(node_command.message_id)?
            .as_ref(),
        Some(&node_message)
    );

    drop(reopened);
    let mut reopened = OperationsCatalog::open(&catalog_path, destination)?;
    let stored_message = reopened
        .workspace_message(node_command.message_id)?
        .ok_or("native node message missing after reopen")?;
    let stored_outbox = reopened
        .provider_message_outbox(node_command.message_id)?
        .ok_or("native node outbox missing after reopen")?;
    assert_eq!(stored_message, node_message);
    assert_eq!(stored_outbox, *node_first.outbox());
    let replay = reopened.append_and_enqueue_provider_message(
        node_command.clone(),
        target,
        Some(node_witness),
    )?;
    assert!(!replay.created());
    assert_eq!(replay.outbox(), &stored_outbox);
    let message_count: i64 = reopened.connection.query_row(
        "SELECT count(*) FROM ops_workspace_messages WHERE message_id=?1",
        rusqlite::params![node_command.message_id.as_bytes()],
        |row| row.get(0),
    )?;
    let outbox_count: i64 = reopened.connection.query_row(
        "SELECT count(*) FROM ops_provider_message_outbox WHERE message_id=?1",
        rusqlite::params![node_command.message_id.as_bytes()],
        |row| row.get(0),
    )?;
    assert_eq!(message_count, 1);
    assert_eq!(outbox_count, 1);
    drop(reopened);
    source.cleanup()?;
    fs::remove_dir_all(catalog_root)?;
    Ok(())
}

#[allow(
    clippy::too_many_lines,
    reason = "Builds one real Operations recipient route, node assignment, and current native leadership witness for the queue regression."
)]
fn seed_native_node_route(
    catalog: &mut OperationsCatalog,
    source: &SourceFixture,
    destination: Uuid,
    sender: crate::OperationalActor,
    sender_participant: Uuid,
    context: crate::MessageContext,
    seed: u128,
) -> Result<
    (
        crate::ProviderMessageTarget,
        crate::NodeMessageRouteWitness,
        i64,
    ),
    Box<dyn Error>,
> {
    let workspace = source.workspace;
    let migration_id = source.migration;
    let (activity, sender_agent, sender_session) = match sender {
        crate::OperationalActor::Participant {
            activity_id,
            agent_id,
            session_id,
        } => (activity_id, agent_id, session_id),
        crate::OperationalActor::LocalOperator => return Err("native sender required".into()),
    };
    let operation_id = Uuid::from_u128(seed + 0x200);
    let request_id = Uuid::from_u128(seed + 0x201);
    let recipient_participant = Uuid::from_u128(seed + 0x202);
    let recipient_agent = AgentInstanceId::from_uuid(Uuid::from_u128(seed + 0x203));
    let recipient_session = AgentSessionId::from_uuid(Uuid::from_u128(seed + 0x204));
    let profile_id = AgentProfileId::from_uuid(Uuid::from_u128(seed + 0x205))?;
    let participation_id = WorkspaceParticipationId::from_uuid(Uuid::from_u128(seed + 0x206))?;
    let runtime_session_id =
        heterarchy_orchestration_sessions::AgentProfileRuntimeSessionId::from_uuid(
            Uuid::from_u128(seed + 0x207),
        )?;
    let assignment_id = Uuid::from_u128(seed + 0x208);
    let leadership_assignment_id = Uuid::from_u128(seed + 0x209);

    catalog.connection.execute(
        "INSERT INTO ops_workspace_participations(workspace_id,agent_profile_id,participation_id,created_at_ms,revision) VALUES(?1,?2,?3,4000,1)",
        rusqlite::params![workspace.into_uuid().as_bytes(), profile_id.into_uuid().as_bytes(), participation_id.into_uuid().as_bytes()],
    )?;
    catalog.connection.execute(
        "INSERT INTO ops_logical_members(workspace_id,participant_id,session_id,principal_kind,principal_id,display_name,declared_role,transport_kind,membership_revision,created_at_ms) VALUES(?1,?2,?3,0,NULL,'Node target','worker',1,1,4000)",
        rusqlite::params![workspace.into_uuid().as_bytes(), recipient_participant.as_bytes(), recipient_session.into_uuid().as_bytes()],
    )?;
    catalog.connection.execute(
        "INSERT INTO ops_current_effect_bindings(workspace_id,participant_id,activity_id,session_id,agent_id,source_kind,source_request_id) VALUES(?1,?2,?3,?4,?5,0,?6)",
        rusqlite::params![workspace.into_uuid().as_bytes(), recipient_participant.as_bytes(), activity.into_uuid().as_bytes(), recipient_session.into_uuid().as_bytes(), recipient_agent.into_uuid().as_bytes(), operation_id.as_bytes()],
    )?;
    catalog.connection.execute(
        "INSERT INTO ops_workspace_participation_mappings(request_id,workspace_id,participation_id,agent_profile_id,profile_revision,profile_mapping_request_id,session_id,session_revision,participant_id,participant_membership_revision,mapped_at_ms) VALUES(?1,?2,?3,?4,1,?5,?6,1,?7,1,4000)",
        rusqlite::params![Uuid::from_u128(seed + 0x20A).as_bytes(), workspace.into_uuid().as_bytes(), participation_id.into_uuid().as_bytes(), profile_id.into_uuid().as_bytes(), operation_id.as_bytes(), recipient_session.into_uuid().as_bytes(), recipient_participant.as_bytes()],
    )?;
    catalog.connection.execute(
        "INSERT INTO ops_agent_requests(request_id,workspace_id,activity_id,requester_agent_id,requester_session_id,coordinator_revision,objective,capability,requested_count,effect_scope,independence_requirement,reason,state,created_at_ms,revision) VALUES(?1,?2,?3,?4,?5,1,'node','worker',1,0,0,'native node conversation',1,4000,1)",
        rusqlite::params![request_id.as_bytes(), workspace.into_uuid().as_bytes(), activity.into_uuid().as_bytes(), sender_agent.into_uuid().as_bytes(), sender_session.into_uuid().as_bytes()],
    )?;
    catalog.connection.execute(
        "INSERT INTO ops_agent_request_resolutions(request_id,slot_ordinal,resolution_kind,claim_id,profile_id,profile_revision,runtime_session_id,runtime_session_revision,policy_revision,reserved_at_ms) VALUES(?1,0,2,?2,?3,1,?4,1,1,4000)",
        rusqlite::params![request_id.as_bytes(), Uuid::from_u128(seed + 0x20B).as_bytes(), profile_id.into_uuid().as_bytes(), runtime_session_id.into_uuid().as_bytes()],
    )?;
    catalog.connection.execute(
        "INSERT INTO ops_agent_provision_operations(operation_id,request_id,slot_ordinal,effect_kind,provider_family,profile_id,runtime_session_id,state,provider_conversation_id,prepared_at_ms,updated_at_ms,revision) VALUES(?1,?2,0,1,1,?3,?4,5,'native-node-conversation',4000,4000,1)",
        rusqlite::params![operation_id.as_bytes(), request_id.as_bytes(), profile_id.into_uuid().as_bytes(), runtime_session_id.into_uuid().as_bytes()],
    )?;

    let scope = crate::NodeTeamBindingScope::new(
        workspace,
        activity,
        context.goal_id().ok_or("node Goal missing")?,
        context
            .goal_created_at()
            .ok_or("node creation timestamp missing")?,
        context.dag_revision().ok_or("node DAG revision missing")?,
        context
            .dag_base_goal_revision()
            .ok_or("node base Goal revision missing")?,
        context.node_id().ok_or("node ID missing")?,
    )?;
    let _assignment_affinity =
        catalog.record_assignment_affinity(&crate::AssignmentAffinityCommand {
            workspace_id: workspace,
            assignment_id: forge_domain::AgentAssignmentId::from_uuid(assignment_id),
            activity_id: activity,
            target_agent_id: recipient_agent,
            target_session_id: recipient_session,
            expected_target_principal: forge_domain::AuthenticatedPrincipal::TrustedStdio,
            requester: crate::OperationalActor::LocalOperator,
            team_id: None,
            assignment_payload_fingerprint: [0xD1; 32],
            original_offer_payload: b"native-node-fixture".to_vec(),
            observed_at: timestamp(4_100)?,
            node_scope: scope,
            expected_binding_revision: None,
        })?;

    let offered_at = 5_000;
    catalog.connection.execute(
        "INSERT INTO ops_effect_fact_consumers(migration_id,workspace_id,destination_id,last_sequence,applied_count) VALUES(?1,?2,?3,0,0)",
        rusqlite::params![migration_id.as_bytes(), workspace.into_uuid().as_bytes(), destination.as_bytes()],
    )?;
    catalog.connection.execute(
        "INSERT INTO ops_assignment_offer_projection(migration_id,source_sequence,fact_digest,assignment_id,workspace_id,activity_id,goal_id,node_id,target_agent_id,target_session_id,credit_budget,offered_at_ms) VALUES(?1,1,?2,?3,?4,?5,?6,?7,?8,?9,NULL,?10)",
        rusqlite::params![migration_id.as_bytes(), [0xD1_u8; 32].as_slice(), leadership_assignment_id.as_bytes(), workspace.into_uuid().as_bytes(), activity.into_uuid().as_bytes(), context.goal_id().ok_or("node Goal missing")?.into_uuid().as_bytes(), context.node_id().ok_or("node ID missing")?.into_uuid().as_bytes(), sender_agent.into_uuid().as_bytes(), sender_session.into_uuid().as_bytes(), offered_at],
    )?;
    {
        let transaction = catalog.connection.transaction()?;
        crate::leadership::bootstrap_from_offer(
            &transaction,
            crate::leadership::OfferBootstrapObservation {
                workspace_id: workspace,
                activity_id: activity,
                assignment_id: forge_domain::AgentAssignmentId::from_uuid(leadership_assignment_id),
                target_agent_id: sender_agent,
                offered_at_ms: offered_at,
            },
        )?;
        transaction.commit()?;
    }
    let leadership_scope = crate::LeadershipScope::workspace(workspace);
    let bootstrapped = catalog
        .leadership_scope_view(leadership_scope, offered_at)?
        .ok_or("native Workspace leadership bootstrap missing")?;
    let confirmed = catalog.confirm_bootstrap_evidence(crate::ConfirmBootstrapEvidenceCommand {
        scope: leadership_scope,
        assignment_id: forge_domain::AgentAssignmentId::from_uuid(leadership_assignment_id),
        expected_revision: bootstrapped.leadership_revision(),
        now_ms: offered_at + 1,
    })?;
    let authority = catalog
        .current_leadership_authority(leadership_scope)?
        .ok_or("native Workspace leader missing")?;
    assert_eq!(authority.participant_id(), sender_participant);
    assert_eq!(
        authority.leadership_revision(),
        confirmed.leadership_revision()
    );

    let target = crate::ProviderMessageTarget::new(
        workspace,
        profile_id,
        participation_id,
        runtime_session_id,
        "native-node-conversation".to_owned(),
    )?;
    let witness = crate::NodeMessageRouteWitness::new(
        operation_id,
        1,
        1,
        sender,
        Some(authority.leadership_revision()),
        crate::NodeMessageRouteTarget::Agent {
            assignment_id,
            assignment_revision: 1,
            agent_id: recipient_agent.into_uuid(),
            session_id: recipient_session.into_uuid(),
        },
    )?;
    Ok((target, witness, authority.leadership_revision()))
}

#[test]
fn ready_registration_without_canonical_native_marker_cannot_claim_native_actor()
-> Result<(), Box<dyn Error>> {
    let destination = Uuid::from_u128(0xFC10);
    let seed = 0xFC11;
    let mut source = source_fixture(seed, 1, false, destination)?;
    let (catalog_root, catalog_path) = catalog_root(seed)?;
    let mut catalog = OperationsCatalog::open(&catalog_path, destination)?;
    let _prepared = catalog.prepare(&source.freeze)?;
    import_all(&mut catalog, &source)?;
    verify_all(&mut catalog, &source)?;

    let workspace = source.workspace;
    let activity = ActivityId::from_uuid(Uuid::from_u128(seed + 2));
    let session = AgentSessionId::from_uuid(Uuid::from_u128(seed + 0x100));
    let agent = AgentInstanceId::from_uuid(Uuid::from_u128(seed + 0x101));
    let participant_id = Uuid::from_u128(seed + 0x102);
    assert_ne!(
        participant_id,
        crate::native_session_participation_id(workspace.into_uuid(), session.into_uuid())
    );
    let principal = AuthenticatedPrincipal::DirectToken(DirectTokenId::from_uuid(Uuid::from_u128(
        seed + 0x103,
    )));
    let binding = AgentBindingReservationRequest::new(
        Uuid::from_u128(seed + 0x104),
        activity,
        agent,
        session,
        principal,
        timestamp(1_001)?,
    )?;
    let request = RegistrationRequest::new(
        participant_id,
        binding.clone(),
        AgentDisplayName::new("Unmarked participant".to_owned())?,
        AgentDeclaredRole::new("coordinator".to_owned())?,
        AgentTransportKind::StreamableHttp,
    )?;
    let _pending = catalog.prepare_registration(source.migration, &request)?;
    let committed = reserve_and_commit_registration(
        &mut source,
        &mut catalog,
        &binding,
        &[],
        timestamp(1_002)?,
        timestamp(2_000)?,
    )?;
    let delegation =
        source
            .store
            .delegate_agent_operations(&source.freeze, [0xFC; 32], timestamp(2_500)?)?;
    activate_registration(
        &mut source,
        &mut catalog,
        &delegation,
        &binding,
        &committed,
        timestamp(3_000)?,
    )?;
    assert_eq!(
        catalog.ready_participant_for_agent(workspace, activity, agent)?,
        Some(participant_id)
    );

    let result = catalog.append_workspace_message(crate::AppendWorkspaceMessageCommand {
        message_id: Uuid::from_u128(seed + 0x105),
        workspace_id: workspace,
        channel: crate::MessageChannel::Workspace,
        actor: crate::OperationalActor::Participant {
            activity_id: activity,
            agent_id: agent,
            session_id: session,
        },
        body: "Must not infer native identity from an absent Profile scope".to_owned(),
        submitted_at: timestamp(3_001)?,
        context: crate::MessageContext::default(),
    });
    assert!(matches!(
        result,
        Err(OperationsError::Conflict(
            "operational actor has no current logical participation"
        ))
    ));

    drop(catalog);
    source.cleanup()?;
    fs::remove_dir_all(catalog_root)?;
    Ok(())
}

#[test]
#[allow(clippy::too_many_lines)]
fn registration_stops_at_pending_and_reserved_without_source_agent_rows()
-> Result<(), Box<dyn Error>> {
    let destination = Uuid::from_u128(0xFA00);
    let seed = 0xFA01;
    let mut source = source_fixture(seed, 1, false, destination)?;
    let second_activity = ActivityId::from_uuid(Uuid::from_u128(seed + 0x200));
    source.store.bootstrap_activity(&BootstrapActivity::new(
        second_activity,
        source.workspace,
        WorkspaceRevision::INITIAL,
        RuntimeInstanceId::from_uuid(Uuid::from_u128(seed + 0x201)),
        timestamp(100_000)?,
        timestamp(20)?,
    ))?;
    let (root, path) = catalog_root(seed)?;
    let mut catalog = OperationsCatalog::open(&path, destination)?;
    let _prepared = catalog.prepare(&source.freeze)?;
    import_all(&mut catalog, &source)?;
    let verified = verify_all(&mut catalog, &source)?;
    assert_eq!(verified.phase(), ImportPhase::Verified);

    let request_id = Uuid::from_u128(0xFA10);
    let session = AgentSessionId::from_uuid(Uuid::from_u128(0xFA11));
    let agent_id = AgentInstanceId::from_uuid(Uuid::from_u128(0xFA12));
    let binding = AgentBindingReservationRequest::new(
        request_id,
        ActivityId::from_uuid(Uuid::from_u128(seed + 2)),
        agent_id,
        session,
        AuthenticatedPrincipal::TrustedStdio,
        timestamp(1_001)?,
    )?;
    let request = RegistrationRequest::new(
        Uuid::from_u128(0xFA13),
        binding.clone(),
        AgentDisplayName::new("pending worker".to_owned())?,
        AgentDeclaredRole::new("worker".to_owned())?,
        AgentTransportKind::Stdio,
    )?;
    let pending = catalog.prepare_registration(source.migration, &request)?;
    assert_eq!(pending.phase(), RegistrationPhase::Pending);
    assert_eq!(pending.participant_id(), request.proposed_participant_id());
    assert_eq!(
        catalog.prepare_registration(source.migration, &request)?,
        pending
    );

    let second_binding = AgentBindingReservationRequest::new(
        Uuid::from_u128(0xFA20),
        ActivityId::from_uuid(Uuid::from_u128(seed + 0x200)),
        AgentInstanceId::from_uuid(Uuid::from_u128(0xFA21)),
        session,
        AuthenticatedPrincipal::TrustedStdio,
        timestamp(1_003)?,
    )?;
    let second_request = RegistrationRequest::new(
        Uuid::from_u128(0xFA22),
        second_binding.clone(),
        AgentDisplayName::new("pending worker".to_owned())?,
        AgentDeclaredRole::new("worker".to_owned())?,
        AgentTransportKind::Stdio,
    )?;
    let second_pending = catalog.prepare_registration(source.migration, &second_request)?;
    assert_eq!(second_pending.phase(), RegistrationPhase::Pending);
    assert_eq!(second_pending.participant_id(), pending.participant_id());

    let third_binding = AgentBindingReservationRequest::new(
        Uuid::from_u128(0xFA30),
        ActivityId::from_uuid(Uuid::from_u128(seed + 0x300)),
        AgentInstanceId::from_uuid(Uuid::from_u128(0xFA31)),
        AgentSessionId::from_uuid(Uuid::from_u128(0xFA32)),
        AuthenticatedPrincipal::TrustedStdio,
        timestamp(1_005)?,
    )?;
    let third_request = RegistrationRequest::new(
        Uuid::from_u128(0xFA33),
        third_binding.clone(),
        AgentDisplayName::new("pending second worker".to_owned())?,
        AgentDeclaredRole::new("worker".to_owned())?,
        AgentTransportKind::Stdio,
    )?;
    let third_pending = catalog.prepare_registration(source.migration, &third_request)?;
    assert_eq!(third_pending.phase(), RegistrationPhase::Pending);

    let changed_payload = RegistrationRequest::new(
        request.proposed_participant_id(),
        binding.clone(),
        AgentDisplayName::new("changed metadata".to_owned())?,
        AgentDeclaredRole::new("worker".to_owned())?,
        AgentTransportKind::Stdio,
    )?;
    assert!(matches!(
        catalog.prepare_registration(source.migration, &changed_payload),
        Err(OperationsError::Conflict("registration request replay"))
    ));
    let conflicting_metadata = RegistrationRequest::new(
        Uuid::from_u128(0xFA40),
        AgentBindingReservationRequest::new(
            Uuid::from_u128(0xFA41),
            ActivityId::from_uuid(Uuid::from_u128(seed + 0x400)),
            AgentInstanceId::from_uuid(Uuid::from_u128(0xFA42)),
            session,
            AuthenticatedPrincipal::TrustedStdio,
            timestamp(1_006)?,
        )?,
        AgentDisplayName::new("conflicting metadata".to_owned())?,
        AgentDeclaredRole::new("worker".to_owned())?,
        AgentTransportKind::Stdio,
    )?;
    assert!(matches!(
        catalog.prepare_registration(source.migration, &conflicting_metadata),
        Err(OperationsError::Conflict("pending participant identity"))
    ));

    let source_connection = Connection::open(source.layout.database_path())?;
    let before: i64 = source_connection.query_row(
        "SELECT COUNT(*) FROM agent_instances WHERE workspace_id=?1",
        params![source.workspace.into_uuid().as_bytes()],
        |row| row.get(0),
    )?;
    drop(source_connection);
    let reservation =
        source
            .store
            .reserve_agent_binding(&source.freeze, &binding, timestamp(1_002)?)?;
    let reserved = catalog.record_registration_reservation(&reservation)?;
    assert_eq!(reserved.phase(), RegistrationPhase::Reserved);
    assert_eq!(reserved.reserved_at(), Some(reservation.reserved_at()));
    assert_eq!(
        catalog.record_registration_reservation(&reservation)?,
        reserved
    );

    let second_reservation =
        source
            .store
            .reserve_agent_binding(&source.freeze, &second_binding, timestamp(1_004)?)?;
    let second_reserved = catalog.record_registration_reservation(&second_reservation)?;
    assert_eq!(second_reserved.phase(), RegistrationPhase::Reserved);
    assert_eq!(second_reserved.participant_id(), reserved.participant_id());
    assert_eq!(
        catalog.record_registration_reservation(&second_reservation)?,
        second_reserved
    );
    assert_eq!(
        catalog.registration_progress(request_id)?,
        Some(reserved.clone())
    );

    let source_connection = Connection::open(source.layout.database_path())?;
    let after: i64 = source_connection.query_row(
        "SELECT COUNT(*) FROM agent_instances WHERE workspace_id=?1",
        params![source.workspace.into_uuid().as_bytes()],
        |row| row.get(0),
    )?;
    assert_eq!(after, before, "reservation inserted a source Agent row");
    drop(source_connection);

    drop(catalog);
    source.reopen_store()?;
    let mut catalog = OperationsCatalog::open(&path, destination)?;
    assert_eq!(
        catalog.registration_progress(third_binding.request_id())?,
        Some(third_pending.clone())
    );
    assert_eq!(
        catalog.registration_progress(request_id)?,
        Some(reserved.clone())
    );
    assert_eq!(
        catalog
            .registration_progress(second_binding.request_id())?
            .ok_or("second reservation progress missing")?
            .phase(),
        RegistrationPhase::Reserved
    );
    let reopened_reservation = source
        .store
        .agent_binding_reservation(request_id)?
        .ok_or("first source reservation missing after reopen")?;
    assert_eq!(
        catalog.record_registration_reservation(&reopened_reservation)?,
        reserved
    );
    let reopened_second_reservation = source
        .store
        .agent_binding_reservation(second_binding.request_id())?
        .ok_or("second source reservation missing after reopen")?;
    assert_eq!(
        catalog.record_registration_reservation(&reopened_second_reservation)?,
        second_reserved
    );
    drop(catalog);
    source.cleanup()?;
    fs::remove_dir_all(root)?;
    Ok(())
}

struct MembershipFixture {
    source: SourceFixture,
    catalog: OperationsCatalog,
    catalog_root: PathBuf,
    catalog_path: PathBuf,
    first_binding: AgentBindingReservationRequest,
    second_binding: AgentBindingReservationRequest,
    first_pending: crate::RegistrationProgress,
    second_pending: crate::RegistrationProgress,
}

fn membership_fixture(seed: u128, destination: Uuid) -> Result<MembershipFixture, Box<dyn Error>> {
    let mut source = source_fixture(seed, 1, false, destination)?;
    source.store.bootstrap_activity(&BootstrapActivity::new(
        ActivityId::from_uuid(Uuid::from_u128(seed + 0x200)),
        source.workspace,
        WorkspaceRevision::INITIAL,
        RuntimeInstanceId::from_uuid(Uuid::from_u128(seed + 0x201)),
        timestamp(100_000)?,
        timestamp(20)?,
    ))?;
    let (root, path) = catalog_root(seed)?;
    let mut catalog = OperationsCatalog::open(&path, destination)?;
    catalog.prepare(&source.freeze)?;
    import_all(&mut catalog, &source)?;
    assert_eq!(
        verify_all(&mut catalog, &source)?.phase(),
        ImportPhase::Verified
    );

    let session = AgentSessionId::from_uuid(Uuid::from_u128(0xFE10));
    let first_binding = AgentBindingReservationRequest::new(
        Uuid::from_u128(0xFE11),
        ActivityId::from_uuid(Uuid::from_u128(seed + 2)),
        AgentInstanceId::from_uuid(Uuid::from_u128(0xFE12)),
        session,
        AuthenticatedPrincipal::TrustedStdio,
        timestamp(1_001)?,
    )?;
    let first_request = RegistrationRequest::new(
        Uuid::from_u128(0xFE13),
        first_binding.clone(),
        AgentDisplayName::new("logical worker".to_owned())?,
        AgentDeclaredRole::new("worker".to_owned())?,
        AgentTransportKind::Stdio,
    )?;
    let second_binding = AgentBindingReservationRequest::new(
        Uuid::from_u128(0xFE14),
        ActivityId::from_uuid(Uuid::from_u128(seed + 0x200)),
        AgentInstanceId::from_uuid(Uuid::from_u128(0xFE15)),
        session,
        AuthenticatedPrincipal::TrustedStdio,
        timestamp(1_002)?,
    )?;
    let second_request = RegistrationRequest::new(
        Uuid::from_u128(0xFE16),
        second_binding.clone(),
        AgentDisplayName::new("logical worker".to_owned())?,
        AgentDeclaredRole::new("worker".to_owned())?,
        AgentTransportKind::Stdio,
    )?;
    let first_pending = catalog.prepare_registration(source.migration, &first_request)?;
    let second_pending = catalog.prepare_registration(source.migration, &second_request)?;
    Ok(MembershipFixture {
        source,
        catalog,
        catalog_root: root,
        catalog_path: path,
        first_binding,
        second_binding,
        first_pending,
        second_pending,
    })
}

fn reserve_two_bindings(
    source: &mut SourceFixture,
    catalog: &mut OperationsCatalog,
    first: &AgentBindingReservationRequest,
    second: &AgentBindingReservationRequest,
) -> Result<(), Box<dyn Error>> {
    let first_reservation =
        source
            .store
            .reserve_agent_binding(&source.freeze, first, timestamp(1_003)?)?;
    let _reserved = catalog.record_registration_reservation(&first_reservation)?;
    let second_reservation =
        source
            .store
            .reserve_agent_binding(&source.freeze, second, timestamp(1_004)?)?;
    let _reserved = catalog.record_registration_reservation(&second_reservation)?;
    Ok(())
}

fn prepare_inbox_actor(
    catalog: &mut OperationsCatalog,
    migration_id: Uuid,
    activity_id: ActivityId,
) -> Result<(AgentBindingReservationRequest, Uuid), Box<dyn Error>> {
    let binding = AgentBindingReservationRequest::new(
        Uuid::from_u128(0xFE72),
        activity_id,
        AgentInstanceId::from_uuid(Uuid::from_u128(0xFE73)),
        AgentSessionId::from_uuid(Uuid::from_u128(0xFE74)),
        AuthenticatedPrincipal::TrustedStdio,
        timestamp(1_011)?,
    )?;
    let pending = catalog.prepare_registration(
        migration_id,
        &RegistrationRequest::new(
            Uuid::from_u128(0xFE75),
            binding.clone(),
            AgentDisplayName::new("logical inbox actor".to_owned())?,
            AgentDeclaredRole::new("worker".to_owned())?,
            AgentTransportKind::Stdio,
        )?,
    )?;
    Ok((binding, pending.participant_id()))
}

fn reopen_inbox_catalog_from_v4(
    path: &Path,
    destination: Uuid,
) -> Result<OperationsCatalog, Box<dyn Error>> {
    let connection = Connection::open(path)?;
    crate::workspace_participation::drop_schema_for_legacy_fixture(&connection)?;
    connection.execute_batch(
        "DROP TABLE ops_admission_records; DROP TABLE ops_approval_requests; DROP TABLE ops_approval_policy; DROP TABLE ops_issuer_config; DROP TABLE ops_leadership_confirm_intents; DROP TABLE ops_ballots; DROP TABLE ops_election_electorate; DROP TABLE ops_leadership_elections; DROP TABLE ops_leadership_receipts; DROP TABLE ops_leadership; DROP TABLE ops_work_credit_entries; DROP TABLE ops_assignment_submit_source_refs; DROP TABLE ops_assignment_submit_projection; DROP TABLE ops_assignment_offer_projection; DROP TABLE ops_assignment_review_projection; DROP TABLE ops_change_set_projection; DROP TABLE ops_execution_terminal_projection; DROP TABLE ops_effect_fact_consumers; DROP TABLE ops_logical_departure_receipts; DROP TABLE ops_logical_departures; DROP TABLE ops_logical_departure_events; DROP TABLE ops_legacy_adoptions; DROP TABLE ops_current_effect_bindings; DROP TRIGGER ops_logical_receipt_state_insert; DROP TABLE ops_logical_receipt_state; PRAGMA user_version=4;",
    )?;
    drop(connection);
    let catalog = OperationsCatalog::open(path, destination)?;
    let version: i64 = catalog
        .connection
        .pragma_query_value(None, "user_version", |row| row.get(0))?;
    assert_eq!(version, crate::catalog::VERSION);
    Ok(catalog)
}

fn acknowledge_logical_inbox_once(
    catalog: &mut OperationsCatalog,
    query: LogicalInboxQuery,
    target: LogicalInboxTarget,
    principal: AuthenticatedPrincipal,
    event_id: Uuid,
) -> Result<(), Box<dyn Error>> {
    let second_read = catalog.list_logical_inbox(query)?;
    assert_eq!(
        second_read[0].receipt_revision(),
        InboxReceiptRevision::INITIAL
    );
    assert_eq!(second_read[0].acknowledged_at(), None);
    assert_eq!(
        catalog.logical_inbox_entry(target, principal, event_id)?,
        Some(second_read[0].clone())
    );
    let acknowledged = catalog.acknowledge_logical_inbox(LogicalInboxAck::new(
        target,
        principal,
        event_id,
        InboxReceiptRevision::INITIAL,
        timestamp(3_500)?,
    ))?;
    assert_eq!(acknowledged.receipt_revision().get(), 2);
    assert_eq!(acknowledged.acknowledged_at(), Some(timestamp(3_500)?));
    assert_eq!(
        catalog.logical_inbox_entry(target, principal, event_id)?,
        Some(acknowledged.clone())
    );
    assert!(matches!(
        catalog.acknowledge_logical_inbox(LogicalInboxAck::new(
            target,
            principal,
            event_id,
            InboxReceiptRevision::INITIAL,
            timestamp(3_501)?,
        )),
        Err(OperationsError::Conflict("logical inbox receipt revision"))
    ));
    Ok(())
}

fn assert_initial_logical_inbox_entry(
    entry: &LogicalInboxEntry,
    recipient_participant_id: Uuid,
    actor_participant_id: Uuid,
    actor_agent_id: AgentInstanceId,
) -> Uuid {
    assert_eq!(entry.recipient_participant_id(), recipient_participant_id);
    assert_eq!(entry.actor_participant_id(), actor_participant_id);
    assert_eq!(entry.actor_agent_id(), actor_agent_id);
    assert_eq!(entry.receipt_revision(), InboxReceiptRevision::INITIAL);
    assert_eq!(entry.acknowledged_at(), None);
    entry.event_id()
}

fn assert_logical_activity_snapshot(
    catalog: &OperationsCatalog,
    workspace_id: WorkspaceId,
    activity_id: ActivityId,
    expected_participants: &[Uuid],
    expected_unread: u32,
) -> Result<(), Box<dyn Error>> {
    let snapshot = catalog.logical_activity_snapshot(workspace_id, activity_id)?;
    assert_eq!(snapshot.workspace_id(), workspace_id);
    assert_eq!(snapshot.activity_id(), activity_id);
    assert_eq!(snapshot.unread_count(), expected_unread);
    assert_eq!(snapshot.unread_join_count(), expected_unread);
    assert_eq!(snapshot.participants().len(), expected_participants.len());
    assert_eq!(snapshot.events().len(), expected_participants.len());
    for participant_id in expected_participants {
        let participant = snapshot
            .participants()
            .iter()
            .find(|value| value.participant_id() == *participant_id)
            .ok_or("logical activity participant missing")?;
        assert_eq!(
            participant.presence_revision(),
            AgentPresenceRevision::INITIAL
        );
        assert_eq!(participant.last_authenticated_at(), None);
        assert!(
            snapshot
                .events()
                .iter()
                .any(|event| event.participant_id() == *participant_id)
        );
    }
    Ok(())
}

fn assert_reopened_inbox_and_activity(
    catalog_path: &Path,
    destination: Uuid,
    query: LogicalInboxQuery,
    receipt: (LogicalInboxTarget, AuthenticatedPrincipal, Uuid),
    activity: (WorkspaceId, ActivityId, [Uuid; 2]),
) -> Result<(), Box<dyn Error>> {
    let (target, principal, event_id) = receipt;
    let (workspace_id, activity_id, participant_ids) = activity;
    let catalog = OperationsCatalog::open(catalog_path, destination)?;
    let after_reopen = catalog.list_logical_inbox(query)?;
    assert_eq!(after_reopen[0].receipt_revision().get(), 2);
    assert_eq!(after_reopen[0].acknowledged_at(), Some(timestamp(3_500)?));
    assert_eq!(
        catalog.logical_inbox_entry(target, principal, event_id)?,
        Some(after_reopen[0].clone())
    );
    assert_logical_activity_snapshot(&catalog, workspace_id, activity_id, &participant_ids, 0)?;
    Ok(())
}

fn reserve_and_commit_registration(
    source: &mut SourceFixture,
    catalog: &mut OperationsCatalog,
    binding: &AgentBindingReservationRequest,
    recipients: &[Uuid],
    reserve_at: TimestampMillis,
    commit_at: TimestampMillis,
) -> Result<crate::MembershipCommitReceipt, Box<dyn Error>> {
    let reservation = source
        .store
        .reserve_agent_binding(&source.freeze, binding, reserve_at)?;
    let _reserved = catalog.record_registration_reservation(&reservation)?;
    Ok(catalog.commit_registration_membership(binding.request_id(), recipients, commit_at)?)
}

fn activate_registration(
    source: &mut SourceFixture,
    catalog: &mut OperationsCatalog,
    delegation: &AgentOperationsDelegation,
    binding: &AgentBindingReservationRequest,
    commit: &crate::MembershipCommitReceipt,
    observed_at: TimestampMillis,
) -> Result<(), Box<dyn Error>> {
    let activation =
        activate_source_registration(source, delegation, binding, commit, observed_at)?;
    let _recorded = catalog.record_registration_activation(&activation)?;
    Ok(())
}

fn activate_source_registration(
    source: &mut SourceFixture,
    delegation: &AgentOperationsDelegation,
    binding: &AgentBindingReservationRequest,
    commit: &crate::MembershipCommitReceipt,
    observed_at: TimestampMillis,
) -> Result<forge_persistence::AgentBindingActivation, Box<dyn Error>> {
    let request = AgentBindingActivationRequest::new(
        binding.request_id(),
        commit.commit_digest(),
        commit.display_name().clone(),
        commit.declared_role().clone(),
        commit.transport(),
    )?;
    Ok(source
        .store
        .activate_agent_binding(delegation, &request, observed_at)?)
}

fn insert_deferred_assignment_offer(
    catalog: &OperationsCatalog,
    source: &SourceFixture,
    destination: Uuid,
    binding: &AgentBindingReservationRequest,
    assignment_id: Uuid,
) -> Result<(), Box<dyn Error>> {
    catalog.connection.execute(
        "INSERT OR IGNORE INTO ops_effect_fact_consumers(migration_id,workspace_id,destination_id,last_sequence,applied_count) VALUES(?1,?2,?3,0,0)",
        params![
            source.migration.as_bytes(),
            source.workspace.into_uuid().as_bytes(),
            destination.as_bytes(),
        ],
    )?;
    let sequence: i64 = catalog.connection.query_row(
        "SELECT COALESCE(MAX(source_sequence),0)+1 FROM ops_assignment_offer_projection WHERE migration_id=?1",
        params![source.migration.as_bytes()],
        |row| row.get(0),
    )?;
    catalog.connection.execute(
        "INSERT INTO ops_assignment_offer_projection(migration_id,source_sequence,fact_digest,assignment_id,workspace_id,activity_id,goal_id,node_id,target_agent_id,target_session_id,credit_budget,offered_at_ms) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,NULL,NULL,?10)",
        params![
            source.migration.as_bytes(),
            sequence,
            [7_u8; 32].as_slice(),
            assignment_id.as_bytes(),
            source.workspace.into_uuid().as_bytes(),
            binding.activity_id().into_uuid().as_bytes(),
            Uuid::from_u128(0xFE5B).as_bytes(),
            Uuid::from_u128(0xFE5C).as_bytes(),
            binding.agent_id().into_uuid().as_bytes(),
            2_100_i64,
        ],
    )?;
    Ok(())
}

fn commit_two_bindings(
    catalog: &mut OperationsCatalog,
    first: &AgentBindingReservationRequest,
    second: &AgentBindingReservationRequest,
    participant: Uuid,
) -> Result<
    (
        crate::MembershipCommitReceipt,
        crate::MembershipCommitReceipt,
    ),
    Box<dyn Error>,
> {
    let recipients = [participant];
    let first_commit = catalog.commit_registration_membership(
        first.request_id(),
        &recipients,
        timestamp(2_000)?,
    )?;
    let second_commit = catalog.commit_registration_membership(
        second.request_id(),
        &recipients,
        timestamp(2_001)?,
    )?;
    Ok((first_commit, second_commit))
}

fn logical_presence_touch(
    workspace_id: WorkspaceId,
    binding: &AgentBindingReservationRequest,
    expected_revision: AgentPresenceRevision,
    observed_at: TimestampMillis,
) -> LogicalPresenceTouch {
    LogicalPresenceTouch::new(
        LogicalPresenceTarget::new(
            workspace_id,
            binding.activity_id(),
            binding.agent_id(),
            binding.session_id(),
        ),
        binding.principal(),
        expected_revision,
        observed_at,
    )
}

fn assert_logical_presence_absence_and_scope(
    catalog: &OperationsCatalog,
    workspace_id: WorkspaceId,
    binding: &AgentBindingReservationRequest,
    participant_id: Uuid,
) -> Result<LogicalPresenceTarget, Box<dyn Error>> {
    let target = LogicalPresenceTarget::new(
        workspace_id,
        binding.activity_id(),
        binding.agent_id(),
        binding.session_id(),
    );
    assert_eq!(catalog.logical_presence(target, binding.principal())?, None);
    let absent_rows: i64 = catalog.connection.query_row(
        "SELECT COUNT(*) FROM ops_logical_presence WHERE workspace_id=?1 AND participant_id=?2",
        params![
            workspace_id.into_uuid().as_bytes(),
            participant_id.as_bytes()
        ],
        |row| row.get(0),
    )?;
    assert_eq!(absent_rows, 0, "presence read created a row");

    for foreign_target in [
        LogicalPresenceTarget::new(
            WorkspaceId::from_uuid(Uuid::from_u128(0xFE5A)),
            binding.activity_id(),
            binding.agent_id(),
            binding.session_id(),
        ),
        LogicalPresenceTarget::new(
            workspace_id,
            ActivityId::from_uuid(Uuid::from_u128(0xFE5F)),
            binding.agent_id(),
            binding.session_id(),
        ),
        LogicalPresenceTarget::new(
            workspace_id,
            binding.activity_id(),
            AgentInstanceId::from_uuid(Uuid::from_u128(0xFE5C)),
            binding.session_id(),
        ),
        LogicalPresenceTarget::new(
            workspace_id,
            binding.activity_id(),
            binding.agent_id(),
            AgentSessionId::from_uuid(Uuid::from_u128(0xFE5D)),
        ),
    ] {
        assert_ne!(
            foreign_target, target,
            "foreign selector must differ from the real binding"
        );
        assert!(matches!(
            catalog.logical_presence(foreign_target, binding.principal()),
            Err(OperationsError::Conflict("logical member not ready"))
        ));
    }
    assert!(matches!(
        catalog.logical_presence(
            target,
            AuthenticatedPrincipal::DirectToken(forge_domain::DirectTokenId::from_uuid(
                Uuid::from_u128(0xFE5E),
            )),
        ),
        Err(OperationsError::Conflict("logical binding principal"))
    ));
    Ok(target)
}

fn assert_logical_rows(
    catalog: &OperationsCatalog,
    workspace: WorkspaceId,
) -> Result<(), Box<dyn Error>> {
    for (table, expected) in [
        ("ops_logical_members", 1_i64),
        ("ops_logical_events", 1_i64),
        ("ops_logical_receipts", 1_i64),
        ("ops_logical_bindings", 2_i64),
    ] {
        let count: i64 = catalog.connection.query_row(
            &format!("SELECT COUNT(*) FROM {table} WHERE workspace_id=?1"),
            params![workspace.into_uuid().as_bytes()],
            |row| row.get(0),
        )?;
        assert_eq!(count, expected, "unexpected {table} count");
    }
    Ok(())
}

fn assert_visible_projection(
    catalog: &OperationsCatalog,
    workspace: WorkspaceId,
) -> Result<(), Box<dyn Error>> {
    assert_eq!(catalog.visible_logical_members(workspace, 64)?.len(), 1);
    assert_eq!(catalog.visible_logical_events(workspace, 64)?.len(), 1);
    assert_eq!(catalog.visible_logical_receipts(workspace, 64)?.len(), 1);
    Ok(())
}

#[test]
fn membership_commit_reuses_one_joined_event_for_two_bindings() -> Result<(), Box<dyn Error>> {
    let destination = Uuid::from_u128(0xFE00);
    let MembershipFixture {
        mut source,
        mut catalog,
        catalog_root,
        catalog_path,
        first_binding,
        second_binding,
        first_pending,
        second_pending,
    } = membership_fixture(0xFE01, destination)?;
    assert_eq!(
        first_pending.participant_id(),
        second_pending.participant_id()
    );
    reserve_two_bindings(&mut source, &mut catalog, &first_binding, &second_binding)?;

    let recipients = [first_pending.participant_id()];
    let first_commit = catalog.commit_registration_membership(
        first_binding.request_id(),
        &recipients,
        timestamp(2_000)?,
    )?;
    assert_eq!(
        first_commit.participant_id(),
        first_pending.participant_id()
    );
    assert_eq!(
        catalog
            .registration_progress(first_binding.request_id())?
            .map(|value| value.phase()),
        Some(RegistrationPhase::MembershipCommitted)
    );
    assert_eq!(
        catalog.commit_registration_membership(
            first_binding.request_id(),
            &recipients,
            timestamp(3_000)?
        )?,
        first_commit.clone()
    );
    assert!(matches!(
        catalog.commit_registration_membership(first_binding.request_id(), &[], timestamp(3_001)?),
        Err(OperationsError::Conflict("membership commit replay"))
    ));
    let second_commit = catalog.commit_registration_membership(
        second_binding.request_id(),
        &recipients,
        timestamp(2_001)?,
    )?;
    assert_eq!(
        second_commit.participant_id(),
        first_commit.participant_id()
    );
    assert_eq!(
        second_commit.joined_event_id(),
        first_commit.joined_event_id()
    );
    assert_ne!(second_commit.commit_digest(), first_commit.commit_digest());

    assert_logical_rows(&catalog, source.workspace)?;

    drop(catalog);
    source.reopen_store()?;
    let catalog = OperationsCatalog::open(&catalog_path, destination)?;
    assert_eq!(
        catalog.membership_commit(first_binding.request_id())?,
        Some(first_commit)
    );
    assert_eq!(
        catalog.membership_commit(second_binding.request_id())?,
        Some(second_commit)
    );
    assert_eq!(
        catalog
            .registration_progress(second_binding.request_id())?
            .ok_or("membership progress missing")?
            .phase(),
        RegistrationPhase::MembershipCommitted
    );
    assert!(
        source
            .store
            .agent_binding_reservation(first_binding.request_id())?
            .is_some()
    );
    assert!(
        source
            .store
            .agent_binding_reservation(second_binding.request_id())?
            .is_some()
    );
    drop(catalog);
    source.cleanup()?;
    fs::remove_dir_all(catalog_root)?;
    Ok(())
}

#[test]
fn activation_receipt_readies_only_its_binding_and_exposes_one_participant()
-> Result<(), Box<dyn Error>> {
    let destination = Uuid::from_u128(0xFE50);
    let MembershipFixture {
        mut source,
        mut catalog,
        catalog_root,
        catalog_path,
        first_binding,
        second_binding,
        first_pending,
        second_pending: _,
    } = membership_fixture(0xFE51, destination)?;
    reserve_two_bindings(&mut source, &mut catalog, &first_binding, &second_binding)?;
    let (first_commit, second_commit) = commit_two_bindings(
        &mut catalog,
        &first_binding,
        &second_binding,
        first_pending.participant_id(),
    )?;
    let delegation =
        source
            .store
            .delegate_agent_operations(&source.freeze, [0x52; 32], timestamp(2_500)?)?;
    let first_request = AgentBindingActivationRequest::new(
        first_binding.request_id(),
        first_commit.commit_digest(),
        first_commit.display_name().clone(),
        first_commit.declared_role().clone(),
        first_commit.transport(),
    )?;
    let source_activation =
        source
            .store
            .activate_agent_binding(&delegation, &first_request, timestamp(3_000)?)?;
    let activation = catalog.record_registration_activation(&source_activation)?;
    assert_eq!(
        catalog
            .registration_progress(first_binding.request_id())?
            .map(|value| value.phase()),
        Some(RegistrationPhase::Ready)
    );
    assert_eq!(
        catalog
            .registration_progress(second_binding.request_id())?
            .map(|value| value.phase()),
        Some(RegistrationPhase::MembershipCommitted)
    );
    assert_visible_projection(&catalog, source.workspace)?;
    assert_corrupt_activation_is_rejected(&catalog, &first_binding, source.workspace)?;
    assert_current_projection_survives_historical_proof_corruption(
        &catalog,
        &first_binding,
        source.workspace,
    )?;
    assert_eq!(
        catalog.record_registration_activation(&source_activation)?,
        activation
    );
    assert_eq!(
        catalog.registration_activation(first_binding.request_id())?,
        Some(activation.clone())
    );

    let wrong_request = AgentBindingActivationRequest::new(
        second_binding.request_id(),
        [0x53; 32],
        second_commit.display_name().clone(),
        second_commit.declared_role().clone(),
        second_commit.transport(),
    )?;
    let wrong_source_activation =
        source
            .store
            .activate_agent_binding(&delegation, &wrong_request, timestamp(3_001)?)?;
    assert!(matches!(
        catalog.record_registration_activation(&wrong_source_activation),
        Err(OperationsError::Conflict("activation source identity"))
    ));
    assert_eq!(
        catalog.registration_activation(second_binding.request_id())?,
        None
    );
    drop(catalog);
    source.reopen_store()?;
    let catalog = OperationsCatalog::open(&catalog_path, destination)?;
    assert_eq!(
        catalog.registration_activation(first_binding.request_id())?,
        Some(activation)
    );
    drop(catalog);
    source.cleanup()?;
    std::fs::remove_dir_all(catalog_root)?;
    Ok(())
}

#[test]
fn member_list_counts_logical_participants_once_across_activity_bindings()
-> Result<(), Box<dyn Error>> {
    let destination = Uuid::from_u128(0xFEA0);
    let MembershipFixture {
        mut source,
        mut catalog,
        catalog_root,
        first_binding,
        second_binding,
        first_pending,
        ..
    } = membership_fixture(0xFEA1, destination)?;
    reserve_two_bindings(&mut source, &mut catalog, &first_binding, &second_binding)?;
    let (first, second) = commit_two_bindings(
        &mut catalog,
        &first_binding,
        &second_binding,
        first_pending.participant_id(),
    )?;
    let delegation =
        source
            .store
            .delegate_agent_operations(&source.freeze, [0xA2; 32], timestamp(2_500)?)?;
    for (binding, commit, at) in [
        (&first_binding, &first, 3_000),
        (&second_binding, &second, 3_001),
    ] {
        let receipt = activate_source_registration(
            &mut source,
            &delegation,
            binding,
            commit,
            timestamp(at)?,
        )?;
        let _recorded = catalog.record_registration_activation(&receipt)?;
    }
    let members = catalog.visible_logical_members(source.workspace, 64)?;
    assert_eq!(
        members.len(),
        1,
        "Activity bindings must not duplicate a logical participant"
    );
    assert_eq!(members[0].participant_id(), first_pending.participant_id());
    assert_eq!(members[0].session_id(), first_binding.session_id());
    assert_eq!(
        catalog.visible_logical_members(source.workspace, 1)?,
        members
    );
    // A corrupt second Activity must not hide behind this participant's valid
    // binding at the logical-member limit.
    catalog.connection.execute(
        "UPDATE ops_current_effect_bindings SET session_id=?1 WHERE workspace_id=?2 AND activity_id=?3",
        params![
            Uuid::from_u128(u128::MAX).as_bytes(),
            source.workspace.into_uuid().as_bytes(),
            second_binding.activity_id().into_uuid().as_bytes(),
        ],
    )?;
    for limit in [1, 64] {
        assert!(matches!(
            catalog.visible_logical_members(source.workspace, limit),
            Err(OperationsError::Corrupt(_))
        ));
    }
    drop(catalog);
    source.cleanup()?;
    std::fs::remove_dir_all(catalog_root)?;
    Ok(())
}

#[test]
fn activation_recovery_never_restores_retired_or_rebound_effects() -> Result<(), Box<dyn Error>> {
    let destination = Uuid::from_u128(0xFEB0);
    let MembershipFixture {
        mut source,
        mut catalog,
        catalog_root,
        catalog_path,
        first_binding,
        second_binding,
        first_pending,
        second_pending: _,
    } = membership_fixture(0xFEB1, destination)?;
    reserve_two_bindings(&mut source, &mut catalog, &first_binding, &second_binding)?;
    let (first_commit, second_commit) = commit_two_bindings(
        &mut catalog,
        &first_binding,
        &second_binding,
        first_pending.participant_id(),
    )?;
    let delegation =
        source
            .store
            .delegate_agent_operations(&source.freeze, [0xB2; 32], timestamp(2_500)?)?;
    let first_activation = activate_source_registration(
        &mut source,
        &delegation,
        &first_binding,
        &first_commit,
        timestamp(3_000)?,
    )?;
    let receipt = catalog.record_registration_activation(&first_activation)?;
    let tx = catalog.connection.unchecked_transaction()?;
    crate::effect_binding::retire_exact(
        &tx,
        source.workspace,
        first_pending.participant_id(),
        first_binding.activity_id(),
        first_binding.session_id(),
        first_binding.agent_id(),
    )?;
    tx.commit()?;
    drop(catalog);
    let mut catalog = OperationsCatalog::open(&catalog_path, destination)?;
    assert_eq!(
        catalog.record_registration_activation(&first_activation)?,
        receipt
    );
    assert!(
        crate::effect_binding::by_agent(
            &catalog.connection,
            source.workspace,
            first_binding.agent_id(),
        )?
        .is_none()
    );

    // Reproduce a legacy rebind that won before this pending activation arrived.
    let new_session = AgentSessionId::from_uuid(Uuid::from_u128(0xFEB3));
    catalog.connection.execute(
        "UPDATE ops_logical_members SET session_id=?1 WHERE workspace_id=?2 AND participant_id=?3",
        params![
            new_session.into_uuid().as_bytes(),
            source.workspace.into_uuid().as_bytes(),
            first_pending.participant_id().as_bytes()
        ],
    )?;
    let second_activation = activate_source_registration(
        &mut source,
        &delegation,
        &second_binding,
        &second_commit,
        timestamp(3_001)?,
    )?;
    assert!(matches!(
        catalog.record_registration_activation(&second_activation),
        Err(OperationsError::Conflict(
            "activation current member Session"
        ))
    ));
    assert_eq!(
        catalog.registration_activation(second_binding.request_id())?,
        None
    );
    assert_eq!(
        catalog.registration_activation(first_binding.request_id())?,
        Some(receipt)
    );
    assert!(
        catalog
            .logical_identity_for_session(source.workspace, new_session)?
            .ok_or("replacement member missing")?
            .effect_bindings()
            .is_empty()
    );
    drop(catalog);
    source.cleanup()?;
    std::fs::remove_dir_all(catalog_root)?;
    Ok(())
}

#[test]
fn ready_activation_retries_deferred_leadership_bootstrap() -> Result<(), Box<dyn Error>> {
    let destination = Uuid::from_u128(0xFE50);
    let MembershipFixture {
        mut source,
        mut catalog,
        catalog_root,
        catalog_path: _,
        first_binding,
        first_pending,
        second_binding: _,
        second_pending: _,
    } = membership_fixture(0xFE51, destination)?;
    let commit = reserve_and_commit_registration(
        &mut source,
        &mut catalog,
        &first_binding,
        &[],
        timestamp(1_010)?,
        timestamp(2_010)?,
    )?;
    let assignment_id = Uuid::from_u128(0xFE5A);
    insert_deferred_assignment_offer(
        &catalog,
        &source,
        destination,
        &first_binding,
        assignment_id,
    )?;
    assert!(
        catalog
            .leadership_view(source.workspace, Some(first_binding.activity_id()), 2_200)?
            .is_none()
    );
    assert!(
        catalog
            .leadership_view(source.workspace, None, 2_200)?
            .is_none()
    );

    let delegation =
        source
            .store
            .delegate_agent_operations(&source.freeze, [0x50; 32], timestamp(2_500)?)?;
    let request = AgentBindingActivationRequest::new(
        first_binding.request_id(),
        commit.commit_digest(),
        commit.display_name().clone(),
        commit.declared_role().clone(),
        commit.transport(),
    )?;
    let source_activation =
        source
            .store
            .activate_agent_binding(&delegation, &request, timestamp(3_000)?)?;
    let activation = catalog.record_registration_activation(&source_activation)?;
    assert_eq!(activation.participant_id(), first_pending.participant_id());

    let activity_view = catalog
        .leadership_view(source.workspace, Some(first_binding.activity_id()), 3_001)?
        .ok_or("activity leadership was not bootstrapped")?;
    assert_eq!(activity_view.state(), crate::LeadershipState::Bootstrapping);
    assert_eq!(
        activity_view.leader_participant_id(),
        Some(first_pending.participant_id())
    );
    let workspace_view = catalog
        .leadership_view(source.workspace, None, 3_001)?
        .ok_or("workspace leadership was not bootstrapped")?;
    assert_eq!(
        workspace_view.state(),
        crate::LeadershipState::Bootstrapping
    );
    assert_eq!(
        workspace_view.leader_participant_id(),
        Some(first_pending.participant_id())
    );

    let _replayed = catalog.record_registration_activation(&source_activation)?;
    let bootstrap_rows: i64 = catalog.connection.query_row(
        "SELECT COUNT(*) FROM ops_leadership_receipts WHERE receipt_kind='BOOTSTRAP' AND work_assignment_id=?1",
        params![assignment_id.as_bytes()],
        |row| row.get(0),
    )?;
    assert_eq!(bootstrap_rows, 2);

    drop(catalog);
    source.cleanup()?;
    fs::remove_dir_all(catalog_root)?;
    Ok(())
}

#[test]
fn logical_presence_requires_ready_binding_and_survives_reopen() -> Result<(), Box<dyn Error>> {
    let destination = Uuid::from_u128(0xFE58);
    let MembershipFixture {
        mut source,
        mut catalog,
        catalog_root,
        catalog_path,
        first_binding,
        first_pending,
        second_binding: _,
        second_pending: _,
    } = membership_fixture(0xFE59, destination)?;
    let reservation =
        source
            .store
            .reserve_agent_binding(&source.freeze, &first_binding, timestamp(1_003)?)?;
    let _reserved = catalog.record_registration_reservation(&reservation)?;
    let commit = catalog.commit_registration_membership(
        first_binding.request_id(),
        &[first_pending.participant_id()],
        timestamp(2_000)?,
    )?;
    assert!(matches!(
        catalog.touch_logical_presence(logical_presence_touch(
            source.workspace,
            &first_binding,
            AgentPresenceRevision::INITIAL,
            timestamp(2_100)?,
        )),
        Err(OperationsError::Conflict("logical member not ready"))
    ));
    let delegation =
        source
            .store
            .delegate_agent_operations(&source.freeze, [0x58; 32], timestamp(2_500)?)?;
    let request = AgentBindingActivationRequest::new(
        first_binding.request_id(),
        commit.commit_digest(),
        commit.display_name().clone(),
        commit.declared_role().clone(),
        commit.transport(),
    )?;
    let source_activation =
        source
            .store
            .activate_agent_binding(&delegation, &request, timestamp(3_000)?)?;
    let _activation = catalog.record_registration_activation(&source_activation)?;
    let target = assert_logical_presence_absence_and_scope(
        &catalog,
        source.workspace,
        &first_binding,
        first_pending.participant_id(),
    )?;
    let first_presence = catalog.touch_logical_presence(logical_presence_touch(
        source.workspace,
        &first_binding,
        AgentPresenceRevision::INITIAL,
        timestamp(3_100)?,
    ))?;
    assert_eq!(first_presence.participant_id(), commit.participant_id());
    assert_eq!(first_presence.presence_revision().get(), 2);
    assert_eq!(
        catalog.logical_presence(target, first_binding.principal())?,
        Some(first_presence.clone())
    );
    assert!(matches!(
        catalog.touch_logical_presence(logical_presence_touch(
            source.workspace,
            &first_binding,
            AgentPresenceRevision::INITIAL,
            timestamp(3_101)?,
        )),
        Err(OperationsError::Conflict("presence revision"))
    ));
    drop(catalog);
    source.reopen_store()?;
    let mut catalog = OperationsCatalog::open(&catalog_path, destination)?;
    assert_eq!(
        catalog.logical_presence(target, first_binding.principal())?,
        Some(first_presence)
    );
    let second_presence = catalog.touch_logical_presence(logical_presence_touch(
        source.workspace,
        &first_binding,
        AgentPresenceRevision::new(2)?,
        timestamp(3_200)?,
    ))?;
    assert_eq!(second_presence.presence_revision().get(), 3);
    assert_eq!(
        catalog.logical_presence(target, first_binding.principal())?,
        Some(second_presence)
    );
    drop(catalog);
    source.cleanup()?;
    fs::remove_dir_all(catalog_root)?;
    Ok(())
}

fn assert_inbox_readback_scope(
    catalog: &OperationsCatalog,
    workspace: WorkspaceId,
    first_binding: &AgentBindingReservationRequest,
) -> Result<(), Box<dyn Error>> {
    let target = LogicalInboxTarget::new(
        workspace,
        first_binding.activity_id(),
        first_binding.agent_id(),
        first_binding.session_id(),
    );
    let missing_event = Uuid::from_u128(0xFE7B);
    assert_eq!(
        catalog.logical_inbox_entry(target, first_binding.principal(), missing_event)?,
        None
    );
    for foreign_target in [
        LogicalInboxTarget::new(
            WorkspaceId::from_uuid(Uuid::from_u128(0xFE77)),
            first_binding.activity_id(),
            first_binding.agent_id(),
            first_binding.session_id(),
        ),
        LogicalInboxTarget::new(
            workspace,
            ActivityId::from_uuid(Uuid::from_u128(0xFE78)),
            first_binding.agent_id(),
            first_binding.session_id(),
        ),
        LogicalInboxTarget::new(
            workspace,
            first_binding.activity_id(),
            AgentInstanceId::from_uuid(Uuid::from_u128(0xFE79)),
            first_binding.session_id(),
        ),
        LogicalInboxTarget::new(
            workspace,
            first_binding.activity_id(),
            first_binding.agent_id(),
            AgentSessionId::from_uuid(Uuid::from_u128(0xFE7A)),
        ),
    ] {
        assert!(matches!(
            catalog.logical_inbox_entry(foreign_target, first_binding.principal(), missing_event),
            Err(OperationsError::Conflict("logical member not ready"))
        ));
    }
    assert!(matches!(
        catalog.logical_inbox_entry(
            target,
            AuthenticatedPrincipal::DirectToken(forge_domain::DirectTokenId::from_uuid(
                Uuid::from_u128(0xFE7C),
            )),
            missing_event,
        ),
        Err(OperationsError::Conflict("logical binding principal"))
    ));
    Ok(())
}

fn read_initial_inbox_event(
    catalog: &OperationsCatalog,
    query: LogicalInboxQuery,
    target: LogicalInboxTarget,
    principal: AuthenticatedPrincipal,
    participants: [Uuid; 2],
    actor: AgentInstanceId,
) -> Result<Uuid, Box<dyn Error>> {
    let first_read = catalog.list_logical_inbox(query)?;
    assert_eq!(first_read.len(), 1);
    let event_id =
        assert_initial_logical_inbox_entry(&first_read[0], participants[0], participants[1], actor);
    assert_eq!(
        catalog.logical_inbox_entry(target, principal, event_id)?,
        Some(first_read[0].clone())
    );

    Ok(event_id)
}

fn assert_inbox_receipt_lifecycle(
    catalog: &mut OperationsCatalog,
    workspace: WorkspaceId,
    binding: &AgentBindingReservationRequest,
    query: LogicalInboxQuery,
    target: LogicalInboxTarget,
    participants: [Uuid; 2],
    actor: AgentInstanceId,
) -> Result<Uuid, Box<dyn Error>> {
    assert_inbox_readback_scope(catalog, workspace, binding)?;
    assert_logical_activity_snapshot(catalog, workspace, binding.activity_id(), &participants, 1)?;
    let event_id = read_initial_inbox_event(
        catalog,
        query,
        target,
        binding.principal(),
        participants,
        actor,
    )?;

    acknowledge_logical_inbox_once(catalog, query, target, binding.principal(), event_id)?;
    assert_logical_activity_snapshot(catalog, workspace, binding.activity_id(), &participants, 0)?;

    Ok(event_id)
}

#[test]
fn logical_inbox_reads_without_ack_and_v4_upgrade_backfills_receipt_state()
-> Result<(), Box<dyn Error>> {
    let destination = Uuid::from_u128(0xFE70);
    let MembershipFixture {
        mut source,
        mut catalog,
        catalog_root,
        catalog_path,
        first_binding,
        first_pending,
        second_binding: _,
        second_pending: _,
    } = membership_fixture(0xFE71, destination)?;

    let first_commit = reserve_and_commit_registration(
        &mut source,
        &mut catalog,
        &first_binding,
        &[],
        timestamp(1_010)?,
        timestamp(2_010)?,
    )?;
    let (actor_binding, actor_participant_id) =
        prepare_inbox_actor(&mut catalog, source.migration, first_binding.activity_id())?;
    let actor_commit = reserve_and_commit_registration(
        &mut source,
        &mut catalog,
        &actor_binding,
        &[first_pending.participant_id()],
        timestamp(1_012)?,
        timestamp(2_011)?,
    )?;
    let delegation =
        source
            .store
            .delegate_agent_operations(&source.freeze, [0x70; 32], timestamp(2_500)?)?;
    activate_registration(
        &mut source,
        &mut catalog,
        &delegation,
        &first_binding,
        &first_commit,
        timestamp(3_010)?,
    )?;
    activate_registration(
        &mut source,
        &mut catalog,
        &delegation,
        &actor_binding,
        &actor_commit,
        timestamp(3_011)?,
    )?;

    drop(catalog);
    let mut catalog = reopen_inbox_catalog_from_v4(&catalog_path, destination)?;

    let target = LogicalInboxTarget::new(
        source.workspace,
        first_binding.activity_id(),
        first_binding.agent_id(),
        first_binding.session_id(),
    );
    let query = LogicalInboxQuery::new(target, first_binding.principal(), 64);
    let event_id = assert_inbox_receipt_lifecycle(
        &mut catalog,
        source.workspace,
        &first_binding,
        query,
        target,
        [first_pending.participant_id(), actor_participant_id],
        actor_binding.agent_id(),
    )?;

    drop(catalog);
    assert_reopened_inbox_and_activity(
        &catalog_path,
        destination,
        query,
        (target, first_binding.principal(), event_id),
        (
            source.workspace,
            first_binding.activity_id(),
            [first_pending.participant_id(), actor_participant_id],
        ),
    )?;
    source.cleanup()?;
    fs::remove_dir_all(catalog_root)?;
    Ok(())
}

#[test]
fn membership_commit_fences_abort_and_preserves_activation_recovery() -> Result<(), Box<dyn Error>>
{
    let destination = Uuid::from_u128(0xFE60);
    let MembershipFixture {
        mut source,
        mut catalog,
        catalog_root,
        first_binding,
        first_pending: _,
        second_binding: _,
        second_pending: _,
        catalog_path: _,
    } = membership_fixture(0xFE61, destination)?;
    let reservation =
        source
            .store
            .reserve_agent_binding(&source.freeze, &first_binding, timestamp(1_003)?)?;
    let _reserved = catalog.record_registration_reservation(&reservation)?;
    let commit = catalog.commit_registration_membership(
        first_binding.request_id(),
        &[],
        timestamp(2_000)?,
    )?;
    let delegation =
        source
            .store
            .delegate_agent_operations(&source.freeze, [0x62; 32], timestamp(2_500)?)?;
    assert!(matches!(
        catalog.abort_import(source.migration),
        Err(OperationsError::Conflict("membership handoff committed"))
    ));
    let activation_request = AgentBindingActivationRequest::new(
        first_binding.request_id(),
        commit.commit_digest(),
        commit.display_name().clone(),
        commit.declared_role().clone(),
        commit.transport(),
    )?;
    let source_activation =
        source
            .store
            .activate_agent_binding(&delegation, &activation_request, timestamp(3_000)?)?;
    let activated = catalog.record_registration_activation(&source_activation)?;
    assert!(matches!(
        catalog.abort_import(source.migration),
        Err(OperationsError::Conflict("membership handoff committed"))
    ));
    assert_eq!(
        catalog
            .registration_progress(first_binding.request_id())?
            .ok_or("registration progress missing")?
            .phase(),
        RegistrationPhase::Ready
    );
    assert_eq!(
        catalog.registration_activation(first_binding.request_id())?,
        Some(activated)
    );
    source.cleanup()?;
    std::fs::remove_dir_all(catalog_root)?;
    Ok(())
}

#[test]
fn membership_commit_rejects_abort_before_commit() -> Result<(), Box<dyn Error>> {
    let destination = Uuid::from_u128(0xFF00);
    let seed = 0xFF01;
    let mut source = source_fixture(seed, 1, false, destination)?;
    let (root, path) = catalog_root(seed)?;
    let mut catalog = OperationsCatalog::open(&path, destination)?;
    catalog.prepare(&source.freeze)?;
    import_all(&mut catalog, &source)?;
    verify_all(&mut catalog, &source)?;
    let binding = AgentBindingReservationRequest::new(
        Uuid::from_u128(0xFF10),
        ActivityId::from_uuid(Uuid::from_u128(seed + 2)),
        AgentInstanceId::from_uuid(Uuid::from_u128(0xFF110)),
        AgentSessionId::from_uuid(Uuid::from_u128(0xFF12)),
        AuthenticatedPrincipal::TrustedStdio,
        timestamp(1_001)?,
    )?;
    let request = RegistrationRequest::new(
        Uuid::from_u128(0xFF13),
        binding.clone(),
        AgentDisplayName::new("abort worker".to_owned())?,
        AgentDeclaredRole::new("worker".to_owned())?,
        AgentTransportKind::Stdio,
    )?;
    let pending = catalog.prepare_registration(source.migration, &request)?;
    let reservation =
        source
            .store
            .reserve_agent_binding(&source.freeze, &binding, timestamp(1_002)?)?;
    let _reserved = catalog.record_registration_reservation(&reservation)?;
    catalog.abort_import(source.migration)?;
    assert!(matches!(
        catalog.commit_registration_membership(
            binding.request_id(),
            &[pending.participant_id()],
            timestamp(2_000)?
        ),
        Err(OperationsError::Conflict("import aborted"))
    ));
    let count: i64 = catalog.connection.query_row(
        "SELECT COUNT(*) FROM ops_logical_members WHERE workspace_id=?1",
        params![source.workspace.into_uuid().as_bytes()],
        |row| row.get(0),
    )?;
    assert_eq!(count, 0);
    source.cleanup()?;
    fs::remove_dir_all(root)?;
    Ok(())
}

#[test]
fn registration_rejects_late_reservation_after_import_abort() -> Result<(), Box<dyn Error>> {
    let destination = Uuid::from_u128(0xFB00);
    let seed = 0xFB01;
    let mut source = source_fixture(seed, 1, false, destination)?;
    let (root, path) = catalog_root(seed)?;
    let mut catalog = OperationsCatalog::open(&path, destination)?;
    let _prepared = catalog.prepare(&source.freeze)?;
    import_all(&mut catalog, &source)?;
    let verified = verify_all(&mut catalog, &source)?;
    assert_eq!(verified.phase(), ImportPhase::Verified);

    let binding = AgentBindingReservationRequest::new(
        Uuid::from_u128(0xFB10),
        ActivityId::from_uuid(Uuid::from_u128(seed + 2)),
        AgentInstanceId::from_uuid(Uuid::from_u128(0xFB12)),
        AgentSessionId::from_uuid(Uuid::from_u128(0xFB11)),
        AuthenticatedPrincipal::TrustedStdio,
        timestamp(1_001)?,
    )?;
    let request = RegistrationRequest::new(
        Uuid::from_u128(0xFB13),
        binding.clone(),
        AgentDisplayName::new("late worker".to_owned())?,
        AgentDeclaredRole::new("worker".to_owned())?,
        AgentTransportKind::Stdio,
    )?;
    let pending = catalog.prepare_registration(source.migration, &request)?;
    assert_eq!(pending.phase(), RegistrationPhase::Pending);
    let aborted = catalog.abort_import(source.migration)?;
    assert_eq!(aborted.phase(), ImportPhase::Aborted);

    let reservation =
        source
            .store
            .reserve_agent_binding(&source.freeze, &binding, timestamp(1_002)?)?;
    assert!(matches!(
        catalog.record_registration_reservation(&reservation),
        Err(OperationsError::Conflict("import aborted"))
    ));
    assert_eq!(
        catalog
            .registration_progress(binding.request_id())?
            .map(|value| value.phase()),
        Some(RegistrationPhase::Pending)
    );
    source.cleanup()?;
    fs::remove_dir_all(root)?;
    Ok(())
}

#[test]
fn registration_schema_v1_upgrade_preserves_import_evidence() -> Result<(), Box<dyn Error>> {
    let destination = Uuid::from_u128(0xFC00);
    let seed = 0xFC01;
    let source = source_fixture(seed, 1, false, destination)?;
    let (root, path) = catalog_root(seed)?;
    let mut catalog = OperationsCatalog::open(&path, destination)?;
    catalog.prepare(&source.freeze)?;
    import_all(&mut catalog, &source)?;
    let verified = verify_all(&mut catalog, &source)?;
    assert_eq!(verified.phase(), ImportPhase::Verified);
    drop(catalog);

    let connection = Connection::open(&path)?;
    crate::workspace_participation::drop_schema_for_legacy_fixture(&connection)?;
    connection.execute_batch(
        "DROP TABLE ops_admission_records; DROP TABLE ops_approval_requests; DROP TABLE ops_approval_policy; DROP TABLE ops_issuer_config; DROP TABLE ops_leadership_confirm_intents; DROP TABLE ops_ballots; DROP TABLE ops_election_electorate; DROP TABLE ops_leadership_elections; DROP TABLE ops_leadership_receipts; DROP TABLE ops_leadership; DROP TABLE ops_work_credit_entries; DROP TABLE ops_assignment_submit_source_refs; DROP TABLE ops_assignment_submit_projection; DROP TABLE ops_assignment_offer_projection; DROP TABLE ops_assignment_review_projection; DROP TABLE ops_change_set_projection; DROP TABLE ops_execution_terminal_projection; DROP TABLE ops_effect_fact_consumers; DROP TABLE ops_logical_departure_receipts; DROP TABLE ops_logical_departures; DROP TABLE ops_logical_departure_events; DROP TABLE ops_legacy_adoptions; DROP TABLE ops_current_effect_bindings; DROP TRIGGER ops_logical_receipt_state_insert; DROP TABLE ops_logical_receipt_state; DROP TABLE ops_logical_presence; DROP TABLE ops_binding_activations; DROP TABLE ops_logical_receipts; DROP TABLE ops_logical_bindings; DROP TABLE ops_logical_events; DROP TABLE ops_logical_members; DROP TABLE ops_registration_requests; DROP TABLE ops_pending_participants; PRAGMA user_version=1;",
    )?;
    drop(connection);

    let catalog = OperationsCatalog::open(&path, destination)?;
    assert_eq!(
        catalog
            .progress(source.migration)?
            .ok_or("import evidence missing after schema upgrade")?
            .phase(),
        ImportPhase::Verified
    );
    let registration_tables: i64 = catalog.connection.query_row(
        "SELECT COUNT(*) FROM sqlite_schema WHERE name IN ('ops_pending_participants','ops_registration_requests')",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(registration_tables, 2);
    drop(catalog);
    source.cleanup()?;
    fs::remove_dir_all(root)?;
    Ok(())
}

#[test]
fn membership_schema_v2_upgrade_preserves_registration_data() -> Result<(), Box<dyn Error>> {
    let destination = Uuid::from_u128(0xFC10);
    let seed = 0xFC11;
    let source = source_fixture(seed, 1, false, destination)?;
    let (root, path) = catalog_root(seed)?;
    let mut catalog = OperationsCatalog::open(&path, destination)?;
    catalog.prepare(&source.freeze)?;
    import_all(&mut catalog, &source)?;
    verify_all(&mut catalog, &source)?;
    let binding = AgentBindingReservationRequest::new(
        Uuid::from_u128(0xFC20),
        ActivityId::from_uuid(Uuid::from_u128(seed + 2)),
        AgentInstanceId::from_uuid(Uuid::from_u128(0xFC210)),
        AgentSessionId::from_uuid(Uuid::from_u128(0xFC22)),
        AuthenticatedPrincipal::TrustedStdio,
        timestamp(1_001)?,
    )?;
    let request = RegistrationRequest::new(
        Uuid::from_u128(0xFC23),
        binding,
        AgentDisplayName::new("v2 registration".to_owned())?,
        AgentDeclaredRole::new("worker".to_owned())?,
        AgentTransportKind::Stdio,
    )?;
    let pending = catalog.prepare_registration(source.migration, &request)?;
    drop(catalog);

    let connection = Connection::open(&path)?;
    crate::workspace_participation::drop_schema_for_legacy_fixture(&connection)?;
    connection.execute_batch(
        "DROP TABLE ops_admission_records; DROP TABLE ops_approval_requests; DROP TABLE ops_approval_policy; DROP TABLE ops_issuer_config; DROP TABLE ops_leadership_confirm_intents; DROP TABLE ops_ballots; DROP TABLE ops_election_electorate; DROP TABLE ops_leadership_elections; DROP TABLE ops_leadership_receipts; DROP TABLE ops_leadership; DROP TABLE ops_work_credit_entries; DROP TABLE ops_assignment_submit_source_refs; DROP TABLE ops_assignment_submit_projection; DROP TABLE ops_assignment_offer_projection; DROP TABLE ops_assignment_review_projection; DROP TABLE ops_change_set_projection; DROP TABLE ops_execution_terminal_projection; DROP TABLE ops_effect_fact_consumers; DROP TABLE ops_logical_departure_receipts; DROP TABLE ops_logical_departures; DROP TABLE ops_logical_departure_events; DROP TABLE ops_legacy_adoptions; DROP TABLE ops_current_effect_bindings; DROP TRIGGER ops_logical_receipt_state_insert; DROP TABLE ops_logical_receipt_state; DROP TABLE ops_logical_presence; DROP TABLE ops_binding_activations; DROP TABLE ops_logical_receipts; DROP TABLE ops_logical_bindings; DROP TABLE ops_logical_events; DROP TABLE ops_logical_members; PRAGMA user_version=2;",
    )?;
    drop(connection);

    let catalog = OperationsCatalog::open(&path, destination)?;
    assert_eq!(
        catalog.registration_progress(request.binding().request_id())?,
        Some(pending)
    );
    let membership_tables: i64 = catalog.connection.query_row(
        "SELECT COUNT(*) FROM sqlite_schema WHERE name IN ('ops_logical_members','ops_logical_events','ops_logical_bindings','ops_logical_receipts')",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(membership_tables, 4);
    let activation_tables: i64 = catalog.connection.query_row(
        "SELECT COUNT(*) FROM sqlite_schema WHERE name='ops_binding_activations'",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(activation_tables, 1);
    drop(catalog);
    source.cleanup()?;
    fs::remove_dir_all(root)?;
    Ok(())
}

#[test]
fn schema_v3_upgrade_adds_logical_presence_without_changing_catalog_identity()
-> Result<(), Box<dyn Error>> {
    let destination = Uuid::from_u128(0xFC30);
    let seed = 0xFC31;
    let (root, path) = catalog_root(seed)?;
    let catalog = OperationsCatalog::open(&path, destination)?;
    drop(catalog);

    let connection = Connection::open(&path)?;
    crate::workspace_participation::drop_schema_for_legacy_fixture(&connection)?;
    connection.execute_batch("DROP TABLE ops_admission_records; DROP TABLE ops_approval_requests; DROP TABLE ops_approval_policy; DROP TABLE ops_issuer_config; DROP TABLE ops_leadership_confirm_intents; DROP TABLE ops_ballots; DROP TABLE ops_election_electorate; DROP TABLE ops_leadership_elections; DROP TABLE ops_leadership_receipts; DROP TABLE ops_leadership; DROP TABLE ops_work_credit_entries; DROP TABLE ops_assignment_submit_source_refs; DROP TABLE ops_assignment_submit_projection; DROP TABLE ops_assignment_offer_projection; DROP TABLE ops_assignment_review_projection; DROP TABLE ops_change_set_projection; DROP TABLE ops_execution_terminal_projection; DROP TABLE ops_effect_fact_consumers; DROP TABLE ops_logical_departure_receipts; DROP TABLE ops_logical_departures; DROP TABLE ops_logical_departure_events; DROP TABLE ops_legacy_adoptions; DROP TABLE ops_current_effect_bindings; DROP TRIGGER ops_logical_receipt_state_insert; DROP TABLE ops_logical_receipt_state; DROP TABLE ops_logical_presence; PRAGMA user_version=3;")?;
    drop(connection);

    let catalog = OperationsCatalog::open(&path, destination)?;
    assert_eq!(catalog.destination_id(), destination);
    let version: i64 = catalog
        .connection
        .pragma_query_value(None, "user_version", |row| row.get(0))?;
    assert_eq!(version, crate::catalog::VERSION);
    let presence_tables: i64 = catalog.connection.query_row(
        "SELECT COUNT(*) FROM sqlite_schema WHERE type='table' AND name='ops_logical_presence'",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(presence_tables, 1);
    let inbox_state_objects: i64 = catalog.connection.query_row(
        "SELECT COUNT(*) FROM sqlite_schema WHERE name IN ('ops_logical_receipt_state','ops_logical_receipt_state_insert')",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(inbox_state_objects, 2);
    drop(catalog);
    fs::remove_dir_all(root)?;
    Ok(())
}

#[test]
fn schema_v15_upgrade_adds_approval_authority_without_changing_catalog_identity()
-> Result<(), Box<dyn Error>> {
    let destination = Uuid::from_u128(0xFC40);
    let seed = 0xFC41;
    let (root, path) = catalog_root(seed)?;
    let catalog = OperationsCatalog::open(&path, destination)?;
    drop(catalog);

    // Demote a current catalog to exactly its v15 shape: the approval
    // authority tables and later source-ref additions must not exist yet.
    let connection = Connection::open(&path)?;
    crate::workspace_participation::drop_schema_for_legacy_fixture(&connection)?;
    connection.execute_batch("DROP TABLE ops_assignment_submit_source_refs; ALTER TABLE ops_assignment_submit_projection DROP COLUMN source_refs_recorded; ALTER TABLE ops_change_set_projection DROP COLUMN digest_version; DROP TABLE ops_admission_records; DROP TABLE ops_approval_requests; DROP TABLE ops_approval_policy; DROP TABLE ops_issuer_config; PRAGMA user_version=15;")?;
    drop(connection);

    let catalog = OperationsCatalog::open(&path, destination)?;
    assert_eq!(catalog.destination_id(), destination);
    let version: i64 = catalog
        .connection
        .pragma_query_value(None, "user_version", |row| row.get(0))?;
    assert_eq!(version, crate::catalog::VERSION);
    let approval_objects: i64 = catalog.connection.query_row(
        "SELECT COUNT(*) FROM sqlite_schema WHERE name IN ('ops_issuer_config','ops_approval_policy','ops_approval_requests','ops_approval_requests_workspace_idx','ops_admission_records')",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(approval_objects, 5);
    let leadership_objects: i64 = catalog.connection.query_row(
        "SELECT COUNT(*) FROM sqlite_schema WHERE name IN ('ops_leadership','ops_leadership_confirm_intents')",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(leadership_objects, 2);
    drop(catalog);

    // Reopening the upgraded catalog revalidates the current schema.
    drop(OperationsCatalog::open(&path, destination)?);
    fs::remove_dir_all(root)?;
    Ok(())
}

#[test]
fn registration_rejects_wire_metadata_that_disagrees_with_canonical_participant()
-> Result<(), Box<dyn Error>> {
    let destination = Uuid::from_u128(0xFD00);
    let seed = 0xFD01;
    let mut source = source_fixture(seed, 1, false, destination)?;
    let (root, path) = catalog_root(seed)?;
    let mut catalog = OperationsCatalog::open(&path, destination)?;
    let _prepared = catalog.prepare(&source.freeze)?;
    import_all(&mut catalog, &source)?;
    let _verified = verify_all(&mut catalog, &source)?;
    let binding = AgentBindingReservationRequest::new(
        Uuid::from_u128(0xFD10),
        ActivityId::from_uuid(Uuid::from_u128(seed + 2)),
        AgentInstanceId::from_uuid(Uuid::from_u128(0xFD110)),
        AgentSessionId::from_uuid(Uuid::from_u128(0xFD120)),
        AuthenticatedPrincipal::TrustedStdio,
        timestamp(1001)?,
    )?;
    let request = RegistrationRequest::new(
        Uuid::from_u128(0xFD13),
        binding.clone(),
        AgentDisplayName::new("canonical participant".to_owned())?,
        AgentDeclaredRole::new("worker".to_owned())?,
        AgentTransportKind::Stdio,
    )?;
    let _pending = catalog.prepare_registration(source.migration, &request)?;
    let reserved =
        source
            .store
            .reserve_agent_binding(&source.freeze, &binding, timestamp(1002)?)?;
    let changed = RegistrationRequest::new(
        request.proposed_participant_id(),
        binding.clone(),
        AgentDisplayName::new("corrupted reported participant".to_owned())?,
        request.declared_role().clone(),
        request.transport(),
    )?;
    catalog.connection.execute(
        "UPDATE ops_registration_requests SET request_wire=?1 WHERE request_id=?2",
        params![changed.encode()?, binding.request_id().as_bytes()],
    )?;
    assert!(catalog.registration_progress(binding.request_id()).is_err());
    assert!(catalog.record_registration_reservation(&reserved).is_err());
    let phase: i64 = catalog.connection.query_row(
        "SELECT phase FROM ops_registration_requests WHERE request_id=?1",
        params![binding.request_id().as_bytes()],
        |row| row.get(0),
    )?;
    assert_eq!(phase, 0, "corrupt metadata must not advance reservation");
    drop(catalog);
    source.cleanup()?;
    fs::remove_dir_all(root)?;
    Ok(())
}

fn assert_corrupt_activation_is_rejected(
    catalog: &OperationsCatalog,
    binding: &AgentBindingReservationRequest,
    workspace: WorkspaceId,
) -> Result<(), Box<dyn Error>> {
    // A valid-looking but foreign session must not inherit another binding's Ready proof.
    catalog.connection.execute(
        "UPDATE ops_logical_members SET session_id=?1 WHERE workspace_id=?2 AND session_id=?3",
        params![
            Uuid::from_u128(0xFEFFF).as_bytes(),
            workspace.into_uuid().as_bytes(),
            binding.session_id().into_uuid().as_bytes()
        ],
    )?;
    assert!(matches!(
        catalog.registration_progress(binding.request_id()),
        Err(OperationsError::Corrupt(_))
    ));
    assert!(matches!(
        catalog.visible_logical_members(workspace, 64),
        Err(OperationsError::Corrupt(_))
    ));
    catalog.connection.execute(
        "UPDATE ops_logical_members SET session_id=?1 WHERE workspace_id=?2 AND session_id=?3",
        params![
            binding.session_id().into_uuid().as_bytes(),
            workspace.into_uuid().as_bytes(),
            Uuid::from_u128(0xFEFFF).as_bytes()
        ],
    )?;
    assert_visible_projection(catalog, workspace)?;
    Ok(())
}

fn assert_current_projection_survives_historical_proof_corruption(
    catalog: &OperationsCatalog,
    binding: &AgentBindingReservationRequest,
    workspace: WorkspaceId,
) -> Result<(), Box<dyn Error>> {
    let original: Vec<u8> = catalog.connection.query_row(
        "SELECT request_wire FROM ops_registration_requests WHERE request_id=?1",
        params![binding.request_id().as_bytes()],
        |row| row.get(0),
    )?;
    catalog.connection.execute(
        "UPDATE ops_registration_requests SET request_wire=?1 WHERE request_id=?2",
        params![
            b"invalid persisted request".as_slice(),
            binding.request_id().as_bytes()
        ],
    )?;
    assert_visible_projection(catalog, workspace)?;
    catalog.connection.execute(
        "UPDATE ops_registration_requests SET request_wire=?1 WHERE request_id=?2",
        params![original, binding.request_id().as_bytes()],
    )?;
    let digest: Vec<u8> = catalog.connection.query_row(
        "SELECT commit_digest FROM ops_logical_bindings WHERE request_id=?1",
        params![binding.request_id().as_bytes()],
        |row| row.get(0),
    )?;
    // Simulate a damaged database, including matching but invalid digests.
    catalog
        .connection
        .execute_batch("PRAGMA ignore_check_constraints=ON")?;
    catalog.connection.execute(
        "UPDATE ops_logical_bindings SET commit_digest=zeroblob(32) WHERE request_id=?1",
        params![binding.request_id().as_bytes()],
    )?;
    catalog.connection.execute(
        "UPDATE ops_binding_activations SET membership_digest=zeroblob(32) WHERE request_id=?1",
        params![binding.request_id().as_bytes()],
    )?;
    catalog
        .connection
        .execute_batch("PRAGMA ignore_check_constraints=OFF")?;
    assert_visible_projection(catalog, workspace)?;
    assert!(
        catalog
            .registration_activation(binding.request_id())
            .is_err()
    );
    catalog.connection.execute(
        "UPDATE ops_logical_bindings SET commit_digest=?1 WHERE request_id=?2",
        params![&digest, binding.request_id().as_bytes()],
    )?;
    catalog.connection.execute(
        "UPDATE ops_binding_activations SET membership_digest=?1 WHERE request_id=?2",
        params![&digest, binding.request_id().as_bytes()],
    )?;
    assert_visible_projection(catalog, workspace)?;
    Ok(())
}

fn departure_digest(request_id: Uuid) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(b"heterarchy-orchestration-test-departure/v1\0");
    digest.update(request_id.as_bytes());
    digest.finalize().into()
}

fn logical_plane_count(
    catalog: &OperationsCatalog,
    table: &str,
    workspace: WorkspaceId,
) -> Result<i64, Box<dyn Error>> {
    let count: i64 = catalog.connection.query_row(
        &format!("SELECT COUNT(*) FROM {table} WHERE workspace_id=?1"),
        params![workspace.into_uuid().as_bytes()],
        |row| row.get(0),
    )?;
    Ok(count)
}

struct ReplacementIdentitySeed {
    profile_id: AgentProfileId,
    old_session: RegisteredAgentSession,
    new_session: RegisteredAgentSession,
    old_link: AgentProfileLogicalSessionLink,
    new_link: AgentProfileLogicalSessionLink,
}

fn replacement_identity_seed(root: &Path) -> Result<ReplacementIdentitySeed, Box<dyn Error>> {
    let mut profiles = AgentProfileCatalog::open(root.join("agent-profiles.sqlite3"))?;
    let mut sessions = AgentSessionCatalog::open(root.join("agent-sessions.sqlite3"))?;
    let profile_id = AgentProfileId::from_uuid(Uuid::from_u128(0xCD01))?;
    let profile = CreateAgentProfile::new(
        profile_id,
        Uuid::from_u128(0xCD02),
        "phase-d-replacement".to_owned(),
        "Phase D Replacement".to_owned(),
        AgentProfileProviderFamily::ChatGpt,
        Some("gpt-5.6-luna".to_owned()),
        Some("max".to_owned()),
        vec!["rust".to_owned()],
        AgentProfileCreationOrigin::UserManual,
        timestamp(20)?,
    )?;
    let _created = profiles.create_profile(&profile)?;
    let principal = AuthenticatedPrincipal::TrustedStdio;
    let old_session = sessions
        .open_session(&OpenAgentSession::new(
            AgentSessionId::from_uuid(Uuid::from_u128(0xCD10)),
            AgentSessionOpenRequestId::from_uuid(Uuid::from_u128(0xCD11)),
            principal,
            timestamp(21)?,
        ))?
        .session()
        .clone();
    let new_session = sessions
        .open_session(&OpenAgentSession::new(
            AgentSessionId::from_uuid(Uuid::from_u128(0xCD12)),
            AgentSessionOpenRequestId::from_uuid(Uuid::from_u128(0xCD13)),
            principal,
            timestamp(22)?,
        ))?
        .session()
        .clone();
    let old_link = link_existing_logical_session(
        &mut profiles,
        &sessions,
        profile_id,
        old_session.id(),
        principal,
        Uuid::from_u128(0xCD14),
        timestamp(23)?,
    )?
    .link();
    let new_link = link_existing_logical_session(
        &mut profiles,
        &sessions,
        profile_id,
        new_session.id(),
        principal,
        Uuid::from_u128(0xCD15),
        timestamp(24)?,
    )?
    .link();
    Ok(ReplacementIdentitySeed {
        profile_id,
        old_session,
        new_session,
        old_link,
        new_link,
    })
}

struct ReadyReplacementFixture {
    source: SourceFixture,
    catalog_root: PathBuf,
    catalog: OperationsCatalog,
    identity: ReplacementIdentitySeed,
    participant_id: Uuid,
    participation_id: WorkspaceParticipationId,
    old_binding: AgentBindingReservationRequest,
    delegation: AgentOperationsDelegation,
}

fn ready_replacement_fixture() -> Result<ReadyReplacementFixture, Box<dyn Error>> {
    let destination = Uuid::from_u128(0xCD00);
    let seed = 0xCD20_u128;
    let mut source = source_fixture(seed, 1, false, destination)
        .map_err(|error| std::io::Error::other(format!("replacement source fixture: {error}")))?;
    let (catalog_root, catalog_path) = catalog_root(seed)
        .map_err(|error| std::io::Error::other(format!("replacement catalog root: {error}")))?;
    let identity = replacement_identity_seed(&catalog_root)
        .map_err(|error| std::io::Error::other(format!("replacement identity seed: {error}")))?;
    let mut catalog = OperationsCatalog::open(&catalog_path, destination)?;
    let _prepared = catalog.prepare(&source.freeze)?;
    import_all(&mut catalog, &source)?;
    verify_all(&mut catalog, &source)?;
    let old_binding = AgentBindingReservationRequest::new(
        Uuid::from_u128(0xCD30),
        ActivityId::from_uuid(Uuid::from_u128(seed + 2)),
        AgentInstanceId::from_uuid(Uuid::from_u128(0xCD31)),
        identity.old_session.id(),
        AuthenticatedPrincipal::TrustedStdio,
        timestamp(1_001)?,
    )?;
    let request = RegistrationRequest::new(
        Uuid::from_u128(0xCD32),
        old_binding.clone(),
        AgentDisplayName::new("replacement worker".to_owned())?,
        AgentDeclaredRole::new("worker".to_owned())?,
        AgentTransportKind::Stdio,
    )?;
    let pending = catalog.prepare_registration(source.migration, &request)?;
    let reservation =
        source
            .store
            .reserve_agent_binding(&source.freeze, &old_binding, timestamp(1_002)?)?;
    let _reserved = catalog.record_registration_reservation(&reservation)?;
    let commit =
        catalog.commit_registration_membership(old_binding.request_id(), &[], timestamp(2_000)?)?;
    let delegation =
        source
            .store
            .delegate_agent_operations(&source.freeze, [0xCD; 32], timestamp(2_500)?)?;
    activate_registration(
        &mut source,
        &mut catalog,
        &delegation,
        &old_binding,
        &commit,
        timestamp(3_000)?,
    )?;
    let logical_identity = catalog
        .logical_identity_for_session(source.workspace, identity.old_session.id())?
        .ok_or("initial logical Session identity missing")?;
    let participation_id = WorkspaceParticipationId::from_uuid(Uuid::from_u128(0xCD33))?;
    let _mapping = catalog.map_workspace_participation(
        Uuid::from_u128(0xCD34),
        participation_id,
        identity.old_link,
        &logical_identity,
        logical_identity.membership_revision(),
        timestamp(3_100)?,
    )?;
    Ok(ReadyReplacementFixture {
        source,
        catalog_root,
        catalog,
        identity,
        participant_id: pending.participant_id(),
        participation_id,
        old_binding,
        delegation,
    })
}

fn detach_ready_replacement_fixture(
    fixture: &mut ReadyReplacementFixture,
) -> Result<(), Box<dyn Error>> {
    let request_id = Uuid::from_u128(0xCD35);
    let departure_request = AgentDepartureReconciliationRequest::new(
        request_id,
        AgentDepartureCommand::Detach(DetachAgent::new(
            fixture.old_binding.agent_id(),
            fixture.old_binding.activity_id(),
            AgentInstanceRevision::new(1)?,
            timestamp(4_000)?,
        )),
        fixture.old_binding.session_id(),
        fixture.old_binding.principal(),
        departure_digest(request_id),
    )?;
    let source_receipt = fixture.source.store.reconcile_agent_departure(
        &fixture.delegation,
        &departure_request,
        timestamp(4_000)?,
    )?;
    let command = LogicalDetachCommand::new(
        fixture.source.workspace,
        fixture.old_binding.activity_id(),
        fixture.participant_id,
        fixture.old_binding.agent_id(),
        fixture.old_binding.session_id(),
        fixture.old_binding.principal(),
        timestamp(4_001)?,
    );
    let _receipt =
        fixture
            .catalog
            .commit_logical_detach(command, &source_receipt, timestamp(4_100)?)?;
    Ok(())
}

fn execute_workspace_participation_replacement(
    fixture: &mut ReadyReplacementFixture,
) -> Result<
    (
        AgentBindingReservationRequest,
        WorkspaceParticipationReplacementCommitReceipt,
        WorkspaceParticipationReplacementActivationReceipt,
    ),
    Box<dyn Error>,
> {
    let target = fixture
        .catalog
        .workspace_participation_session_expectation(
            fixture.source.workspace,
            fixture.identity.profile_id,
        )?
        .ok_or("replacement target missing")?;
    assert_eq!(target.participant_id(), fixture.participant_id);
    assert_eq!(target.participation_id(), fixture.participation_id);
    let binding = AgentBindingReservationRequest::new(
        Uuid::from_u128(0xCD40),
        fixture.old_binding.activity_id(),
        AgentInstanceId::from_uuid(Uuid::from_u128(0xCD41)),
        fixture.identity.new_session.id(),
        AuthenticatedPrincipal::TrustedStdio,
        timestamp(5_001)?,
    )?;
    let request = RegistrationRequest::new(
        fixture.participant_id,
        binding.clone(),
        AgentDisplayName::new("replacement worker".to_owned())?,
        AgentDeclaredRole::new("worker".to_owned())?,
        AgentTransportKind::Stdio,
    )?;
    let pending = fixture
        .catalog
        .prepare_workspace_participation_replacement(
            fixture.source.migration,
            target,
            fixture.identity.new_link,
            &fixture.identity.new_session,
            &request,
        )?;
    assert_eq!(pending.phase(), RegistrationPhase::Pending);
    let reservation = fixture.source.store.reserve_agent_binding(
        &fixture.source.freeze,
        &binding,
        timestamp(5_002)?,
    )?;
    let reserved = fixture
        .catalog
        .record_workspace_participation_replacement_reservation(&reservation)?;
    assert_eq!(reserved.phase(), RegistrationPhase::Reserved);
    let rebind_command = WorkspaceParticipationSessionRebindCommand::new(
        Uuid::from_u128(0xCD42),
        fixture.source.workspace,
        target.current_session_id(),
        target.participation_revision(),
        timestamp(5_003)?,
    )?;
    let rebind = fixture.catalog.rebind_workspace_participation_session(
        rebind_command,
        fixture.identity.new_link,
        &fixture.identity.new_session,
    )?;
    let committed = fixture.catalog.commit_workspace_participation_replacement(
        binding.request_id(),
        rebind,
        &[],
        timestamp(5_004)?,
    )?;
    let activation_request = AgentBindingActivationRequest::new(
        binding.request_id(),
        committed.commit_digest(),
        request.display_name().clone(),
        request.declared_role().clone(),
        request.transport(),
    )?;
    let activation = fixture.source.store.activate_agent_binding(
        &fixture.delegation,
        &activation_request,
        timestamp(5_005)?,
    )?;
    let activated = fixture
        .catalog
        .record_workspace_participation_replacement_activation(&activation)?;
    assert_eq!(
        fixture.catalog.rebind_workspace_participation_session(
            rebind_command,
            fixture.identity.new_link,
            &fixture.identity.new_session,
        )?,
        rebind
    );
    assert_eq!(
        fixture.catalog.commit_workspace_participation_replacement(
            binding.request_id(),
            rebind,
            &[],
            timestamp(5_004)?,
        )?,
        committed
    );
    assert_eq!(
        fixture
            .catalog
            .record_workspace_participation_replacement_activation(&activation)?,
        activated
    );
    Ok((binding, committed, activated))
}

#[test]
fn logical_departure_readback_survives_profile_session_replacement() -> Result<(), Box<dyn Error>> {
    let mut fixture = ready_replacement_fixture()?;
    detach_ready_replacement_fixture(&mut fixture)?;
    let request_id = Uuid::from_u128(0xCD35);
    let before = fixture
        .catalog
        .logical_departure(request_id)?
        .ok_or("departure receipt missing before profile replacement")?;

    let _replacement = execute_workspace_participation_replacement(&mut fixture)?;
    let after = fixture
        .catalog
        .logical_departure(request_id)?
        .ok_or("departure receipt missing after profile replacement")?;

    assert_eq!(after, before);
    assert_eq!(after.display_name().as_str(), "replacement worker");
    assert_eq!(after.declared_role().as_str(), "worker");
    assert_eq!(after.transport(), AgentTransportKind::Stdio);
    drop(fixture.catalog);
    fixture.source.cleanup()?;
    fs::remove_dir_all(fixture.catalog_root)?;
    Ok(())
}

#[test]
fn logical_departure_readback_uses_activation_metadata_after_member_metadata_drift()
-> Result<(), Box<dyn Error>> {
    let mut fixture = ready_replacement_fixture()?;
    detach_ready_replacement_fixture(&mut fixture)?;
    let request_id = Uuid::from_u128(0xCD35);

    fixture.catalog.connection.execute(
        "UPDATE ops_logical_members SET display_name=?1,declared_role=?2,transport_kind=?3 WHERE workspace_id=?4 AND participant_id=?5 AND session_id=?6",
        params![
            "current member label",
            "reviewer",
            3_i64,
            fixture.source.workspace.into_uuid().as_bytes(),
            fixture.participant_id.as_bytes(),
            fixture.old_binding.session_id().into_uuid().as_bytes(),
        ],
    )?;

    let receipt = fixture
        .catalog
        .logical_departure(request_id)?
        .ok_or("departure receipt missing after current member metadata drift")?;
    assert_eq!(receipt.display_name().as_str(), "replacement worker");
    assert_eq!(receipt.declared_role().as_str(), "worker");
    assert_eq!(receipt.transport(), AgentTransportKind::Stdio);
    drop(fixture.catalog);
    fixture.source.cleanup()?;
    fs::remove_dir_all(fixture.catalog_root)?;
    Ok(())
}

#[test]
fn replacement_departure_readback_uses_its_immutable_registration_request()
-> Result<(), Box<dyn Error>> {
    let mut fixture = ready_replacement_fixture()?;
    detach_ready_replacement_fixture(&mut fixture)?;
    let (binding, _, _) = execute_workspace_participation_replacement(&mut fixture)?;
    let request_id = Uuid::from_u128(0xCD43);
    let departure_request = AgentDepartureReconciliationRequest::new(
        request_id,
        AgentDepartureCommand::Detach(DetachAgent::new(
            binding.agent_id(),
            binding.activity_id(),
            AgentInstanceRevision::new(1)?,
            timestamp(6_000)?,
        )),
        binding.session_id(),
        binding.principal(),
        departure_digest(request_id),
    )?;
    let source_receipt = fixture.source.store.reconcile_agent_departure(
        &fixture.delegation,
        &departure_request,
        timestamp(6_000)?,
    )?;
    let command = LogicalDetachCommand::new(
        fixture.source.workspace,
        binding.activity_id(),
        fixture.participant_id,
        binding.agent_id(),
        binding.session_id(),
        binding.principal(),
        timestamp(6_001)?,
    );
    let _detached =
        fixture
            .catalog
            .commit_logical_detach(command, &source_receipt, timestamp(6_100)?)?;

    fixture.catalog.connection.execute(
        "UPDATE ops_logical_members SET display_name=?1,declared_role=?2,transport_kind=?3 WHERE workspace_id=?4 AND participant_id=?5 AND session_id=?6",
        params![
            "current replacement label",
            "reviewer",
            3_i64,
            fixture.source.workspace.into_uuid().as_bytes(),
            fixture.participant_id.as_bytes(),
            binding.session_id().into_uuid().as_bytes(),
        ],
    )?;

    let receipt = fixture
        .catalog
        .logical_departure(request_id)?
        .ok_or("replacement departure receipt missing after metadata drift")?;
    assert_eq!(receipt.display_name().as_str(), "replacement worker");
    assert_eq!(receipt.declared_role().as_str(), "worker");
    assert_eq!(receipt.transport(), AgentTransportKind::Stdio);
    drop(fixture.catalog);
    fixture.source.cleanup()?;
    fs::remove_dir_all(fixture.catalog_root)?;
    Ok(())
}

#[test]
fn profile_session_replacement_does_not_mint_or_fragment_work_credit() -> Result<(), Box<dyn Error>>
{
    let mut fixture = ready_replacement_fixture()?;
    let assignment_id = Uuid::from_u128(0xCDA0);
    fixture.catalog.connection.execute(
        "INSERT INTO ops_work_credit_entries(credit_id,workspace_id,assignment_id,participant_id,agent_id,activity_id,goal_id,node_id,budget,award_source_sequence,evidence_execution_id,awarded_at_ms,created_digest) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,3,1,?9,3500,?10)",
        rusqlite::params![
            Uuid::from_u128(0xCDA1).as_bytes(),
            fixture.source.workspace.into_uuid().as_bytes(),
            assignment_id.as_bytes(),
            fixture.participant_id.as_bytes(),
            fixture.old_binding.agent_id().into_uuid().as_bytes(),
            fixture.old_binding.activity_id().into_uuid().as_bytes(),
            Uuid::from_u128(0xCDA2).as_bytes(),
            Uuid::from_u128(0xCDA3).as_bytes(),
            Uuid::from_u128(0xCDA4).as_bytes(),
            [0xFA_u8; 32].as_slice(),
        ],
    )?;
    let before_count: i64 = fixture.catalog.connection.query_row(
        "SELECT COUNT(*) FROM ops_work_credit_entries WHERE workspace_id=?1",
        rusqlite::params![fixture.source.workspace.into_uuid().as_bytes()],
        |row| row.get(0),
    )?;
    let before = fixture
        .catalog
        .work_credit_scores(fixture.source.workspace, 6_000)?;
    assert_eq!(before_count, 1);
    assert_eq!(before.total_credits(), 3);
    assert_eq!(before.per_subject().len(), 1);
    assert_eq!(before.per_subject()[0].total_credits(), 3);
    assert_eq!(before.per_subject()[0].source_participant_count(), 1);

    detach_ready_replacement_fixture(&mut fixture)?;
    let _replacement = execute_workspace_participation_replacement(&mut fixture)?;

    let after_count: i64 = fixture.catalog.connection.query_row(
        "SELECT COUNT(*) FROM ops_work_credit_entries WHERE workspace_id=?1",
        rusqlite::params![fixture.source.workspace.into_uuid().as_bytes()],
        |row| row.get(0),
    )?;
    let after = fixture
        .catalog
        .work_credit_scores(fixture.source.workspace, 6_000)?;
    assert_eq!(after_count, before_count);
    assert_eq!(after.total_credits(), before.total_credits());
    assert_eq!(after.per_subject().len(), 1);
    assert_eq!(after.per_subject()[0].total_credits(), 3);
    assert_eq!(after.per_subject()[0].source_participant_count(), 1);
    assert_eq!(
        after.per_subject()[0].subject().participation_id()?,
        Some(fixture.participation_id)
    );

    let catalog_root = fixture.catalog_root.clone();
    drop(fixture.catalog);
    fixture.source.cleanup()?;
    fs::remove_dir_all(catalog_root)?;
    Ok(())
}

#[test]
fn pending_registration_blocks_session_replacement_without_losing_history()
-> Result<(), Box<dyn Error>> {
    let mut fixture = ready_replacement_fixture()?;
    let activity_id = ActivityId::from_uuid(Uuid::from_u128(0xCDC0));
    fixture
        .source
        .store
        .bootstrap_activity(&BootstrapActivity::new(
            activity_id,
            fixture.source.workspace,
            WorkspaceRevision::INITIAL,
            RuntimeInstanceId::from_uuid(Uuid::from_u128(0xCDC1)),
            timestamp(100_000)?,
            timestamp(3_200)?,
        ))?;
    let binding = AgentBindingReservationRequest::new(
        Uuid::from_u128(0xCDC2),
        activity_id,
        AgentInstanceId::from_uuid(Uuid::from_u128(0xCDC3)),
        fixture.identity.old_session.id(),
        AuthenticatedPrincipal::TrustedStdio,
        timestamp(3_300)?,
    )?;
    let request = RegistrationRequest::new(
        fixture.participant_id,
        binding.clone(),
        AgentDisplayName::new("replacement worker".to_owned())?,
        AgentDeclaredRole::new("worker".to_owned())?,
        AgentTransportKind::Stdio,
    )?;
    let _pending = fixture
        .catalog
        .prepare_registration(fixture.source.migration, &request)?;
    detach_ready_replacement_fixture(&mut fixture)?;
    let error = execute_workspace_participation_replacement(&mut fixture)
        .err()
        .ok_or("Session replacement admitted with a pending registration")?;
    assert!(matches!(
        error.downcast_ref::<OperationsError>(),
        Some(OperationsError::Conflict(
            "Session replacement registration still pending"
        ))
    ));
    assert_eq!(
        fixture
            .catalog
            .registration_progress(binding.request_id())?
            .ok_or("pending registration lost")?
            .phase(),
        RegistrationPhase::Pending
    );
    assert!(
        fixture
            .catalog
            .logical_identity_for_session(
                fixture.source.workspace,
                fixture.identity.new_session.id(),
            )?
            .is_none()
    );
    assert_eq!(
        fixture
            .catalog
            .workspace_participation(fixture.source.workspace, fixture.identity.profile_id,)?
            .ok_or("participation lost")?
            .revision(),
        1
    );
    let root = fixture.catalog_root.clone();
    drop(fixture.catalog);
    fixture.source.cleanup()?;
    fs::remove_dir_all(root)?;
    Ok(())
}

#[test]
fn profile_session_replacement_reuses_one_member_and_joined_through_real_forge_effects()
-> Result<(), Box<dyn Error>> {
    let mut fixture = ready_replacement_fixture()?;
    let original_activation = fixture
        .catalog
        .registration_activation(fixture.old_binding.request_id())?
        .ok_or("original activation missing")?;
    detach_ready_replacement_fixture(&mut fixture)?;
    let (binding, committed, activated) =
        execute_workspace_participation_replacement(&mut fixture)?;
    assert_eq!(
        fixture
            .catalog
            .registration_activation(fixture.old_binding.request_id())?,
        Some(original_activation)
    );
    assert_eq!(activated.participant_id(), fixture.participant_id);
    assert_eq!(
        fixture
            .catalog
            .workspace_participation_replacement_progress(binding.request_id())?
            .ok_or("replacement progress missing")?
            .phase(),
        RegistrationPhase::Ready
    );
    assert_eq!(
        logical_plane_count(
            &fixture.catalog,
            "ops_logical_members",
            fixture.source.workspace
        )?,
        1
    );
    assert_eq!(
        logical_plane_count(
            &fixture.catalog,
            "ops_logical_events",
            fixture.source.workspace
        )?,
        1
    );
    assert_eq!(
        committed.joined_event_id(),
        crate::membership::joined_event_id(fixture.source.workspace, fixture.participant_id)
    );
    let visible = fixture
        .catalog
        .visible_logical_members(fixture.source.workspace, 64)?;
    assert_eq!(visible.len(), 1);
    assert_eq!(visible[0].participant_id(), fixture.participant_id);
    assert_eq!(visible[0].session_id(), fixture.identity.new_session.id());
    let current = fixture
        .catalog
        .logical_identity_for_session(fixture.source.workspace, fixture.identity.new_session.id())?
        .ok_or("replacement logical Session identity missing")?;
    assert_eq!(current.participant_id(), fixture.participant_id);
    assert_eq!(current.effect_bindings().len(), 1);
    assert_eq!(current.effect_bindings()[0].agent_id(), binding.agent_id());
    assert!(
        fixture
            .catalog
            .logical_identity_for_session(
                fixture.source.workspace,
                fixture.identity.old_session.id()
            )?
            .is_none()
    );
    let catalog_root = fixture.catalog_root.clone();
    drop(fixture.catalog);
    fixture.source.cleanup()?;
    fs::remove_dir_all(catalog_root)?;
    Ok(())
}

#[test]
#[allow(clippy::too_many_lines)]
fn logical_detach_uses_forge_departure_receipt_without_legacy_operational_writes()
-> Result<(), Box<dyn Error>> {
    let destination = Uuid::from_u128(0xCC00);
    let seed = 0xCC01_u128;
    let mut source = source_fixture(seed, 1, false, destination)?;
    let activity_id = ActivityId::from_uuid(Uuid::from_u128(seed + 2));
    let (catalog_root, catalog_path) = catalog_root(seed)?;
    let mut catalog = OperationsCatalog::open(&catalog_path, destination)?;
    let _prepared = catalog.prepare(&source.freeze)?;
    import_all(&mut catalog, &source)?;
    verify_all(&mut catalog, &source)?;

    let binding = AgentBindingReservationRequest::new(
        Uuid::from_u128(0xCC10),
        activity_id,
        AgentInstanceId::from_uuid(Uuid::from_u128(0xCC70)),
        AgentSessionId::from_uuid(Uuid::from_u128(0xCC71)),
        AuthenticatedPrincipal::TrustedStdio,
        timestamp(1_001)?,
    )?;
    let request = RegistrationRequest::new(
        Uuid::from_u128(0xCC13),
        binding.clone(),
        AgentDisplayName::new("detach worker".to_owned())?,
        AgentDeclaredRole::new("worker".to_owned())?,
        AgentTransportKind::Stdio,
    )?;
    let pending = catalog.prepare_registration(source.migration, &request)?;
    let participant_id = pending.participant_id();
    let reservation =
        source
            .store
            .reserve_agent_binding(&source.freeze, &binding, timestamp(1_002)?)?;
    let _reserved = catalog.record_registration_reservation(&reservation)?;
    let commit =
        catalog.commit_registration_membership(binding.request_id(), &[], timestamp(2_000)?)?;
    let leadership_assignment = Uuid::from_u128(0xCC15);
    insert_deferred_assignment_offer(
        &catalog,
        &source,
        destination,
        &binding,
        leadership_assignment,
    )?;
    let delegation =
        source
            .store
            .delegate_agent_operations(&source.freeze, [0xCC; 32], timestamp(2_500)?)?;
    let activation_request = AgentBindingActivationRequest::new(
        binding.request_id(),
        commit.commit_digest(),
        request.display_name().clone(),
        request.declared_role().clone(),
        request.transport(),
    )?;
    let activation =
        source
            .store
            .activate_agent_binding(&delegation, &activation_request, timestamp(3_000)?)?;
    let _recorded = catalog.record_registration_activation(&activation)?;
    let activity_leadership_scope = crate::LeadershipScope::activity(source.workspace, activity_id);
    let workspace_leadership_scope = crate::LeadershipScope::workspace(source.workspace);
    let activity_leadership =
        catalog.confirm_pending_bootstrap(crate::ConfirmPendingBootstrapCommand {
            scope: activity_leadership_scope,
            expected_revision: 1,
            now_ms: 3_001,
        })?;
    let workspace_leadership =
        catalog.confirm_pending_bootstrap(crate::ConfirmPendingBootstrapCommand {
            scope: workspace_leadership_scope,
            expected_revision: 1,
            now_ms: 3_001,
        })?;
    assert_eq!(
        activity_leadership.state(),
        crate::LeadershipState::Stabilizing
    );
    assert_eq!(
        workspace_leadership.state(),
        crate::LeadershipState::Stabilizing
    );
    let presence = catalog.touch_logical_presence(logical_presence_touch(
        source.workspace,
        &binding,
        AgentPresenceRevision::INITIAL,
        timestamp(3_100)?,
    ))?;
    assert_eq!(presence.presence_revision().get(), 2);

    let legacy_counts_before = source_legacy_operational_counts(&source)?;
    let members_before = logical_plane_count(&catalog, "ops_logical_members", source.workspace)?;
    let joined_events_before =
        logical_plane_count(&catalog, "ops_logical_events", source.workspace)?;

    let request_id = Uuid::from_u128(0xCC14);
    let departure_request = AgentDepartureReconciliationRequest::new(
        request_id,
        AgentDepartureCommand::Detach(DetachAgent::new(
            binding.agent_id(),
            activity_id,
            AgentInstanceRevision::new(1)?,
            timestamp(4_000)?,
        )),
        binding.session_id(),
        binding.principal(),
        departure_digest(request_id),
    )?;
    let source_receipt = source.store.reconcile_agent_departure(
        &delegation,
        &departure_request,
        timestamp(4_000)?,
    )?;
    let counts_after_forge = source_legacy_operational_counts(&source)?;
    assert_eq!(counts_after_forge[0], legacy_counts_before[0]);
    assert_eq!(counts_after_forge[1], legacy_counts_before[1]);
    assert_eq!(counts_after_forge[2], legacy_counts_before[2]);
    assert_eq!(
        counts_after_forge[3],
        legacy_counts_before[3] + 1,
        "Forge records only its own detached focus fact on the effect plane"
    );

    let command = LogicalDetachCommand::new(
        source.workspace,
        activity_id,
        participant_id,
        binding.agent_id(),
        binding.session_id(),
        binding.principal(),
        timestamp(4_001)?,
    );
    let foreign_participant = LogicalDetachCommand::new(
        source.workspace,
        activity_id,
        Uuid::from_u128(0xCC99),
        binding.agent_id(),
        binding.session_id(),
        binding.principal(),
        timestamp(4_001)?,
    );
    assert!(matches!(
        catalog.commit_logical_detach(foreign_participant, &source_receipt, timestamp(4_100)?),
        Err(OperationsError::Conflict("departure participant binding"))
    ));

    let receipt = catalog.commit_logical_detach(command, &source_receipt, timestamp(4_100)?)?;
    assert_eq!(receipt.request_id(), request_id);
    assert_eq!(receipt.participant_id(), participant_id);
    assert_eq!(receipt.kind(), LogicalDepartureKind::Detach);
    assert!(receipt.departed_event_id().is_some());
    assert_eq!(receipt.handoff_started_event_id(), None);
    assert_eq!(receipt.handoff_completed_event_id(), None);
    assert_eq!(receipt.membership_revision(), 2);
    assert!(receipt.presence_cleared());
    assert_eq!(
        source_legacy_operational_counts(&source)?,
        counts_after_forge,
        "the operations commit must not write any source row"
    );
    assert_eq!(
        logical_plane_count(&catalog, "ops_logical_events", source.workspace)?,
        joined_events_before,
        "the single JOINED event journal must stay untouched by detach"
    );
    assert_eq!(
        logical_plane_count(&catalog, "ops_logical_members", source.workspace)?,
        members_before,
        "logical identity must persist after detach"
    );
    assert_eq!(
        logical_plane_count(&catalog, "ops_current_effect_bindings", source.workspace)?,
        members_before - 1,
        "detach must retire the departing current effect binding"
    );
    assert!(
        catalog
            .visible_logical_members(source.workspace, 64)?
            .is_empty(),
        "a detached participant must not remain in the current member projection"
    );
    let activity_after_detach = catalog
        .leadership_view(source.workspace, Some(activity_id), 4_100)?
        .ok_or("activity leadership missing after detach")?;
    let workspace_after_detach = catalog
        .leadership_view(source.workspace, None, 4_100)?
        .ok_or("workspace leadership missing after detach")?;
    assert_eq!(
        activity_after_detach.state(),
        crate::LeadershipState::FailoverRequired
    );
    assert_eq!(activity_after_detach.leader_participant_id(), None);
    assert_eq!(
        workspace_after_detach.state(),
        crate::LeadershipState::FailoverRequired
    );
    assert_eq!(workspace_after_detach.leader_participant_id(), None);
    let activity_revision_after_detach = activity_after_detach.leadership_revision();

    assert_eq!(
        catalog.commit_logical_detach(command, &source_receipt, timestamp(4_200)?)?,
        receipt
    );
    assert_eq!(
        catalog
            .leadership_view(source.workspace, Some(activity_id), 4_200)?
            .ok_or("activity leadership missing after detach replay")?
            .leadership_revision(),
        activity_revision_after_detach,
        "departure replay must not apply leadership failover twice"
    );
    assert!(matches!(
        catalog.touch_logical_presence(logical_presence_touch(
            source.workspace,
            &binding,
            AgentPresenceRevision::INITIAL,
            timestamp(4_300)?,
        )),
        Err(OperationsError::Conflict("logical member not ready"))
    ));

    drop(catalog);
    source.reopen_store()?;
    let catalog = OperationsCatalog::open(&catalog_path, destination)?;
    assert_eq!(catalog.logical_departure(request_id)?, Some(receipt));
    assert!(
        catalog
            .visible_logical_members(source.workspace, 64)?
            .is_empty()
    );
    source.cleanup()?;
    fs::remove_dir_all(catalog_root)?;
    Ok(())
}

#[test]
#[allow(clippy::too_many_lines)]
fn logical_handoff_records_events_and_single_unacknowledged_target_receipt()
-> Result<(), Box<dyn Error>> {
    let destination = Uuid::from_u128(0xCD00);
    let seed = 0xCD01_u128;
    let mut source = source_fixture(seed, 1, false, destination)?;
    source.store.bootstrap_activity(&BootstrapActivity::new(
        ActivityId::from_uuid(Uuid::from_u128(seed + 0x200)),
        source.workspace,
        WorkspaceRevision::INITIAL,
        RuntimeInstanceId::from_uuid(Uuid::from_u128(seed + 0x201)),
        timestamp(100_000)?,
        timestamp(20)?,
    ))?;
    let activity_id = ActivityId::from_uuid(Uuid::from_u128(seed + 0x200));
    let (catalog_root, catalog_path) = catalog_root(seed)?;
    let mut catalog = OperationsCatalog::open(&catalog_path, destination)?;
    catalog.prepare(&source.freeze)?;
    import_all(&mut catalog, &source)?;
    verify_all(&mut catalog, &source)?;

    let source_binding = AgentBindingReservationRequest::new(
        Uuid::from_u128(0xCD10),
        activity_id,
        AgentInstanceId::from_uuid(Uuid::from_u128(0xCD70)),
        AgentSessionId::from_uuid(Uuid::from_u128(0xCD71)),
        AuthenticatedPrincipal::TrustedStdio,
        timestamp(1_001)?,
    )?;
    let target_binding = AgentBindingReservationRequest::new(
        Uuid::from_u128(0xCD20),
        activity_id,
        AgentInstanceId::from_uuid(Uuid::from_u128(0xCD72)),
        AgentSessionId::from_uuid(Uuid::from_u128(0xCD73)),
        AuthenticatedPrincipal::TrustedStdio,
        timestamp(1_002)?,
    )?;
    let mut commits = Vec::new();
    let mut participants = Vec::new();
    for binding in [&source_binding, &target_binding] {
        let request = RegistrationRequest::new(
            Uuid::from_u128(binding.request_id().as_u128() + 3),
            binding.clone(),
            AgentDisplayName::new("handoff worker".to_owned())?,
            AgentDeclaredRole::new("worker".to_owned())?,
            AgentTransportKind::Stdio,
        )?;
        let pending = catalog.prepare_registration(source.migration, &request)?;
        participants.push(pending.participant_id());
        let reservation =
            source
                .store
                .reserve_agent_binding(&source.freeze, binding, timestamp(1_100)?)?;
        let _reserved = catalog.record_registration_reservation(&reservation)?;
        commits.push((
            catalog.commit_registration_membership(binding.request_id(), &[], timestamp(2_000)?)?,
            request,
        ));
    }
    let leadership_assignment = Uuid::from_u128(0xCD15);
    insert_deferred_assignment_offer(
        &catalog,
        &source,
        destination,
        &source_binding,
        leadership_assignment,
    )?;
    let delegation =
        source
            .store
            .delegate_agent_operations(&source.freeze, [0xCD; 32], timestamp(2_500)?)?;
    for (commit, request) in &commits {
        let activation_request = AgentBindingActivationRequest::new(
            commit.request_id(),
            commit.commit_digest(),
            request.display_name().clone(),
            request.declared_role().clone(),
            request.transport(),
        )?;
        let activation = source.store.activate_agent_binding(
            &delegation,
            &activation_request,
            timestamp(3_000)?,
        )?;
        let _recorded = catalog.record_registration_activation(&activation)?;
    }
    let activity_leadership_scope = crate::LeadershipScope::activity(source.workspace, activity_id);
    let workspace_leadership_scope = crate::LeadershipScope::workspace(source.workspace);
    let activity_leadership =
        catalog.confirm_pending_bootstrap(crate::ConfirmPendingBootstrapCommand {
            scope: activity_leadership_scope,
            expected_revision: 1,
            now_ms: 3_001,
        })?;
    let workspace_leadership =
        catalog.confirm_pending_bootstrap(crate::ConfirmPendingBootstrapCommand {
            scope: workspace_leadership_scope,
            expected_revision: 1,
            now_ms: 3_001,
        })?;
    assert_eq!(
        activity_leadership.leader_participant_id(),
        Some(participants[0])
    );
    assert_eq!(
        workspace_leadership.leader_participant_id(),
        Some(participants[0])
    );
    let _claimed = source.store.claim_activity_coordinator(
        ClaimActivityCoordinator::new(
            source.workspace,
            activity_id,
            source_binding.agent_id(),
            source_binding.principal(),
            timestamp(3_200)?,
            source_binding.session_id(),
        ),
        timestamp(3_200)?,
    )?;
    let source_touch = catalog.touch_logical_presence(logical_presence_touch(
        source.workspace,
        &source_binding,
        AgentPresenceRevision::INITIAL,
        timestamp(3_300)?,
    ))?;
    assert_eq!(source_touch.presence_revision().get(), 2);
    let target_touch = catalog.touch_logical_presence(logical_presence_touch(
        source.workspace,
        &target_binding,
        AgentPresenceRevision::INITIAL,
        timestamp(3_301)?,
    ))?;
    assert_eq!(target_touch.presence_revision().get(), 2);

    let legacy_counts_before = source_legacy_operational_counts(&source)?;
    let request_id = Uuid::from_u128(0xCD30);
    let target_agent = source
        .store
        .get_agent(target_binding.agent_id())?
        .ok_or("handoff target Agent missing")?;
    let handoff = AgentHandoff::new(
        ActivityCoordinatorActor::new(
            source.workspace,
            activity_id,
            source_binding.agent_id(),
            ActivityCoordinatorRevision::INITIAL,
            source_binding.principal(),
            source_binding.session_id(),
        ),
        AgentInstanceRevision::new(1)?,
        target_binding.agent_id(),
        target_binding.session_id(),
        target_agent.principal(),
        AgentInstanceRevision::new(1)?,
        CoordinationSnapshot::empty(),
        AgentEventId::from_uuid(request_id),
        timestamp(4_000)?,
    );
    let departure_request = AgentDepartureReconciliationRequest::new(
        request_id,
        AgentDepartureCommand::Handoff(Box::new(handoff)),
        source_binding.session_id(),
        source_binding.principal(),
        departure_digest(request_id),
    )?;
    let source_receipt = source.store.reconcile_agent_departure(
        &delegation,
        &departure_request,
        timestamp(4_000)?,
    )?;
    let counts_after_forge = source_legacy_operational_counts(&source)?;
    assert_eq!(counts_after_forge[0], legacy_counts_before[0]);
    assert_eq!(counts_after_forge[1], legacy_counts_before[1]);
    assert_eq!(counts_after_forge[2], legacy_counts_before[2]);
    assert_eq!(
        counts_after_forge[3],
        legacy_counts_before[3] + 2,
        "Forge records the detached focus fact plus the target focus transfer"
    );

    let detach_command = LogicalDetachCommand::new(
        source.workspace,
        activity_id,
        participants[0],
        source_binding.agent_id(),
        source_binding.session_id(),
        source_binding.principal(),
        timestamp(4_001)?,
    );
    let self_target = LogicalHandoffCommand::new(
        detach_command,
        participants[0],
        source_binding.agent_id(),
        source_binding.session_id(),
    );
    assert!(matches!(
        catalog.commit_logical_handoff(self_target, &source_receipt, timestamp(4_100)?),
        Err(OperationsError::Conflict("departure handoff target"))
    ));
    let foreign_target = LogicalHandoffCommand::new(
        detach_command,
        Uuid::from_u128(0xCD99),
        target_binding.agent_id(),
        target_binding.session_id(),
    );
    assert!(matches!(
        catalog.commit_logical_handoff(foreign_target, &source_receipt, timestamp(4_100)?),
        Err(OperationsError::Conflict(
            "departure handoff target binding"
        ))
    ));

    let command = LogicalHandoffCommand::new(
        detach_command,
        participants[1],
        target_binding.agent_id(),
        target_binding.session_id(),
    );
    let receipt = catalog.commit_logical_handoff(command, &source_receipt, timestamp(4_100)?)?;
    assert_eq!(receipt.request_id(), request_id);
    assert_eq!(receipt.participant_id(), participants[0]);
    assert_eq!(receipt.kind(), LogicalDepartureKind::Handoff);
    assert_eq!(receipt.departed_event_id(), None);
    assert!(receipt.handoff_started_event_id().is_some());
    assert!(receipt.handoff_completed_event_id().is_some());
    assert_ne!(
        receipt.handoff_started_event_id(),
        receipt.handoff_completed_event_id()
    );
    assert_eq!(receipt.membership_revision(), 3);
    assert!(receipt.presence_cleared());
    assert_eq!(receipt.target_participant_id(), Some(participants[1]));
    assert_eq!(
        source_legacy_operational_counts(&source)?,
        counts_after_forge,
        "the operations commit must not write any source row"
    );
    assert_eq!(
        logical_plane_count(&catalog, "ops_logical_events", source.workspace)?,
        2,
        "the single JOINED event journal must stay untouched by handoff"
    );
    assert_eq!(
        logical_plane_count(&catalog, "ops_current_effect_bindings", source.workspace)?,
        1,
        "handoff must retire only the departing source binding"
    );
    let visible_after_handoff = catalog.visible_logical_members(source.workspace, 64)?;
    assert_eq!(visible_after_handoff.len(), 1);
    assert_eq!(visible_after_handoff[0].participant_id(), participants[1]);
    let activity_after_handoff = catalog
        .leadership_view(source.workspace, Some(activity_id), 4_100)?
        .ok_or("activity leadership missing after handoff")?;
    let workspace_after_handoff = catalog
        .leadership_view(source.workspace, None, 4_100)?
        .ok_or("workspace leadership missing after handoff")?;
    assert_eq!(
        activity_after_handoff.state(),
        crate::LeadershipState::FailoverRequired
    );
    assert_eq!(activity_after_handoff.leader_participant_id(), None);
    assert_eq!(
        workspace_after_handoff.state(),
        crate::LeadershipState::FailoverRequired
    );
    assert_eq!(workspace_after_handoff.leader_participant_id(), None);
    assert_ne!(
        activity_after_handoff.leader_participant_id(),
        Some(participants[1]),
        "logical handoff must not bypass election and Broker authority fencing"
    );
    let activity_revision_after_handoff = activity_after_handoff.leadership_revision();

    let (target_receipts, unacknowledged): (i64, i64) = catalog.connection.query_row(
        "SELECT COUNT(*), COALESCE(SUM(acknowledged_at_ms IS NULL),0) FROM ops_logical_departure_receipts WHERE workspace_id=?1",
        params![source.workspace.into_uuid().as_bytes()],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    assert_eq!(target_receipts, 1, "exactly one handoff target receipt");
    assert_eq!(
        unacknowledged, 1,
        "the target receipt must be unacknowledged"
    );

    assert_eq!(
        catalog.commit_logical_handoff(command, &source_receipt, timestamp(4_200)?)?,
        receipt
    );
    assert_eq!(
        catalog
            .leadership_view(source.workspace, Some(activity_id), 4_200)?
            .ok_or("activity leadership missing after handoff replay")?
            .leadership_revision(),
        activity_revision_after_handoff,
        "handoff replay must not apply leadership failover twice"
    );
    assert_eq!(
        logical_plane_count(&catalog, "ops_logical_departure_receipts", source.workspace)?,
        1
    );
    assert!(matches!(
        catalog.touch_logical_presence(logical_presence_touch(
            source.workspace,
            &source_binding,
            AgentPresenceRevision::INITIAL,
            timestamp(4_300)?,
        )),
        Err(OperationsError::Conflict("logical member not ready"))
    ));
    let advanced_target_presence = catalog.touch_logical_presence(logical_presence_touch(
        source.workspace,
        &target_binding,
        AgentPresenceRevision::new(2)?,
        timestamp(4_301)?,
    ))?;
    assert_eq!(advanced_target_presence.presence_revision().get(), 3);
    assert!(matches!(
        catalog.touch_logical_presence(logical_presence_touch(
            source.workspace,
            &target_binding,
            AgentPresenceRevision::INITIAL,
            timestamp(4_302)?,
        )),
        Err(OperationsError::Conflict("presence revision"))
    ));

    drop(catalog);
    source.reopen_store()?;
    let catalog = OperationsCatalog::open(&catalog_path, destination)?;
    assert_eq!(catalog.logical_departure(request_id)?, Some(receipt));
    let visible_after_reopen = catalog.visible_logical_members(source.workspace, 64)?;
    assert_eq!(visible_after_reopen.len(), 1);
    assert_eq!(visible_after_reopen[0].participant_id(), participants[1]);
    source.cleanup()?;
    fs::remove_dir_all(catalog_root)?;
    Ok(())
}
