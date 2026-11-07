use super::*;
use std::{
    error::Error,
    fs,
    path::{Path, PathBuf},
    sync::{
        Arc, Barrier,
        atomic::{AtomicU64, Ordering},
    },
    thread,
    time::Duration,
};

const DESTINATION_ID: Uuid = Uuid::from_u128(0x100);
const WORKSPACE_ID: Uuid = Uuid::from_u128(0x200);
const RULE_ID: Uuid = Uuid::from_u128(0x300);
const REQUESTER_SESSION_ID: Uuid = Uuid::from_u128(0x400);

fn workspace() -> WorkspaceId {
    WorkspaceId::from_uuid(WORKSPACE_ID)
}

fn requester_session() -> AgentSessionId {
    AgentSessionId::from_uuid(REQUESTER_SESSION_ID)
}

fn owner(seed: u128) -> Result<EvidenceSourceOwnerId, OperationsError> {
    EvidenceSourceOwnerId::new(
        EvidenceOwnerNamespace::RuntimeAdapter,
        Uuid::from_u128(seed),
    )
}

fn forge_owner(seed: u128) -> Result<EvidenceSourceOwnerId, OperationsError> {
    EvidenceSourceOwnerId::new(EvidenceOwnerNamespace::Forge, Uuid::from_u128(seed))
}

fn runtime_text_predicate(
    fact: ActualRuntimeFact,
    value: &str,
) -> Result<RuntimeFactPredicate, OperationsError> {
    RuntimeFactPredicate::exact_text(
        fact,
        BTreeSet::from([ExactRuntimeText::new(value.to_owned())?]),
    )
}

fn runtime_digest_predicate(
    fact: ActualRuntimeFact,
    value: u8,
) -> Result<RuntimeFactPredicate, OperationsError> {
    RuntimeFactPredicate::exact_digests(
        fact,
        BTreeSet::from([RuntimeFactDigest::new(
            ExactRuntimeText::new("test-digest-v1".to_owned())?,
            vec![value; 32],
        )?]),
    )
}

fn runtime_predicates(
    model: &str,
    toolset: &str,
    environment_digest: u8,
) -> Result<BTreeSet<RuntimeFactPredicate>, OperationsError> {
    Ok(BTreeSet::from([
        runtime_text_predicate(ActualRuntimeFact::Model, model)?,
        runtime_text_predicate(ActualRuntimeFact::ToolVersions, toolset)?,
        runtime_digest_predicate(
            ActualRuntimeFact::EnvironmentFingerprint,
            environment_digest,
        )?,
    ]))
}

fn actor(seed: u128) -> AuthenticatedPrincipal {
    AuthenticatedPrincipal::DirectToken(DirectTokenId::from_uuid(Uuid::from_u128(seed)))
}

fn policy(
    domain: &str,
    runtime_owner: EvidenceSourceOwnerId,
) -> Result<ReviewerQualificationRulePolicy, OperationsError> {
    let fact_predicates = runtime_predicates(domain, "test-toolset-v1", 0xA5)?;
    let evidence_requirements =
        BTreeSet::from([QualificationEvidenceRequirement::ActualRuntimeProvenance {
            source_owner: runtime_owner,
            fact_predicates,
        }]);
    ReviewerQualificationRulePolicy::new(
        QualificationDomain::new(domain.to_owned())?,
        QualificationScope::new("workspace_review".to_owned())?,
        evidence_requirements,
    )
}

fn publish_command(
    request_id: u128,
    expected_generation: Option<ReviewerQualificationRuleGeneration>,
    operator: AuthenticatedPrincipal,
    policy: ReviewerQualificationRulePolicy,
) -> Result<ReviewerQualificationRuleMutationCommand, OperationsError> {
    ReviewerQualificationRuleMutationCommand::new(
        workspace(),
        RULE_ID,
        Uuid::from_u128(request_id),
        expected_generation,
        requester_session(),
        operator,
        ReviewerQualificationRuleChange::Publish { policy },
    )
}

#[test]
fn exported_bounds_preserve_policy_limits_and_closed_runtime_fact_count() {
    assert_eq!(MAX_QUALIFICATION_EVIDENCE_REQUIREMENTS, 16);
    assert_eq!(MAX_RUNTIME_FACT_ALLOWLIST_VALUES, 32);
    assert_eq!(MAX_RUNTIME_FACT_PREDICATES, 5);
}

#[test]
fn mutation_command_accepts_only_direct_token_principal_kind() -> Result<(), Box<dyn Error>> {
    let change = || -> Result<ReviewerQualificationRuleChange, OperationsError> {
        Ok(ReviewerQualificationRuleChange::Publish {
            policy: policy("quality", owner(0x501)?)?,
        })
    };
    let direct_token = ReviewerQualificationRuleMutationCommand::new(
        workspace(),
        RULE_ID,
        Uuid::from_u128(0x710),
        None,
        requester_session(),
        actor(0x601),
        change()?,
    );
    assert!(direct_token.is_ok());

    let nil_session = ReviewerQualificationRuleMutationCommand::new(
        workspace(),
        RULE_ID,
        Uuid::from_u128(0x713),
        None,
        AgentSessionId::from_uuid(Uuid::nil()),
        actor(0x601),
        change()?,
    );
    assert!(matches!(nil_session, Err(OperationsError::Bounds(_))));

    let trusted_stdio = ReviewerQualificationRuleMutationCommand::new(
        workspace(),
        RULE_ID,
        Uuid::from_u128(0x711),
        None,
        requester_session(),
        AuthenticatedPrincipal::TrustedStdio,
        change()?,
    );
    assert!(matches!(trusted_stdio, Err(OperationsError::Conflict(_))));

    let oauth = ReviewerQualificationRuleMutationCommand::new(
        workspace(),
        RULE_ID,
        Uuid::from_u128(0x712),
        None,
        requester_session(),
        AuthenticatedPrincipal::OAuthGrant(forge_domain::OAuthGrantId::from_uuid(Uuid::from_u128(
            0x602,
        ))),
        change()?,
    );
    assert!(matches!(oauth, Err(OperationsError::Conflict(_))));
    Ok(())
}

fn in_memory_catalog() -> Result<OperationsCatalog, Box<dyn Error>> {
    let mut connection = Connection::open_in_memory()?;
    connection.pragma_update(None, "foreign_keys", true)?;
    seed_minimal_verified_import(&connection, workspace())?;
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    schema::create_schema(&transaction)?;
    transaction.commit()?;
    Ok(OperationsCatalog {
        connection,
        destination: DESTINATION_ID,
    })
}

/// Minimal owner-side import fixture: rule policy is not Workspace authority.
/// The production guard reads this verified import and destination identity in
/// the same transaction as a rule mutation.
fn seed_minimal_verified_import(
    connection: &Connection,
    verified_workspace: WorkspaceId,
) -> Result<(), Box<dyn Error>> {
    connection.execute_batch(
        "CREATE TABLE ops_catalog_identity (
            singleton INTEGER PRIMARY KEY CHECK(singleton=1),
            destination_id BLOB NOT NULL UNIQUE CHECK(length(destination_id)=16)
        );
        CREATE TABLE ops_imports (
            workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16),
            destination_id BLOB NOT NULL CHECK(length(destination_id)=16),
            phase INTEGER NOT NULL CHECK(phase IN(0,1,2,3))
        );",
    )?;
    connection.execute(
        "INSERT INTO ops_catalog_identity(singleton,destination_id) VALUES(1,?1)",
        params![DESTINATION_ID.as_bytes()],
    )?;
    connection.execute(
        "INSERT INTO ops_imports(workspace_id,destination_id,phase) VALUES(?1,?2,2)",
        params![
            verified_workspace.into_uuid().as_bytes(),
            DESTINATION_ID.as_bytes(),
        ],
    )?;
    Ok(())
}

#[test]
fn empty_workspace_has_no_rule_or_implicit_qualification() -> Result<(), Box<dyn Error>> {
    let catalog = in_memory_catalog()?;
    assert!(
        catalog
            .reviewer_qualification_rule(workspace(), RULE_ID)?
            .is_none()
    );

    let empty = ReviewerQualificationRulePolicy::new(
        QualificationDomain::new("repository_quality".to_owned())?,
        QualificationScope::new("workspace_review".to_owned())?,
        BTreeSet::new(),
    );
    assert!(matches!(empty, Err(OperationsError::Bounds(_))));

    let incomplete_runtime =
        BTreeSet::from([QualificationEvidenceRequirement::ActualRuntimeProvenance {
            source_owner: owner(0x501)?,
            fact_predicates: BTreeSet::from([runtime_text_predicate(
                ActualRuntimeFact::Model,
                "user-selected-model",
            )?]),
        }]);
    let missing_model_tool_environment = ReviewerQualificationRulePolicy::new(
        QualificationDomain::new("repository_quality".to_owned())?,
        QualificationScope::new("workspace_review".to_owned())?,
        incomplete_runtime,
    );
    assert!(matches!(
        missing_model_tool_environment,
        Err(OperationsError::Bounds(_))
    ));
    Ok(())
}

#[test]
fn independent_work_is_an_explicit_typed_requirement() -> Result<(), Box<dyn Error>> {
    let runtime_owner = owner(0x501)?;
    let quality_owner = forge_owner(0x501)?;
    let requirements = BTreeSet::from([
        QualificationEvidenceRequirement::ActualRuntimeProvenance {
            source_owner: runtime_owner,
            fact_predicates: runtime_predicates(
                "user-selected-model",
                "user-selected-toolset",
                0x61,
            )?,
        },
        QualificationEvidenceRequirement::IndependentlyAcceptedWork {
            source_owner: quality_owner,
        },
    ]);
    let policy = ReviewerQualificationRulePolicy::new(
        QualificationDomain::new("repository_quality".to_owned())?,
        QualificationScope::new("workspace_review".to_owned())?,
        requirements,
    )?;
    assert!(policy.evidence_requirements().contains(
        &QualificationEvidenceRequirement::IndependentlyAcceptedWork {
            source_owner: quality_owner,
        }
    ));
    assert_eq!(quality_owner.namespace(), EvidenceOwnerNamespace::Forge);
    assert_eq!(quality_owner.id(), runtime_owner.id());
    assert_ne!(quality_owner, runtime_owner);
    let digest = RuntimeFactDigest::new(
        ExactRuntimeText::new("test-digest-v1".to_owned())?,
        vec![0x61; 32],
    )?;
    assert_eq!(digest.algorithm().as_str(), "test-digest-v1");
    assert_eq!(digest.bytes(), &[0x61; 32]);

    let serialized = serde_json::to_string(&policy)?;
    assert!(serialized.contains("\"valueKind\":\"text\""));
    assert!(serialized.contains("\"valueKind\":\"digest\""));
    assert!(serialized.contains("\"namespace\":\"runtime_adapter\""));
    assert!(serialized.contains("\"namespace\":\"forge\""));
    let restored: ReviewerQualificationRulePolicy = serde_json::from_str(&serialized)?;
    assert_eq!(restored, policy);
    Ok(())
}

#[test]
fn source_namespaces_and_runtime_value_kinds_cannot_alias() -> Result<(), Box<dyn Error>> {
    let wrong_runtime_owner = forge_owner(0x501)?;
    let wrong_namespace = ReviewerQualificationRulePolicy::new(
        QualificationDomain::new("repository_quality".to_owned())?,
        QualificationScope::new("workspace_review".to_owned())?,
        BTreeSet::from([QualificationEvidenceRequirement::ActualRuntimeProvenance {
            source_owner: wrong_runtime_owner,
            fact_predicates: runtime_predicates("user-selected-model", "user-toolset", 0x61)?,
        }]),
    );
    assert!(matches!(wrong_namespace, Err(OperationsError::Bounds(_))));

    let wrong_work_namespace = ReviewerQualificationRulePolicy::new(
        QualificationDomain::new("repository_quality".to_owned())?,
        QualificationScope::new("workspace_review".to_owned())?,
        BTreeSet::from([
            QualificationEvidenceRequirement::ActualRuntimeProvenance {
                source_owner: owner(0x501)?,
                fact_predicates: runtime_predicates("user-selected-model", "user-toolset", 0x61)?,
            },
            QualificationEvidenceRequirement::IndependentlyAcceptedWork {
                source_owner: owner(0x502)?,
            },
        ]),
    );
    assert!(matches!(
        wrong_work_namespace,
        Err(OperationsError::Bounds(_))
    ));

    let split_runtime_bundle = ReviewerQualificationRulePolicy::new(
        QualificationDomain::new("repository_quality".to_owned())?,
        QualificationScope::new("workspace_review".to_owned())?,
        BTreeSet::from([
            QualificationEvidenceRequirement::ActualRuntimeProvenance {
                source_owner: owner(0x501)?,
                fact_predicates: BTreeSet::from([runtime_text_predicate(
                    ActualRuntimeFact::Model,
                    "user-selected-model",
                )?]),
            },
            QualificationEvidenceRequirement::ActualRuntimeProvenance {
                source_owner: owner(0x502)?,
                fact_predicates: BTreeSet::from([
                    runtime_text_predicate(ActualRuntimeFact::ToolVersions, "user-toolset")?,
                    runtime_digest_predicate(ActualRuntimeFact::EnvironmentFingerprint, 0x61)?,
                ]),
            },
        ]),
    );
    assert!(matches!(
        split_runtime_bundle,
        Err(OperationsError::Bounds(_))
    ));

    let mut predicates = runtime_predicates("user-selected-model", "user-toolset", 0x61)?;
    predicates.insert(runtime_digest_predicate(ActualRuntimeFact::Model, 0x62)?);
    let mixed_kind_for_one_fact = ReviewerQualificationRulePolicy::new(
        QualificationDomain::new("repository_quality".to_owned())?,
        QualificationScope::new("workspace_review".to_owned())?,
        BTreeSet::from([QualificationEvidenceRequirement::ActualRuntimeProvenance {
            source_owner: owner(0x501)?,
            fact_predicates: predicates,
        }]),
    );
    assert!(matches!(
        mixed_kind_for_one_fact,
        Err(OperationsError::Bounds(_))
    ));

    assert!(ExactRuntimeText::new(String::new()).is_err());
    let exact_whitespace = ExactRuntimeText::new(" model-v1 ".to_owned())?;
    assert_eq!(exact_whitespace.as_str(), " model-v1 ");
    assert!(RuntimeFactPredicate::exact_text(ActualRuntimeFact::Model, BTreeSet::new()).is_err());
    Ok(())
}

#[test]
fn identical_request_replays_before_stale_cas_and_keeps_issuer() -> Result<(), Box<dyn Error>> {
    let mut catalog = in_memory_catalog()?;
    let original_actor = actor(0x601);
    let command = publish_command(
        0x701,
        None,
        original_actor,
        policy("quality", owner(0x501)?)?,
    )?;
    let first = catalog.mutate_reviewer_qualification_rule(command.clone())?;
    assert!(!first.replayed());
    assert_eq!(first.generation().generation().get(), 1);

    // The original expected generation is now stale. Exact same actor and
    // payload replay is resolved by request ID before evaluating the CAS.
    let replay = catalog.mutate_reviewer_qualification_rule(command)?;
    assert!(replay.replayed());
    assert_eq!(replay.generation().generation().get(), 1);
    assert_eq!(replay.generation().original_actor(), original_actor);
    Ok(())
}

#[test]
fn fresh_mutation_rejects_unverified_workspace_without_rule_rows() -> Result<(), Box<dyn Error>> {
    let mut catalog = in_memory_catalog()?;
    let unverified_workspace = WorkspaceId::from_uuid(Uuid::from_u128(0x201));
    let command = ReviewerQualificationRuleMutationCommand::new(
        unverified_workspace,
        RULE_ID,
        Uuid::from_u128(0x702),
        None,
        requester_session(),
        actor(0x601),
        ReviewerQualificationRuleChange::Publish {
            policy: policy("quality", owner(0x501)?)?,
        },
    )?;

    assert!(matches!(
        catalog.mutate_reviewer_qualification_rule(command),
        Err(OperationsError::Conflict(_))
    ));
    let generation_count: i64 = catalog.connection.query_row(
        "SELECT COUNT(*) FROM ops_reviewer_qualification_rule_generations
         WHERE workspace_id=?1 AND rule_id=?2",
        params![
            unverified_workspace.into_uuid().as_bytes(),
            RULE_ID.as_bytes()
        ],
        |row| row.get(0),
    )?;
    let head_count: i64 = catalog.connection.query_row(
        "SELECT COUNT(*) FROM ops_reviewer_qualification_rule_heads
         WHERE workspace_id=?1 AND rule_id=?2",
        params![
            unverified_workspace.into_uuid().as_bytes(),
            RULE_ID.as_bytes()
        ],
        |row| row.get(0),
    )?;
    assert_eq!(generation_count, 0);
    assert_eq!(head_count, 0);
    Ok(())
}

#[test]
fn another_operator_uses_readback_and_cannot_rewrite_the_original_issuer()
-> Result<(), Box<dyn Error>> {
    let mut catalog = in_memory_catalog()?;
    let original_actor = actor(0x601);
    let original_session = requester_session();
    let request_id = Uuid::from_u128(0x701);
    let first_command = publish_command(
        request_id.as_u128(),
        None,
        original_actor,
        policy("quality", owner(0x501)?)?,
    )?;
    let first = catalog.mutate_reviewer_qualification_rule(first_command.clone())?;

    let other_operator_retry = publish_command(
        request_id.as_u128(),
        None,
        actor(0x602),
        policy("quality", owner(0x501)?)?,
    )?;
    assert!(matches!(
        catalog.mutate_reviewer_qualification_rule(other_operator_retry),
        Err(OperationsError::Conflict(_))
    ));

    let changed_session_retry = ReviewerQualificationRuleMutationCommand::new(
        workspace(),
        RULE_ID,
        request_id,
        None,
        AgentSessionId::from_uuid(Uuid::from_u128(0x401)),
        original_actor,
        ReviewerQualificationRuleChange::Publish {
            policy: policy("quality", owner(0x501)?)?,
        },
    )?;
    assert!(matches!(
        catalog.mutate_reviewer_qualification_rule(changed_session_retry),
        Err(OperationsError::Conflict(_))
    ));

    catalog.connection.execute(
        "UPDATE ops_imports SET phase=3 WHERE workspace_id=?1 AND destination_id=?2",
        params![WORKSPACE_ID.as_bytes(), DESTINATION_ID.as_bytes()],
    )?;
    assert!(matches!(
        catalog.mutate_reviewer_qualification_rule(first_command),
        Err(OperationsError::Conflict(_))
    ));

    // The Backend must authorize the current reader before this internal
    // readback. Readback is historical and remains available after the import
    // is no longer verified; it is not a fresh mutation replay.
    let recovered = catalog
        .reviewer_qualification_rule_request(workspace(), RULE_ID, request_id)?
        .ok_or("missing exact request readback")?;
    assert_eq!(recovered.original_actor(), original_actor);
    assert_eq!(recovered.requester_session_id(), original_session);
    assert_eq!(recovered.request_id(), request_id);
    assert_eq!(first.generation(), &recovered);
    assert!(
        catalog
            .reviewer_qualification_rule_request(
                WorkspaceId::from_uuid(Uuid::from_u128(0x201)),
                RULE_ID,
                request_id,
            )?
            .is_none()
    );
    Ok(())
}

#[test]
fn changed_payload_with_same_request_id_conflicts() -> Result<(), Box<dyn Error>> {
    let mut catalog = in_memory_catalog()?;
    let operator = actor(0x601);
    let command = publish_command(0x701, None, operator, policy("quality", owner(0x501)?)?)?;
    catalog.mutate_reviewer_qualification_rule(command)?;

    let changed = publish_command(0x701, None, operator, policy("security", owner(0x501)?)?)?;
    assert!(matches!(
        catalog.mutate_reviewer_qualification_rule(changed),
        Err(OperationsError::Conflict(_))
    ));
    Ok(())
}

#[test]
fn replacement_and_deactivation_append_immutable_future_only_generations()
-> Result<(), Box<dyn Error>> {
    let mut catalog = in_memory_catalog()?;
    let operator = actor(0x601);
    let first_policy = policy("quality", owner(0x501)?)?;
    let first = catalog.mutate_reviewer_qualification_rule(publish_command(
        0x701,
        None,
        operator,
        first_policy.clone(),
    )?)?;
    let first_generation = first.generation().generation();
    assert_eq!(first.generation().policy(), &first_policy);

    let replacement_policy = policy("security", owner(0x502)?)?;
    let replacement = catalog.mutate_reviewer_qualification_rule(publish_command(
        0x702,
        Some(first_generation),
        operator,
        replacement_policy.clone(),
    )?)?;
    assert_eq!(replacement.generation().generation().get(), 2);
    assert_eq!(first.generation().policy().domain().as_str(), "quality");
    assert_eq!(replacement.generation().policy(), &replacement_policy);

    let deactivate = ReviewerQualificationRuleMutationCommand::new(
        workspace(),
        RULE_ID,
        Uuid::from_u128(0x703),
        Some(replacement.generation().generation()),
        requester_session(),
        operator,
        ReviewerQualificationRuleChange::DeactivateForFuturePools,
    )?;
    let deactivated = catalog.mutate_reviewer_qualification_rule(deactivate)?;
    assert_eq!(deactivated.generation().generation().get(), 3);
    assert_eq!(
        deactivated.generation().state(),
        ReviewerQualificationRuleState::DeactivatedForFuturePools
    );
    assert_eq!(
        catalog
            .reviewer_qualification_rule_history(workspace(), RULE_ID, None, 8)?
            .entries()
            .iter()
            .map(ReviewerQualificationRuleGenerationRecord::generation)
            .map(ReviewerQualificationRuleGeneration::get)
            .collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
    let first_page = catalog.reviewer_qualification_rule_history(workspace(), RULE_ID, None, 1)?;
    assert!(first_page.has_more());
    let next = first_page.next_after().ok_or("missing history cursor")?;
    assert_eq!(next.get(), 1);
    let second_page =
        catalog.reviewer_qualification_rule_history(workspace(), RULE_ID, Some(next), 1)?;
    assert!(second_page.has_more());
    assert_eq!(second_page.entries()[0].generation().get(), 2);

    let update = catalog.connection.execute(
        "UPDATE ops_reviewer_qualification_rule_generations SET state=1
         WHERE workspace_id=?1 AND rule_id=?2 AND generation=1",
        params![WORKSPACE_ID.as_bytes(), RULE_ID.as_bytes()],
    );
    assert!(update.is_err());
    let delete = catalog.connection.execute(
        "DELETE FROM ops_reviewer_qualification_rule_generations
         WHERE workspace_id=?1 AND rule_id=?2 AND generation=1",
        params![WORKSPACE_ID.as_bytes(), RULE_ID.as_bytes()],
    );
    assert!(delete.is_err());
    Ok(())
}

#[test]
fn schema_addition_preserves_existing_data_starts_empty_and_reopens() -> Result<(), Box<dyn Error>>
{
    let database = TestDatabase::new();
    let mut connection = Connection::open(&database.path)?;
    connection.execute(
        "CREATE TABLE preexisting_owner_data(value INTEGER NOT NULL)",
        [],
    )?;
    connection.execute("INSERT INTO preexisting_owner_data(value) VALUES(41)", [])?;
    connection.pragma_update(None, "foreign_keys", true)?;
    seed_minimal_verified_import(&connection, workspace())?;
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    schema::create_schema(&transaction)?;
    transaction.commit()?;

    let existing_value: i64 =
        connection.query_row("SELECT value FROM preexisting_owner_data", [], |row| {
            row.get(0)
        })?;
    let rule_count: i64 = connection.query_row(
        "SELECT COUNT(*) FROM ops_reviewer_qualification_rule_generations",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(existing_value, 41);
    assert_eq!(rule_count, 0);
    schema::validate_schema(&connection)?;
    drop(connection);

    let mut reopened = open_file_catalog(&database.path)?;
    assert!(
        reopened
            .reviewer_qualification_rule(workspace(), RULE_ID)?
            .is_none()
    );
    let mutation = reopened.mutate_reviewer_qualification_rule(publish_command(
        0x701,
        None,
        actor(0x601),
        policy("quality", owner(0x501)?)?,
    )?)?;
    let expected_generation = mutation.generation().generation();
    drop(reopened);

    let reopened = open_file_catalog(&database.path)?;
    let history = reopened.reviewer_qualification_rule_history(
        workspace(),
        RULE_ID,
        None,
        MAX_HISTORY_PAGE_SIZE,
    )?;
    assert_eq!(history.entries().len(), 1);
    assert_eq!(history.entries()[0].generation(), expected_generation);
    assert_eq!(
        history.entries()[0].requester_session_id(),
        requester_session()
    );
    Ok(())
}

#[test]
fn concurrent_first_generation_compare_and_set_has_one_winner() -> Result<(), Box<dyn Error>> {
    let database = TestDatabase::new();
    initialize_file_schema(&database.path)?;
    let barrier = Arc::new(Barrier::new(2));
    let command_one =
        publish_command(0x701, None, actor(0x601), policy("quality", owner(0x501)?)?)?;
    let command_two = publish_command(
        0x702,
        None,
        actor(0x602),
        policy("security", owner(0x502)?)?,
    )?;
    let first = spawn_mutation(database.path.clone(), Arc::clone(&barrier), command_one);
    let second = spawn_mutation(database.path.clone(), barrier, command_two);
    let first_won = first
        .join()
        .map_err(|_| std::io::Error::other("qualification CAS worker panicked"))?
        .map_err(std::io::Error::other)?;
    let second_won = second
        .join()
        .map_err(|_| std::io::Error::other("qualification CAS worker panicked"))?
        .map_err(std::io::Error::other)?;
    assert_ne!(first_won, second_won);

    let catalog = open_file_catalog(&database.path)?;
    let current = catalog
        .reviewer_qualification_rule(workspace(), RULE_ID)?
        .ok_or("missing winning rule generation")?;
    assert_eq!(current.generation().get(), 1);
    Ok(())
}

fn spawn_mutation(
    path: PathBuf,
    barrier: Arc<Barrier>,
    command: ReviewerQualificationRuleMutationCommand,
) -> thread::JoinHandle<Result<bool, String>> {
    thread::spawn(move || {
        let mut catalog = open_file_catalog(&path).map_err(|error| error.to_string())?;
        barrier.wait();
        match catalog.mutate_reviewer_qualification_rule(command) {
            Ok(_) => Ok(true),
            Err(OperationsError::Conflict(_)) => Ok(false),
            Err(error) => Err(error.to_string()),
        }
    })
}

fn initialize_file_schema(path: &Path) -> Result<(), Box<dyn Error>> {
    let mut connection = Connection::open(path)?;
    connection.busy_timeout(Duration::from_secs(5))?;
    connection.pragma_update(None, "foreign_keys", true)?;
    seed_minimal_verified_import(&connection, workspace())?;
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    schema::create_schema(&transaction)?;
    transaction.commit()?;
    Ok(())
}

fn open_file_catalog(path: &Path) -> Result<OperationsCatalog, Box<dyn Error>> {
    let connection = Connection::open(path)?;
    connection.busy_timeout(Duration::from_secs(5))?;
    connection.pragma_update(None, "foreign_keys", true)?;
    Ok(OperationsCatalog {
        connection,
        destination: DESTINATION_ID,
    })
}

struct TestDatabase {
    path: PathBuf,
}

impl TestDatabase {
    fn new() -> Self {
        static NEXT_ID: AtomicU64 = AtomicU64::new(0);
        let sequence = NEXT_ID.fetch_add(1, Ordering::Relaxed);
        Self {
            path: std::env::temp_dir().join(format!(
                "reviewer-qualification-rule-{}-{sequence}.sqlite",
                std::process::id()
            )),
        }
    }
}

impl Drop for TestDatabase {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
        let _ = fs::remove_file(self.path.with_extension("sqlite-wal"));
        let _ = fs::remove_file(self.path.with_extension("sqlite-shm"));
    }
}
