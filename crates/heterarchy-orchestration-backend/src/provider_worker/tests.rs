//! Real socket worker readback/permission/restart journey with isolated auth.

use crate::{
    client::{ClientError, OrchestrationHttpClient, ServiceCode},
    wire::{
        node_chat::ChatProviderStateWire,
        provider_worker::{
            ProviderDeliveryConfirmation, ProviderMessageFence, ProviderMessageInspection,
            ProviderMessageSelectionQuery, ProviderMessageTargetWire,
            ProviderMessageUnknownIsolationQuery, ProviderMessageUnknownIsolationReasonWire,
            ProviderMessageUnknownIsolationRequest, ProviderProvisionInspection,
        },
    },
};
use forge_domain::{AuthenticatedPrincipal, DirectTokenId, TimestampMillis, WorkspaceId};
use heterarchy_orchestration_auth::{
    McpTokenCatalog, McpTokenIssueRequest, McpTokenLabel, McpTokenRuntime, McpTokenService,
    McpTokenWorker,
};
use heterarchy_orchestration_operations::{
    AgentRequestEffectScope, AgentRequestIndependence, AppendWorkspaceMessageCommand,
    BrokerClusterConfig, ChatGptProjectId, EnqueueProviderMessageCommand, MessageChannel,
    MessageContext, OperationalActor, OperationsCatalog, ProviderMessageTarget,
    WorkspaceParticipationId, WorkspaceProjectConfigureCommand, WorkspaceProjectScope,
};
use heterarchy_orchestration_runtime::{
    OrchestrationRuntime, access::OperatorPolicy, provisioning::AgentRequestSubmit,
};
use heterarchy_orchestration_sessions::AgentProfileProviderFamily;
use hyper::header::HeaderValue;
use std::{
    error::Error,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

type TestResult<T = ()> = Result<T, Box<dyn Error>>;
const TEST_PROJECT_ID: &str = "g-p-6ad231f7ad7c8191bfc1d5ff782c171f";
const STAGED_SNAPSHOT_ENVIRONMENT: &str = "HETERARCHY_ORCHESTRATION_STAGED_SNAPSHOT_DIR";

#[path = "../../../heterarchy-orchestration-runtime/tests/support/broker_cluster.rs"]
mod broker_cluster;

struct Fixture {
    root: PathBuf,
    authority: McpTokenRuntime,
    operator: AuthenticatedPrincipal,
    operator_header: HeaderValue,
    foreign_header: HeaderValue,
    operation_id: Uuid,
    operation_revision: u64,
    workspace_id: Uuid,
}

impl Fixture {
    fn open(broker_cluster: BrokerClusterConfig) -> TestResult<(Self, Arc<OrchestrationRuntime>)> {
        let root = std::env::temp_dir().join(format!("worker-http-readback-{}", Uuid::now_v7()));
        std::fs::create_dir_all(&root)?;
        let catalog = McpTokenCatalog::open(root.join("tokens.sqlite3"))?;
        let (client, worker) = McpTokenWorker::start(McpTokenService::new([0x63; 32], catalog))?;
        let token = client.issue(McpTokenIssueRequest::new(
            McpTokenLabel::new("operator".to_owned())?,
            TimestampMillis::new(1)?,
        ))?;
        let operator = AuthenticatedPrincipal::DirectToken(DirectTokenId::from_uuid(
            token.metadata().id().into_uuid(),
        ));
        let operator_header =
            HeaderValue::from_str(&format!("Bearer {}", token.reveal().as_str()))?;
        let foreign = client.issue(McpTokenIssueRequest::new(
            McpTokenLabel::new("ordinary".to_owned())?,
            TimestampMillis::new(1)?,
        ))?;
        let foreign_header =
            HeaderValue::from_str(&format!("Bearer {}", foreign.reveal().as_str()))?;
        let authority = McpTokenRuntime::from_parts(client, worker);
        let runtime = OrchestrationRuntime::open_with_broker_cluster(&root, broker_cluster)?
            .with_operator_policy(OperatorPolicy::new(vec![operator])?);
        let request_id = Uuid::now_v7();
        let workspace_id = Uuid::now_v7();
        runtime.configure_workspace_project(
            operator,
            WorkspaceProjectConfigureCommand::new(
                Uuid::now_v7(),
                WorkspaceProjectScope::new(
                    workspace_id,
                    Uuid::now_v7(),
                    1,
                    1,
                    "provider-worker-test-workspace".to_owned(),
                )?,
                None,
                ChatGptProjectId::try_from(TEST_PROJECT_ID)?,
                TimestampMillis::new(1)?,
            )?,
            &[],
        )?;
        runtime.submit_agent_request(AgentRequestSubmit {
            workspace_id,
            activity_id: Uuid::now_v7(),
            requester_agent_id: Uuid::now_v7(),
            requester_session_id: Uuid::now_v7(),
            coordinator_revision: 1,
            request_id,
            objective: "fixture provider readback".to_owned(),
            capability: "chat".to_owned(),
            count: 1,
            effect_scope: AgentRequestEffectScope::SourceWrite,
            independence: AgentRequestIndependence::None,
            reason: "read-only client proof".to_owned(),
            created_at_ms: 1,
        })?;
        runtime.reserve_agent_request_resolution(request_id, 2, 3)?;
        runtime.prepare_agent_request_effect_targets(request_id, 6, 7)?;
        let released = runtime
            .provider_worker_next_provision(operator, 10)?
            .ok_or("fixture Execute missing")?;
        let operation_id = released.operation().operation_id();
        let operation_revision = released.operation().revision();
        Ok((
            Self {
                root,
                authority,
                operator,
                operator_header,
                foreign_header,
                operation_id,
                operation_revision,
                workspace_id,
            },
            Arc::new(runtime),
        ))
    }

    fn seed_queued_message(
        &self,
        runtime: &OrchestrationRuntime,
    ) -> TestResult<(Uuid, ProviderMessageTarget)> {
        let confirmed = runtime.provider_worker_confirm_provision_with_project(
            self.operator,
            self.operation_id,
            self.operation_revision,
            "conversation-test",
            Some(TEST_PROJECT_ID),
            11,
        )?;
        let profile = confirmed.profile_id().ok_or("profile binding")?;
        let runtime_id = confirmed.runtime_session_id().ok_or("runtime binding")?;
        let participation = WorkspaceParticipationId::from_uuid(Uuid::now_v7())?;
        let workspace = WorkspaceId::from_uuid(self.workspace_id);
        let seed = rusqlite::Connection::open(self.root.join("agent-operations.sqlite3"))?;
        seed.execute(
            "INSERT INTO ops_imports(migration_id,workspace_id,destination_id,source_revision,phase,next_cursor,page_count,record_count,import_digest,verified_pages,verify_digest)
             VALUES(?1,?2,?3,1,2,NULL,5,0,?4,5,?4)",
            rusqlite::params![Uuid::now_v7().as_bytes(), workspace.into_uuid().as_bytes(), runtime.destination_id().as_bytes(), [0_u8; 32].as_slice()],
        )?;
        seed.execute(
            "INSERT INTO ops_workspace_participations(workspace_id,agent_profile_id,participation_id,created_at_ms,revision) VALUES(?1,?2,?3,0,1)",
            rusqlite::params![workspace.into_uuid().as_bytes(), profile.into_uuid().as_bytes(), participation.into_uuid().as_bytes()],
        )?;
        drop(seed);
        let target = ProviderMessageTarget::new_with_project_route(
            workspace,
            profile,
            participation,
            AgentProfileProviderFamily::ChatGpt,
            runtime_id,
            "conversation-test".to_owned(),
            confirmed.project(),
        )?;
        let message_id = Uuid::now_v7();
        {
            let mut catalog = OperationsCatalog::open(
                &self.root.join("agent-operations.sqlite3"),
                runtime.destination_id(),
            )?;
            catalog.append_workspace_message(AppendWorkspaceMessageCommand {
                message_id,
                workspace_id: workspace,
                channel: MessageChannel::Workspace,
                actor: OperationalActor::LocalOperator,
                body: "queued message prompt".to_owned(),
                submitted_at: TimestampMillis::new(12)?,
                context: MessageContext::default(),
            })?;
            catalog.enqueue_provider_message(EnqueueProviderMessageCommand {
                message_id,
                target: target.clone(),
                queued_at: TimestampMillis::new(13)?,
                node_route_witness: None,
            })?;
        }
        Ok((message_id, target))
    }
}

async fn isolated_broker_cluster() -> TestResult<(broker_cluster::Cluster, BrokerClusterConfig)> {
    let cluster = broker_cluster::Cluster::start().await?;
    let config = cluster.orchestration_config()?;
    Ok((cluster, config))
}

async fn start(
    fixture: &Fixture,
    runtime: Arc<OrchestrationRuntime>,
) -> TestResult<(
    OrchestrationHttpClient,
    CancellationToken,
    tokio::task::JoinHandle<Result<(), crate::BackendError>>,
)> {
    let reservation = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address: SocketAddr = reservation.local_addr()?;
    drop(reservation);
    let cancellation = CancellationToken::new();
    let server = tokio::spawn(crate::serve_with_runtime(
        runtime,
        address,
        fixture.authority.client(),
        None,
        cancellation.clone(),
    ));
    for _ in 0..100 {
        if tokio::net::TcpStream::connect(address).await.is_ok() {
            return Ok((
                OrchestrationHttpClient::new(address, Duration::from_secs(2))?,
                cancellation,
                server,
            ));
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    cancellation.cancel();
    let _ = server.await;
    Err("worker HTTP listener never became ready".into())
}

async fn assert_readback(fixture: &Fixture, client: &OrchestrationHttpClient) -> TestResult {
    let query = ProviderProvisionInspection {
        operation_id: fixture.operation_id,
    };
    let first = client
        .provider_provision_query(&fixture.operator_header, &query)
        .await?
        .operation
        .ok_or("exact operation readback absent")?;
    assert_eq!(first.operation_id, fixture.operation_id);
    assert_eq!(first.state, "effect_pending");
    let selection = client
        .provider_provision_in_flight(&fixture.operator_header)
        .await?;
    assert!(!selection.retry_authorized && !selection.request_equivalence_proven);
    assert_eq!(selection.operation, Some(first.clone()));
    let again = client
        .provider_provision_query(&fixture.operator_header, &query)
        .await?;
    assert_eq!(again.operation, Some(first));
    let messages = client
        .provider_messages_in_flight(
            &fixture.operator_header,
            &ProviderMessageSelectionQuery {
                after_message_id: None,
                limit: 32,
            },
        )
        .await?;
    assert!(messages.messages.is_empty());
    assert!(!messages.retry_authorized && !messages.request_equivalence_proven);
    assert!(matches!(
        client
            .provider_provision_query(&fixture.foreign_header, &query)
            .await,
        Err(ClientError::Rejected {
            status: 403,
            code: ServiceCode::Forbidden
        })
    ));
    Ok(())
}

#[tokio::test]
async fn official_worker_readback_is_pure_authorized_and_survives_restart() -> TestResult {
    let (_broker_cluster, broker_config) = isolated_broker_cluster().await?;
    let (fixture, runtime) = Fixture::open(broker_config)?;
    let (client, cancellation, server) = start(&fixture, Arc::clone(&runtime)).await?;
    assert_readback(&fixture, &client).await?;
    cancellation.cancel();
    tokio::time::timeout(Duration::from_secs(3), server).await???;
    drop(client);
    drop(runtime);
    let reopened = Arc::new(
        OrchestrationRuntime::open_with_broker_cluster(&fixture.root, broker_config)?
            .with_operator_policy(OperatorPolicy::new(vec![fixture.operator])?),
    );
    let (client, cancellation, server) = start(&fixture, reopened).await?;
    assert_readback(&fixture, &client).await?;
    cancellation.cancel();
    tokio::time::timeout(Duration::from_secs(3), server).await???;
    drop(client);
    let Fixture {
        root, authority, ..
    } = fixture;
    drop(authority);
    std::fs::remove_dir_all(root)?;
    Ok(())
}

#[tokio::test]
async fn official_worker_cancel_http_contract() -> TestResult {
    let (_broker_cluster, broker_config) = isolated_broker_cluster().await?;
    let (fixture, runtime) = Fixture::open(broker_config)?;
    let (message_id, _target) = fixture.seed_queued_message(&runtime)?;
    let (client, cancellation, server) = start(&fixture, Arc::clone(&runtime)).await?;

    assert_cancel_rejections(
        &client,
        &fixture.foreign_header,
        &fixture.operator_header,
        message_id,
    )
    .await?;
    assert_cancel_success_and_readback(&client, &fixture.operator_header, message_id).await?;

    cancellation.cancel();
    tokio::time::timeout(Duration::from_secs(3), server).await???;
    drop(client);
    drop(runtime);

    let Fixture {
        root, authority, ..
    } = fixture;
    drop(authority);
    std::fs::remove_dir_all(root)?;
    Ok(())
}

#[tokio::test]
async fn official_worker_unknown_isolation_http_is_operator_only_and_recoverable_after_delivery()
-> TestResult {
    let (_broker_cluster, broker_config) = isolated_broker_cluster().await?;
    let (fixture, runtime) = Fixture::open(broker_config)?;
    let (message_id, target) = fixture.seed_queued_message(&runtime)?;
    let mut catalog = OperationsCatalog::open(
        &fixture.root.join("agent-operations.sqlite3"),
        runtime.destination_id(),
    )?;
    let execute = catalog
        .provider_message_dispatch(message_id, TimestampMillis::new(14)?)?
        .ok_or("provider Execute missing")?;
    assert_eq!(execute.outbox().message_id(), message_id);
    let unknown = catalog.mark_provider_message_effect_unknown(
        message_id,
        execute.outbox().revision(),
        &target,
        TimestampMillis::new(15)?,
    )?;
    drop(catalog);
    let (client, cancellation, server) = start(&fixture, Arc::clone(&runtime)).await?;
    let (request, receipt) = assert_unknown_isolation_http_contract(
        &fixture,
        &client,
        message_id,
        &target,
        unknown.revision(),
    )
    .await?;

    let delivery = ProviderDeliveryConfirmation {
        message_id,
        expected_revision: unknown.revision(),
        provider_conversation_id: target.provider_conversation_id().to_owned(),
        provider_turn_id: "turn-after-isolation".to_owned(),
        body: "queued message prompt".to_owned(),
        provider_project_id: Some(TEST_PROJECT_ID.to_owned()),
    };
    let delivered = client
        .provider_message_delivered(&fixture.operator_header, &delivery)
        .await?;
    assert_eq!(delivered.message.state, ChatProviderStateWire::Delivered);

    let after_delivery = client
        .provider_message_unknown_isolation(&fixture.operator_header, &request)
        .await?;
    assert_eq!(after_delivery.receipt, Some(receipt));
    assert!(!after_delivery.retry_authorized);

    cancellation.cancel();
    tokio::time::timeout(Duration::from_secs(3), server).await???;
    drop(client);
    drop(runtime);
    let Fixture {
        root, authority, ..
    } = fixture;
    drop(authority);
    std::fs::remove_dir_all(root)?;
    Ok(())
}

async fn assert_unknown_isolation_http_contract(
    fixture: &Fixture,
    client: &OrchestrationHttpClient,
    message_id: Uuid,
    target: &ProviderMessageTarget,
    expected_revision: u64,
) -> TestResult<(
    ProviderMessageUnknownIsolationRequest,
    crate::wire::provider_worker::ProviderMessageUnknownIsolationReceiptWire,
)> {
    let request = ProviderMessageUnknownIsolationRequest {
        request_id: Uuid::from_u128(0xAB01),
        message_id,
        expected_revision,
        expected_target: ProviderMessageTargetWire::from(target),
        reason: ProviderMessageUnknownIsolationReasonWire::OperatorAcknowledgedUnknown,
    };
    let unauthorized = client
        .provider_message_unknown_isolation(&fixture.foreign_header, &request)
        .await;
    assert!(matches!(
        unauthorized,
        Err(ClientError::Rejected {
            status: 403,
            code: ServiceCode::Forbidden
        })
    ));

    let response = client
        .provider_message_unknown_isolation(&fixture.operator_header, &request)
        .await?;
    assert!(!response.retry_authorized);
    let receipt = response.receipt.ok_or("isolation receipt missing")?;
    assert_eq!(receipt.request_id, request.request_id);
    assert_eq!(receipt.message_id, message_id);
    assert_eq!(receipt.expected_revision, expected_revision);
    assert_eq!(receipt.expected_target, request.expected_target);
    assert_eq!(
        receipt.reason,
        ProviderMessageUnknownIsolationReasonWire::OperatorAcknowledgedUnknown
    );
    assert_eq!(
        receipt.principal.kind,
        crate::wire::provider_worker::ProviderMessageIsolationPrincipalKindWire::DirectToken
    );
    assert_eq!(
        receipt.principal.id,
        fixture
            .operator
            .direct_token_id()
            .map(DirectTokenId::into_uuid)
    );
    assert!(receipt.acknowledged_at_ms >= 0);

    let replay = client
        .provider_message_unknown_isolation(&fixture.operator_header, &request)
        .await?;
    assert_eq!(replay.receipt, Some(receipt.clone()));
    assert!(!replay.retry_authorized);

    let query_by_message = ProviderMessageUnknownIsolationQuery {
        message_id,
        request_id: None,
    };
    let serialized_query = serde_json::to_value(&query_by_message)?;
    assert_eq!(serialized_query["requestId"], serde_json::Value::Null);
    let query = client
        .provider_message_unknown_isolation_query(&fixture.operator_header, &query_by_message)
        .await?;
    assert_eq!(query.receipt, Some(receipt.clone()));
    assert!(!query.retry_authorized);
    assert!(
        serde_json::from_value::<ProviderMessageUnknownIsolationQuery>(
            serde_json::json!({ "messageId": message_id })
        )
        .is_err()
    );

    let query_by_request = ProviderMessageUnknownIsolationQuery {
        message_id,
        request_id: Some(request.request_id),
    };
    let recovered = client
        .provider_message_unknown_isolation_query(&fixture.operator_header, &query_by_request)
        .await?;
    assert_eq!(recovered.receipt, Some(receipt.clone()));
    assert!(!recovered.retry_authorized);
    assert!(matches!(
        client
            .provider_message_unknown_isolation_query(&fixture.foreign_header, &query_by_message)
            .await,
        Err(ClientError::Rejected {
            status: 403,
            code: ServiceCode::Forbidden
        })
    ));

    let isolated = client
        .provider_messages_in_flight(
            &fixture.operator_header,
            &ProviderMessageSelectionQuery {
                after_message_id: None,
                limit: 32,
            },
        )
        .await?;
    assert!(
        !isolated
            .messages
            .iter()
            .any(|message| message.message_id == message_id)
    );
    Ok((request, receipt))
}

async fn assert_cancel_rejections(
    client: &OrchestrationHttpClient,
    foreign_header: &HeaderValue,
    operator_header: &HeaderValue,
    message_id: Uuid,
) -> TestResult {
    let valid_fence = ProviderMessageFence {
        message_id,
        expected_revision: 1,
    };

    // 1. Non-Operator rejection (403 Forbidden)
    let non_op_err = client
        .provider_message_cancel(foreign_header, &valid_fence)
        .await;
    assert!(
        matches!(
            non_op_err,
            Err(ClientError::Rejected {
                status: 403,
                code: ServiceCode::Forbidden
            })
        ),
        "expected 403 Forbidden for non-operator, got {non_op_err:?}"
    );

    // 2. Client-side invalid message ID (Nil) rejection (InvalidRequest)
    let nil_fence = ProviderMessageFence {
        message_id: Uuid::nil(),
        expected_revision: 1,
    };
    let nil_err = client
        .provider_message_cancel(operator_header, &nil_fence)
        .await;
    assert!(
        matches!(nil_err, Err(ClientError::InvalidRequest)),
        "expected InvalidRequest for nil message ID, got {nil_err:?}"
    );

    // 3. Unknown message ID rejection (404 NotFound)
    let unknown_fence = ProviderMessageFence {
        message_id: Uuid::now_v7(),
        expected_revision: 1,
    };
    let unknown_err = client
        .provider_message_cancel(operator_header, &unknown_fence)
        .await;
    assert!(
        matches!(
            unknown_err,
            Err(ClientError::Rejected {
                status: 404,
                code: ServiceCode::NotFound
            })
        ),
        "expected 404 NotFound for unknown message ID, got {unknown_err:?}"
    );

    // 4. Stale/Wrong Revision rejection (409 Conflict)
    let wrong_rev_fence = ProviderMessageFence {
        message_id,
        expected_revision: 999,
    };
    let conflict_err = client
        .provider_message_cancel(operator_header, &wrong_rev_fence)
        .await;
    assert!(
        matches!(
            conflict_err,
            Err(ClientError::Rejected {
                status: 409,
                code: ServiceCode::Conflict
            })
        ),
        "expected 409 Conflict for wrong revision, got {conflict_err:?}"
    );

    Ok(())
}

async fn assert_cancel_success_and_readback(
    client: &OrchestrationHttpClient,
    operator_header: &HeaderValue,
    message_id: Uuid,
) -> TestResult {
    let valid_fence = ProviderMessageFence {
        message_id,
        expected_revision: 1,
    };
    // 5. Operator allowed: successful cancellation
    let cancel_res = client
        .provider_message_cancel(operator_header, &valid_fence)
        .await?;
    assert_eq!(cancel_res.message.message_id, message_id);
    assert_eq!(cancel_res.message.state, ChatProviderStateWire::Failed);
    assert_eq!(cancel_res.message.revision, 2);
    assert_eq!(cancel_res.message.last_dispatch_at_ms, 0);

    // 6. Idempotent re-cancel: second call succeeds without advancing revision
    let replay_fence = ProviderMessageFence {
        message_id,
        expected_revision: 2,
    };
    let replay_res = client
        .provider_message_cancel(operator_header, &replay_fence)
        .await?;
    assert_eq!(replay_res.message.message_id, message_id);
    assert_eq!(replay_res.message.state, ChatProviderStateWire::Failed);
    assert_eq!(replay_res.message.revision, 2);

    // 7. Query readback confirms state=Failed and action=None
    let query_res = client
        .provider_message_query(operator_header, &ProviderMessageInspection { message_id })
        .await?;
    assert_eq!(query_res.message.state, ChatProviderStateWire::Failed);
    assert!(query_res.action.is_none());

    // 8. Canceled message is NOT selected in-flight or dispatched
    let in_flight = client
        .provider_messages_in_flight(
            operator_header,
            &ProviderMessageSelectionQuery {
                after_message_id: None,
                limit: 32,
            },
        )
        .await?;
    assert!(
        !in_flight
            .messages
            .iter()
            .any(|m| m.message_id == message_id),
        "canceled message should not appear in in-flight selection"
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires explicit offline snapshot"]
async fn staged_production_snapshot_cancel_dry_run() -> TestResult {
    let backup_dir = std::env::var_os(STAGED_SNAPSHOT_ENVIRONMENT)
        .map(PathBuf::from)
        .ok_or_else(|| {
            format!(
                "{STAGED_SNAPSHOT_ENVIRONMENT} must name the approved offline snapshot directory"
            )
        })?;
    if !backup_dir.is_absolute() || !backup_dir.is_dir() {
        return Err(format!(
            "{STAGED_SNAPSHOT_ENVIRONMENT} must name an existing absolute directory: {}",
            backup_dir.display()
        )
        .into());
    }
    for file in [
        "agent-operations.sqlite3",
        "agent-profiles.sqlite3",
        "agent-sessions.sqlite3",
    ] {
        let path = backup_dir.join(file);
        if !path.is_file() {
            return Err(format!(
                "{STAGED_SNAPSHOT_ENVIRONMENT} is missing required file: {}",
                path.display()
            )
            .into());
        }
    }

    let scratch = std::env::temp_dir().join(format!("staged-cancel-{}", Uuid::now_v7()));
    create_private_scratch_root(&scratch)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            std::fs::metadata(&scratch)?.permissions().mode() & 0o777,
            0o700
        );
    }
    stage_production_snapshot(&backup_dir, &scratch)?;
    let (_broker_cluster, broker_config) = isolated_broker_cluster().await?;

    let StagedWorkerFixture {
        authority,
        runtime,
        client,
        cancellation,
        server,
        operator_header,
        foreign_header,
    } = start_staged_worker_fixture(&scratch, broker_config).await?;

    let target_message_id = Uuid::parse_str("f1a2b3c4-d5e6-47a8-9b0c-1d2e3f4a5b6c")?;
    assert_staged_cancel_rejections(
        &client,
        &foreign_header,
        &operator_header,
        target_message_id,
    )
    .await?;
    assert_staged_cancel_success_and_readback(&client, &operator_header, target_message_id).await?;

    cancellation.cancel();
    tokio::time::timeout(Duration::from_secs(3), server).await???;
    drop(client);
    drop(runtime);
    drop(authority);
    std::fs::remove_dir_all(scratch)?;
    Ok(())
}

fn create_private_scratch_root(path: &Path) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        let mut builder = std::fs::DirBuilder::new();
        builder.mode(0o700).create(path)
    }
    #[cfg(not(unix))]
    {
        std::fs::create_dir(path)
    }
}

fn stage_production_snapshot(backup_dir: &Path, scratch: &Path) -> TestResult {
    for file in &[
        "agent-operations.sqlite3",
        "agent-profiles.sqlite3",
        "agent-sessions.sqlite3",
    ] {
        std::fs::copy(backup_dir.join(file), scratch.join(file))?;
    }
    let conn = rusqlite::Connection::open(scratch.join("agent-operations.sqlite3"))?;
    let destination_bytes: Vec<u8> = conn.query_row(
        "SELECT destination_id FROM ops_catalog_identity",
        [],
        |row| row.get(0),
    )?;
    let destination = Uuid::from_slice(&destination_bytes)?;
    std::fs::write(
        scratch.join("agent-operations-destination"),
        destination.as_bytes(),
    )?;
    let mut foreign_keys = conn.prepare("PRAGMA foreign_key_check")?;
    let mut violations = foreign_keys.query([])?;
    assert!(
        violations.next()?.is_none(),
        "copied Operations snapshot has foreign-key violations before runtime open"
    );
    Ok(())
}

struct StagedWorkerFixture {
    authority: McpTokenRuntime,
    runtime: Arc<OrchestrationRuntime>,
    client: OrchestrationHttpClient,
    cancellation: CancellationToken,
    server: tokio::task::JoinHandle<Result<(), crate::BackendError>>,
    operator_header: HeaderValue,
    foreign_header: HeaderValue,
}

async fn start_staged_worker_fixture(
    scratch: &Path,
    broker_cluster: BrokerClusterConfig,
) -> TestResult<StagedWorkerFixture> {
    let catalog = McpTokenCatalog::open(scratch.join("tokens.sqlite3"))?;
    let (token_client, worker) = McpTokenWorker::start(McpTokenService::new([0x63; 32], catalog))?;
    let token = token_client.issue(McpTokenIssueRequest::new(
        McpTokenLabel::new("operator".to_owned())?,
        TimestampMillis::new(1)?,
    ))?;
    let issued_operator = AuthenticatedPrincipal::DirectToken(DirectTokenId::from_uuid(
        token.metadata().id().into_uuid(),
    ));
    let operator_header = HeaderValue::from_str(&format!("Bearer {}", token.reveal().as_str()))?;
    let foreign = token_client.issue(McpTokenIssueRequest::new(
        McpTokenLabel::new("foreign".to_owned())?,
        TimestampMillis::new(1)?,
    ))?;
    let foreign_header = HeaderValue::from_str(&format!("Bearer {}", foreign.reveal().as_str()))?;
    let authority = McpTokenRuntime::from_parts(token_client, worker);

    let runtime = Arc::new(
        OrchestrationRuntime::open_with_broker_cluster(scratch, broker_cluster)?
            .with_operator_policy(OperatorPolicy::new(vec![issued_operator])?),
    );

    let reservation = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = reservation.local_addr()?;
    drop(reservation);
    let cancellation = CancellationToken::new();
    let server = tokio::spawn(crate::serve_with_runtime(
        Arc::clone(&runtime),
        address,
        authority.client(),
        None,
        cancellation.clone(),
    ));

    let mut client = None;
    for _ in 0..100 {
        if tokio::net::TcpStream::connect(address).await.is_ok() {
            client = Some(OrchestrationHttpClient::new(
                address,
                Duration::from_secs(2),
            )?);
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let client = client.ok_or("staging listener failed to start")?;
    Ok(StagedWorkerFixture {
        authority,
        runtime,
        client,
        cancellation,
        server,
        operator_header,
        foreign_header,
    })
}

async fn assert_staged_cancel_rejections(
    client: &OrchestrationHttpClient,
    foreign_header: &HeaderValue,
    operator_header: &HeaderValue,
    target_message_id: Uuid,
) -> TestResult {
    // 1. Foreign token rejected with 403 Forbidden
    let non_op = client
        .provider_message_cancel(
            foreign_header,
            &ProviderMessageFence {
                message_id: target_message_id,
                expected_revision: 1,
            },
        )
        .await;
    assert!(
        matches!(
            non_op,
            Err(ClientError::Rejected {
                status: 403,
                code: ServiceCode::Forbidden
            })
        ),
        "expected 403 for non-operator, got {non_op:?}"
    );

    // 2. Query initial state on deleted XRP workspace message
    let query_before = client
        .provider_message_query(
            operator_header,
            &ProviderMessageInspection {
                message_id: target_message_id,
            },
        )
        .await?;
    assert_eq!(query_before.message.state, ChatProviderStateWire::Queued);
    assert_eq!(query_before.message.revision, 1);
    assert_eq!(query_before.message.last_dispatch_at_ms, 0);

    // 3. Stale revision rejected with 409 Conflict
    let stale_fence = ProviderMessageFence {
        message_id: target_message_id,
        expected_revision: 99,
    };
    let stale_cancel = client
        .provider_message_cancel(operator_header, &stale_fence)
        .await;
    assert!(
        matches!(
            stale_cancel,
            Err(ClientError::Rejected {
                status: 409,
                code: ServiceCode::Conflict
            })
        ),
        "expected 409 for stale revision, got {stale_cancel:?}"
    );
    Ok(())
}

async fn assert_staged_cancel_success_and_readback(
    client: &OrchestrationHttpClient,
    operator_header: &HeaderValue,
    target_message_id: Uuid,
) -> TestResult {
    // 4. Operator cancel succeeds against deleted XRP workspace
    let cancel_res = client
        .provider_message_cancel(
            operator_header,
            &ProviderMessageFence {
                message_id: target_message_id,
                expected_revision: 1,
            },
        )
        .await?;
    assert_eq!(cancel_res.message.message_id, target_message_id);
    assert_eq!(cancel_res.message.state, ChatProviderStateWire::Failed);
    assert_eq!(cancel_res.message.revision, 2);
    assert_eq!(cancel_res.message.last_dispatch_at_ms, 0);

    // 5. Idempotent re-cancel succeeds without advancing revision
    let replay_res = client
        .provider_message_cancel(
            operator_header,
            &ProviderMessageFence {
                message_id: target_message_id,
                expected_revision: 2,
            },
        )
        .await?;
    assert_eq!(replay_res.message.message_id, target_message_id);
    assert_eq!(replay_res.message.state, ChatProviderStateWire::Failed);
    assert_eq!(replay_res.message.revision, 2);

    // 6. Readback query confirms state=Failed and action=None
    let query_after = client
        .provider_message_query(
            operator_header,
            &ProviderMessageInspection {
                message_id: target_message_id,
            },
        )
        .await?;
    assert_eq!(query_after.message.state, ChatProviderStateWire::Failed);
    assert_eq!(query_after.message.revision, 2);
    assert_eq!(query_after.message.last_dispatch_at_ms, 0);
    assert!(query_after.action.is_none());

    // 7. Message is completely excluded from in-flight selection
    let in_flight = client
        .provider_messages_in_flight(
            operator_header,
            &ProviderMessageSelectionQuery {
                after_message_id: None,
                limit: 32,
            },
        )
        .await?;
    assert!(
        !in_flight
            .messages
            .iter()
            .any(|m| m.message_id == target_message_id),
        "target message should not appear in in-flight selection after cancel"
    );
    Ok(())
}
