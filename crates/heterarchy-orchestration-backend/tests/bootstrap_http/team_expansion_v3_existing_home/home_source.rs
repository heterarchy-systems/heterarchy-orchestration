//! Real Forge HTTP Source dispatch after an exact existing-Home preparation.

use std::{
    error::Error,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use forge_backend::{HomeTransferSourceRecord, HomeTransferSourceState};
use forge_domain::AgentSessionId;
use heterarchy_orchestration_backend::{
    client::{ClientError, OrchestrationHttpClient, ReadbackTarget},
    wire::{AgentSessionCloseRequest, team_permanent_transfer as transfer_wire},
};
use hyper::StatusCode;
use hyper::header::HeaderValue;
use sha2::{Digest, Sha256};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::oneshot,
    task::JoinHandle,
};
use uuid::Uuid;

use super::super::{TestResult, temporary_dispatch_lifecycle};
use super::{ExistingHomeV3FlowContext, preparation::PreparationBaseline};

const MAX_SOURCE_HTTP_RESPONSE_BYTES: usize = 1024 * 1024 + 8 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SourceHttpCallCounts {
    pub(crate) get_requests: usize,
    pub(crate) prepare_requests: usize,
}

#[derive(Default)]
struct SourceHttpCounters {
    get_requests: AtomicUsize,
    prepare_requests: AtomicUsize,
    drop_first_prepare_reply: AtomicBool,
}

/// Local byte-preserving probe around the real Forge HTTP source listener.
/// The first Prepare commits at Forge but its reply is dropped, making Source
/// Get the only recovery path. The probe retains counts, never request bodies.
pub(crate) struct SourceHttpProxy {
    address: SocketAddr,
    counters: Arc<SourceHttpCounters>,
    stop: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<Result<(), String>>>,
}

impl SourceHttpProxy {
    pub(crate) async fn start(upstream: SocketAddr) -> Result<Self, Box<dyn Error>> {
        let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).await?;
        let address = listener.local_addr()?;
        let counters = Arc::new(SourceHttpCounters {
            drop_first_prepare_reply: AtomicBool::new(true),
            ..SourceHttpCounters::default()
        });
        let task_counters = Arc::clone(&counters);
        let (stop, mut stopped) = oneshot::channel();
        let task = tokio::spawn(async move {
            loop {
                let (downstream, _) = tokio::select! {
                    _ = &mut stopped => return Ok(()),
                    accepted = listener.accept() => accepted.map_err(|error| error.to_string())?,
                };
                tokio::time::timeout(
                    Duration::from_secs(5),
                    forward_source_request(downstream, upstream, &task_counters),
                )
                .await
                .map_err(|_| "Home Source probe request exceeded its bound".to_owned())??;
            }
        });
        Ok(Self {
            address,
            counters,
            stop: Some(stop),
            task: Some(task),
        })
    }

    pub(crate) const fn address(&self) -> SocketAddr {
        self.address
    }

    pub(crate) async fn finish(mut self) -> Result<SourceHttpCallCounts, Box<dyn Error>> {
        if let Some(stop) = self.stop.take() {
            let _ignored = stop.send(());
        }
        if let Some(task) = self.task.take() {
            tokio::time::timeout(Duration::from_secs(5), task)
                .await
                .map_err(|_| std::io::Error::other("Home Source HTTP probe did not stop"))?
                .map_err(|_| std::io::Error::other("Home Source HTTP probe task failed"))?
                .map_err(|_| std::io::Error::other("Home Source HTTP probe failed"))?;
        }
        Ok(SourceHttpCallCounts {
            get_requests: self.counters.get_requests.load(Ordering::Acquire),
            prepare_requests: self.counters.prepare_requests.load(Ordering::Acquire),
        })
    }
}

async fn forward_source_request(
    mut downstream: TcpStream,
    upstream_address: SocketAddr,
    counters: &SourceHttpCounters,
) -> Result<(), String> {
    let request = temporary_dispatch_lifecycle::read_http_request(&mut downstream).await?;
    let (method, path, _) = temporary_dispatch_lifecycle::http_request_parts(&request)?;
    if method != "POST" {
        return Err("Home Source probe received an unexpected method".to_owned());
    }
    let drop_reply = if path.ends_with("/home_transfer.source_prepare") {
        counters.prepare_requests.fetch_add(1, Ordering::AcqRel);
        counters
            .drop_first_prepare_reply
            .swap(false, Ordering::AcqRel)
    } else if path.ends_with("/home_transfer.source_get") {
        counters.get_requests.fetch_add(1, Ordering::AcqRel);
        false
    } else {
        return Err("Home Source probe received an unexpected command".to_owned());
    };

    let mut upstream = TcpStream::connect(upstream_address)
        .await
        .map_err(|error| error.to_string())?;
    upstream
        .write_all(&request)
        .await
        .map_err(|error| error.to_string())?;
    if drop_reply {
        let mut response = Vec::new();
        let mut bounded = upstream.take((MAX_SOURCE_HTTP_RESPONSE_BYTES + 1) as u64);
        tokio::time::timeout(Duration::from_secs(5), bounded.read_to_end(&mut response))
            .await
            .map_err(|_| "Forge Source Prepare reply exceeded its bound".to_owned())?
            .map_err(|error| error.to_string())?;
        if response.len() > MAX_SOURCE_HTTP_RESPONSE_BYTES {
            return Err("Forge Source Prepare reply exceeded the bounded response size".to_owned());
        }
        let status = std::str::from_utf8(&response)
            .ok()
            .and_then(|response| response.lines().next())
            .unwrap_or_default();
        if !status.contains(" 200 ") {
            return Err("Forge did not confirm the Source Prepare before reply loss".to_owned());
        }
        drop(downstream);
        return Ok(());
    }

    let mut bounded = upstream.take((MAX_SOURCE_HTTP_RESPONSE_BYTES + 1) as u64);
    let copied = tokio::time::timeout(
        Duration::from_secs(5),
        tokio::io::copy(&mut bounded, &mut downstream),
    )
    .await
    .map_err(|_| "Forge Source reply forwarding exceeded its bound".to_owned())?
    .map_err(|error| error.to_string())?;
    if copied > MAX_SOURCE_HTTP_RESPONSE_BYTES as u64 {
        return Err("Forge Source reply exceeded the bounded response size".to_owned());
    }
    Ok(())
}

pub(super) async fn assert_source_dispatch_after_preparation(
    context: &ExistingHomeV3FlowContext<'_>,
    preparation: &transfer_wire::TeamPermanentTransferPreparationRequest,
    preparation_receipt: &transfer_wire::TeamPermanentTransferPreparationReceiptWire,
    baseline: &PreparationBaseline<'_>,
) -> TestResult {
    let request = source_dispatch_request(preparation, preparation_receipt);
    close_source_target_session(context).await?;
    let committed = dispatch_with_dropped_reply(context, &request).await?;
    assert_eq!(
        committed.disposition,
        transfer_wire::TeamPermanentTransferSourceDispatchDispositionWire::Created
    );
    assert_eq!(committed.request, request);
    assert!(
        committed.source.is_none(),
        "lost Forge Prepare reply must remain unconfirmed until exact Source Get"
    );
    assert_dispatch_scope(context, &request, &committed.dispatch);
    let source = recover_exact_source(context, &request, &committed.dispatch).await?;
    assert_replay_and_identity_conflict(context, &request, &committed.dispatch, &source).await?;
    assert_generation_remains_preparing(context, &request).await?;
    super::assert_rosters_and_population_unchanged(
        context,
        baseline.home,
        baseline.destination,
        baseline.coordinates,
        baseline.population,
        "Home Source dispatch and exact recovery",
    )
    .await
}

fn source_dispatch_request(
    preparation: &transfer_wire::TeamPermanentTransferPreparationRequest,
    preparation_receipt: &transfer_wire::TeamPermanentTransferPreparationReceiptWire,
) -> transfer_wire::TeamPermanentTransferSourceDispatchRequest {
    transfer_wire::TeamPermanentTransferSourceDispatchRequest {
        actor: preparation.actor.clone(),
        command: transfer_wire::TeamPermanentTransferSourceDispatchActionWire {
            request_id: Uuid::now_v7(),
            preparation_id: preparation.command.preparation_id,
            transfer_id: preparation.command.transfer_id,
            expected_generation: preparation_receipt.generation,
            expected_workspace_leadership_revision: preparation
                .command
                .expected_workspace_leadership_revision,
        },
    }
}

async fn close_source_target_session(context: &ExistingHomeV3FlowContext<'_>) -> TestResult {
    let closed = context
        .client
        .close_session(
            context.bearer,
            context.home_member.session_id,
            &AgentSessionCloseRequest {
                expected_revision: context.home_member.revision,
            },
        )
        .await?;
    assert_eq!(closed.session_id, context.home_member.session_id);
    assert_eq!(closed.revision, context.home_member.revision + 1);
    assert_eq!(closed.state, "closed");
    Ok(())
}

async fn recover_exact_source(
    context: &ExistingHomeV3FlowContext<'_>,
    request: &transfer_wire::TeamPermanentTransferSourceDispatchRequest,
    dispatch: &transfer_wire::TeamPermanentTransferSourceDispatchRecordWire,
) -> TestResult<transfer_wire::TeamPermanentTransferSourceSnapshotWire> {
    // The first read stays local and proves no Source result was inferred.
    let local_only = context
        .client
        .recover_team_permanent_transfer_source(context.bearer, request, &request.actor, false)
        .await?;
    assert_eq!(
        local_only.request.preparation_id,
        request.command.preparation_id
    );
    assert_eq!(local_only.dispatch.as_ref(), Some(dispatch));
    assert!(local_only.source.is_none());

    // Explicit reconciliation performs Source Get using that same identity.
    let recovered = context
        .client
        .recover_team_permanent_transfer_source(context.bearer, request, &request.actor, true)
        .await?;
    assert_eq!(recovered.dispatch, local_only.dispatch);
    let source = recovered
        .source
        .ok_or("exact Source Get did not recover the prepared receipt")?;
    assert_source_record(context, request, dispatch, &source)?;
    Ok(source)
}

async fn assert_replay_and_identity_conflict(
    context: &ExistingHomeV3FlowContext<'_>,
    request: &transfer_wire::TeamPermanentTransferSourceDispatchRequest,
    dispatch: &transfer_wire::TeamPermanentTransferSourceDispatchRecordWire,
    source: &transfer_wire::TeamPermanentTransferSourceSnapshotWire,
) -> TestResult {
    let replay = context
        .client
        .dispatch_team_permanent_transfer_source(context.bearer, request)
        .await?;
    assert_eq!(
        replay.disposition,
        transfer_wire::TeamPermanentTransferSourceDispatchDispositionWire::Existing
    );
    assert_eq!(&replay.dispatch, dispatch);
    assert_eq!(replay.source.as_ref(), Some(source));

    let mut mismatched_request = request.clone();
    mismatched_request.command.request_id = Uuid::now_v7();
    let mismatch = context
        .client
        .dispatch_team_permanent_transfer_source(context.bearer, &mismatched_request)
        .await;
    if !matches!(
        &mismatch,
        Err(ClientError::Rejected { status, .. })
            if *status == StatusCode::CONFLICT.as_u16()
    ) {
        return Err(
            format!("changed dispatch identity did not return HTTP 409: {mismatch:?}").into(),
        );
    }

    let readback = context
        .client
        .read_team_permanent_transfer_source(
            context.bearer,
            &transfer_wire::TeamPermanentTransferSourceReadbackRequest {
                workspace_id: request.actor.scope.workspace_id,
                preparation_id: request.command.preparation_id,
                current_actor: request.actor.clone(),
            },
        )
        .await?;
    assert_eq!(readback.dispatch.as_ref(), Some(dispatch));
    assert_eq!(readback.source.as_ref(), Some(source));
    Ok(())
}

async fn assert_generation_remains_preparing(
    context: &ExistingHomeV3FlowContext<'_>,
    request: &transfer_wire::TeamPermanentTransferSourceDispatchRequest,
) -> TestResult {
    let generation = context
        .client
        .read_team_permanent_transfer_generation(
            context.bearer,
            &transfer_wire::TeamPermanentTransferReadbackRequest {
                workspace_id: request.actor.scope.workspace_id,
                transfer_id: request.command.transfer_id,
                current_actor: request.actor.clone(),
            },
        )
        .await?
        .result
        .ok_or("prepared transfer generation disappeared after Source recovery")?;
    assert_eq!(
        generation.generation.state,
        transfer_wire::TeamPermanentTransferGenerationStateWire::Preparing,
        "Source custody is not a Home cutover"
    );
    Ok(())
}

fn assert_dispatch_scope(
    context: &ExistingHomeV3FlowContext<'_>,
    request: &transfer_wire::TeamPermanentTransferSourceDispatchRequest,
    dispatch: &transfer_wire::TeamPermanentTransferSourceDispatchRecordWire,
) {
    assert_eq!(dispatch.workspace_id, request.actor.scope.workspace_id);
    assert_eq!(dispatch.preparation_id, request.command.preparation_id);
    assert_eq!(dispatch.transfer_id, request.command.transfer_id);
    assert_eq!(dispatch.generation, request.command.expected_generation);
    assert_eq!(dispatch.request_id, request.command.request_id);
    assert_eq!(dispatch.issuer_id, context.source.issuer.into_uuid());
    assert_eq!(
        dispatch.backend_id,
        context.source.client.expected_backend_id().into_uuid()
    );
    assert_eq!(
        dispatch.service_destination_id,
        context.source.client.destination_id()
    );
    assert_eq!(
        dispatch.source_scope.workspace_id,
        request.actor.scope.workspace_id
    );
    assert_eq!(
        dispatch.source_scope.location_id,
        request.actor.scope.location_id
    );
    assert_eq!(
        dispatch.source_scope.location_revision,
        request.actor.scope.location_revision
    );
    assert_eq!(
        dispatch.source_scope.workspace_revision,
        request.actor.scope.workspace_revision
    );
    assert_eq!(dispatch.source_scope.locator, request.actor.scope.locator);
    assert_eq!(
        dispatch.expected_workspace_leadership_revision,
        request.command.expected_workspace_leadership_revision
    );
    assert!(dispatch.grant_expires_at_ms > dispatch.grant_issued_at_ms);
}

fn assert_source_record(
    context: &ExistingHomeV3FlowContext<'_>,
    request: &transfer_wire::TeamPermanentTransferSourceDispatchRequest,
    dispatch: &transfer_wire::TeamPermanentTransferSourceDispatchRecordWire,
    source: &transfer_wire::TeamPermanentTransferSourceSnapshotWire,
) -> TestResult {
    assert_eq!(source.workspace_id, request.actor.scope.workspace_id);
    assert_eq!(source.preparation_id, request.command.preparation_id);
    assert_eq!(source.transfer_id, request.command.transfer_id);
    assert_eq!(source.generation, request.command.expected_generation);
    assert_eq!(
        source.state,
        transfer_wire::TeamPermanentTransferSourceStateWire::Prepared
    );
    assert_eq!(source.issuer_id, dispatch.issuer_id);

    let snapshot = URL_SAFE_NO_PAD.decode(source.snapshot_base64.as_bytes())?;
    let record = HomeTransferSourceRecord::from_snapshot_bytes(&snapshot)?;
    assert_eq!(record.to_snapshot_bytes()?, snapshot);
    assert_eq!(record.state(), HomeTransferSourceState::Prepared);
    assert_eq!(
        record.request().witness().preparation_id(),
        request.command.preparation_id
    );
    assert_eq!(
        record.request().scope().workspace_id().into_uuid(),
        request.actor.scope.workspace_id
    );
    assert_eq!(
        record.request().scope().backend_id(),
        context.source.client.expected_backend_id()
    );
    assert_eq!(
        record.request().scope().destination_id(),
        context.source.client.destination_id()
    );
    assert!(
        record
            .request()
            .sessions()
            .contains(&AgentSessionId::from_uuid(context.home_member.session_id))
    );
    assert_eq!(
        source.source_request_digest,
        lower_hex(record.request().request_digest().as_bytes())
    );
    assert_eq!(
        source.snapshot_sha256,
        lower_hex(&Sha256::digest(&snapshot))
    );
    assert_eq!(
        source.inventory_digest,
        record.inventory_digest().map(|digest| lower_hex(digest))
    );

    let source_request_bytes =
        URL_SAFE_NO_PAD.decode(dispatch.source_request_envelope_base64.as_bytes())?;
    let source_request = forge_backend::decode_home_transfer_source_request(&source_request_bytes)?;
    assert_eq!(&source_request, record.request());
    Ok(())
}

fn lower_hex(bytes: &[u8]) -> String {
    let mut result = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        let _ignored = write!(&mut result, "{byte:02x}");
    }
    result
}

async fn dispatch_with_dropped_reply(
    context: &ExistingHomeV3FlowContext<'_>,
    request: &transfer_wire::TeamPermanentTransferSourceDispatchRequest,
) -> TestResult<transfer_wire::TeamPermanentTransferSourceDispatchResponse> {
    let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).await?;
    let proxy_client =
        OrchestrationHttpClient::new(listener.local_addr()?, super::super::DEADLINE)?;
    let expected = request.clone();
    let bearer = context.bearer.clone();
    let backend = context.backend_address;
    let proxy = tokio::spawn(async move {
        tokio::time::timeout(
            super::super::DEADLINE,
            forward_and_drop_dispatch_reply(listener, backend, bearer, expected),
        )
        .await
        .map_err(|_| "Source dispatch response-drop proxy timed out".to_owned())?
    });
    let result = proxy_client
        .dispatch_team_permanent_transfer_source(context.bearer, request)
        .await;
    let committed = proxy.await??;
    if !matches!(
        &result,
        Err(ClientError::UnknownOutcome {
            readback: ReadbackTarget::TeamPermanentTransferSourceDispatch(original),
        }) if original.as_ref() == request
    ) {
        return Err(format!(
            "lost Source dispatch reply did not retain exact readback identity: {result:?}"
        )
        .into());
    }
    if committed.request != *request
        || committed.disposition
            != transfer_wire::TeamPermanentTransferSourceDispatchDispositionWire::Created
    {
        return Err("upstream did not commit the exact Source dispatch before reply loss".into());
    }
    Ok(committed)
}

async fn forward_and_drop_dispatch_reply(
    listener: TcpListener,
    backend: SocketAddr,
    bearer: HeaderValue,
    expected: transfer_wire::TeamPermanentTransferSourceDispatchRequest,
) -> Result<transfer_wire::TeamPermanentTransferSourceDispatchResponse, String> {
    let (mut downstream, _) = listener.accept().await.map_err(|error| error.to_string())?;
    let bytes = temporary_dispatch_lifecycle::read_http_request(&mut downstream).await?;
    let (method, path, body) = temporary_dispatch_lifecycle::http_request_parts(&bytes)?;
    let expected_path = format!(
        "/internal/v1/workspaces/{}/teams/permanent-transfers/source/dispatch",
        expected.actor.scope.workspace_id
    );
    if method != "POST"
        || path != expected_path
        || temporary_dispatch_lifecycle::request_authorization(&bytes)?
            != bearer.to_str().map_err(|error| error.to_string())?
    {
        return Err("Source dispatch response-drop proxy request scope mismatch".to_owned());
    }
    let parsed: transfer_wire::TeamPermanentTransferSourceDispatchRequest =
        serde_json::from_slice(body).map_err(|error| error.to_string())?;
    if parsed != expected {
        return Err("Source dispatch response-drop proxy request changed".to_owned());
    }
    let upstream = OrchestrationHttpClient::new(backend, super::super::DEADLINE)
        .map_err(|error| error.to_string())?;
    let response = upstream
        .dispatch_team_permanent_transfer_source(&bearer, &parsed)
        .await
        .map_err(|error| format!("upstream Source dispatch before reply loss: {error:?}"))?;
    if response.request != expected
        || response.disposition
            != transfer_wire::TeamPermanentTransferSourceDispatchDispositionWire::Created
    {
        return Err("upstream did not confirm the exact fresh Source dispatch".to_owned());
    }
    drop(downstream);
    Ok(response)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires AGENT_BROKERD_BIN; real Broker and Forge/Orchestration HTTP with a synthetic single-principal fixture, no provider execution"]
async fn real_broker_existing_home_source_dispatch_recovers_without_resend_http() -> TestResult {
    let cluster = super::super::broker_cluster::Cluster::start().await?;
    let result = Box::pin(super::super::bootstrap_fixture_with_broker_options(
        cluster.orchestration_config()?,
        true,
        false,
        super::super::TransferExercise::SourceDispatch,
    ))
    .await;
    drop(cluster);
    result
}
