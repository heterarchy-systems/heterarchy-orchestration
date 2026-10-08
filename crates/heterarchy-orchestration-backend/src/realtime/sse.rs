//! Axum SSE adapter over the realtime hub.
//!
//! This is a thin transport boundary: admission decisions happen before the
//! response headers are sent (so they stay ordinary typed HTTP errors), and
//! every stream frame is a bounded envelope encoded once per subscriber.
//! Keep-alives are comment frames injected by axum and never carry or advance
//! the semantic cursor.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use axum::Json;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use futures_core::Stream;
use serde::Serialize;
use uuid::Uuid;

use super::CursorInvalid;
use super::cursor::EventCursor;
use super::event::RealtimeEventKind;
use super::hub::{PublishedEvent, RealtimeHub, SubscriptionError};
use super::stream::{EventStream, ResyncNotice, ResyncReason, StreamFrame};

/// `Last-Event-ID` header carrying the client's resume cursor.
const LAST_EVENT_ID_HEADER: axum::http::HeaderName =
    axum::http::header::HeaderName::from_static("last-event-id");

/// Keep-alive comment interval.
///
/// 15 seconds stays below the 30-60s idle timeouts commonly enforced by
/// intermediaries while adding negligible traffic; the frames are SSE
/// comments, so they never advance the client's semantic cursor.
const KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(15);

/// Reconnect guidance attached to the terminal `resync_required` event.
///
/// Purely advisory (SSE `retry` field); it spreads reconnect attempts so a
/// server restart does not create a thundering herd.
const RESYNC_RETRY_HINT: Duration = Duration::from_secs(2);

/// SSE `event:` name for ordinary state-change observations.
const STATE_CHANGED_EVENT: &str = "state_changed";
/// SSE `event:` name for the terminal resync notice.
const RESYNC_REQUIRED_EVENT: &str = "resync_required";

/// Serve the Orchestration realtime observation stream.
///
/// The `Last-Event-ID` request header, when present, is parsed once by the
/// typed cursor parser and resolved against the hub's bounded replay window
/// before the stream is admitted.
///
/// # Errors
///
/// Returns [`RealtimeAdmissionError`] for a rejected cursor, a required
/// resync, or exhausted subscriber admission; no response headers have been
/// sent at that point.
// Axum handlers consume their extractors by value; `HeaderMap` is an
// extractor here, not a plain argument.
#[allow(clippy::needless_pass_by_value)]
pub fn events_handler(
    State(hub): State<RealtimeHub>,
    headers: HeaderMap,
) -> Result<
    Sse<impl Stream<Item = Result<Event, serde_json::Error>> + Send + 'static>,
    RealtimeAdmissionError,
> {
    Ok(sse_response(subscribe_stream(&hub, &headers, None)?))
}

/// Serve the workspace-scoped realtime observation stream.
///
/// The caller supplies a workspace UUID that has already been authenticated
/// and authorized. This adapter only applies the scope to replay and live
/// delivery; it does not establish or refresh that authorization.
///
/// # Errors
///
/// Returns [`RealtimeAdmissionError`] for a rejected cursor, a required
/// resync, or exhausted subscriber admission; no response headers have been
/// sent at that point.
// Axum handlers consume their extractors by value; `HeaderMap` is an
// extractor here, not a plain argument.
#[allow(clippy::needless_pass_by_value)]
pub fn events_handler_for_workspace(
    State(hub): State<RealtimeHub>,
    headers: HeaderMap,
    workspace_id: Uuid,
) -> Result<
    Sse<impl Stream<Item = Result<Event, serde_json::Error>> + Send + 'static>,
    RealtimeAdmissionError,
> {
    Ok(sse_response(subscribe_stream(
        &hub,
        &headers,
        Some(workspace_id),
    )?))
}

/// Serve a realtime observation stream with an injected authorization guard.
///
/// `None` is the trusted operator/global stream; `Some` scopes delivery to an
/// already-authorized workspace. The guard is responsible for token and
/// membership policy outside this transport boundary. It runs before the first
/// semantic event and then once per second while the stream remains open.
///
/// # Errors
///
/// Returns [`RealtimeAdmissionError`] for a rejected cursor, a required
/// resync, or exhausted subscriber admission; no response headers have been
/// sent at that point.
// Axum handlers consume their extractors by value; `HeaderMap` is an
// extractor here, not a plain argument.
#[allow(clippy::needless_pass_by_value)]
pub fn events_handler_authorized<F, Fut>(
    State(hub): State<RealtimeHub>,
    headers: HeaderMap,
    workspace_id: Option<Uuid>,
    guard: F,
) -> Result<
    Sse<impl Stream<Item = Result<Event, serde_json::Error>> + Send + 'static>,
    RealtimeAdmissionError,
>
where
    F: FnMut() -> Fut + Send + 'static,
    Fut: Future<Output = Result<(), ResyncReason>> + Send + 'static,
{
    Ok(sse_response(subscribe_stream_authorized(
        &hub,
        &headers,
        workspace_id,
        guard,
    )?))
}

fn subscribe_stream(
    hub: &RealtimeHub,
    headers: &HeaderMap,
    workspace_id: Option<Uuid>,
) -> Result<EventStream, RealtimeAdmissionError> {
    let last_event_id = last_event_id(headers)?;
    let subscription = hub.subscribe(last_event_id)?;
    Ok(match workspace_id {
        Some(workspace_id) => EventStream::new_for_workspace(subscription, workspace_id),
        None => EventStream::new(subscription),
    })
}

fn subscribe_stream_authorized<F, Fut>(
    hub: &RealtimeHub,
    headers: &HeaderMap,
    workspace_id: Option<Uuid>,
    guard: F,
) -> Result<EventStream, RealtimeAdmissionError>
where
    F: FnMut() -> Fut + Send + 'static,
    Fut: Future<Output = Result<(), ResyncReason>> + Send + 'static,
{
    let last_event_id = last_event_id(headers)?;
    let subscription = hub.subscribe(last_event_id)?;
    Ok(EventStream::new_authorized(
        subscription,
        workspace_id,
        guard,
    ))
}

fn last_event_id(headers: &HeaderMap) -> Result<Option<&str>, RealtimeAdmissionError> {
    headers
        .get(LAST_EVENT_ID_HEADER)
        .map(|value| {
            value.to_str().map_err(|_| {
                RealtimeAdmissionError::from(SubscriptionError::InvalidCursor(
                    CursorInvalid::Malformed(super::MalformedCursor),
                ))
            })
        })
        .transpose()
}

fn sse_response(
    stream: EventStream,
) -> Sse<impl Stream<Item = Result<Event, serde_json::Error>> + Send + 'static> {
    Sse::new(SseEventStream { inner: stream }).keep_alive(
        KeepAlive::new()
            .interval(KEEP_ALIVE_INTERVAL)
            .text("keep-alive"),
    )
}

/// [`Stream`] adapter that renders [`StreamFrame`]s as axum SSE events.
struct SseEventStream {
    inner: EventStream,
}

impl Stream for SseEventStream {
    type Item = Result<Event, serde_json::Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        match std::task::ready!(this.inner.poll_frame(cx)) {
            None => Poll::Ready(None),
            Some(StreamFrame::Event(event)) => Poll::Ready(Some(encode_event(&event))),
            Some(StreamFrame::Resync(notice)) => Poll::Ready(Some(encode_resync(&notice))),
        }
    }
}

/// Wire projection of one published event; the only place the SSE JSON shape
/// is defined.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct EventWire<'a> {
    epoch: u64,
    sequence: u64,
    kind: RealtimeEventKind,
    resource_id: &'a Uuid,
    resource_revision: u64,
    workspace_id: Option<Uuid>,
    occurred_at_ms: i64,
}

fn encode_event(event: &PublishedEvent) -> Result<Event, serde_json::Error> {
    let data = serde_json::to_string(&EventWire {
        epoch: event.epoch,
        sequence: event.sequence,
        kind: event.event.kind,
        resource_id: &event.event.resource_id,
        resource_revision: event.event.resource_revision,
        workspace_id: event.event.workspace_id,
        occurred_at_ms: event.event.occurred_at_ms,
    })?;
    let id = EventCursor {
        epoch: event.epoch,
        sequence: event.sequence,
    }
    .encode();
    Ok(Event::default()
        .id(id)
        .event(STATE_CHANGED_EVENT)
        .data(data))
}

fn encode_resync(notice: &ResyncNotice) -> Result<Event, serde_json::Error> {
    let data = serde_json::to_string(notice)?;
    Ok(Event::default()
        .event(RESYNC_REQUIRED_EVENT)
        .retry(RESYNC_RETRY_HINT)
        .data(data))
}

/// HTTP rejection for a refused realtime subscription.
///
/// Mirrors the backend `ErrorResponse` wire shape (`code`, `message`,
/// `retryable`) so clients see one error vocabulary across the API.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error(transparent)]
pub struct RealtimeAdmissionError(#[from] SubscriptionError);

impl RealtimeAdmissionError {
    const INVALID_CURSOR: &'static str = "INVALID_CURSOR";
    const RESYNC_REQUIRED: &'static str = "RESYNC_REQUIRED";
    const SUBSCRIBER_LIMIT: &'static str = "REALTIME_SUBSCRIBER_LIMIT";
    const HUB_POISONED: &'static str = "REALTIME_HUB_POISONED";

    /// Bounded JSON error body.
    fn body(&self) -> ErrorBody {
        match self.0 {
            SubscriptionError::ShuttingDown => ErrorBody {
                code: "ORCHESTRATION_UNAVAILABLE",
                message: "The realtime owner is shutting down.",
                retryable: true,
            },
            SubscriptionError::InvalidCursor(CursorInvalid::Malformed(_)) => ErrorBody {
                code: Self::INVALID_CURSOR,
                message: "The Last-Event-ID cursor is not a well-formed <epoch>:<sequence> value.",
                retryable: false,
            },
            SubscriptionError::InvalidCursor(CursorInvalid::Future { .. }) => ErrorBody {
                code: Self::INVALID_CURSOR,
                message: "The Last-Event-ID cursor names an event this stream never published.",
                retryable: false,
            },
            SubscriptionError::ResyncRequired(_) => ErrorBody {
                code: Self::RESYNC_REQUIRED,
                message: "The cursor names another stream epoch or an evicted range; re-read canonical state and reconnect without it.",
                retryable: true,
            },
            SubscriptionError::SubscriberLimitReached => ErrorBody {
                code: Self::SUBSCRIBER_LIMIT,
                message: "The realtime subscriber admission limit is reached; retry after backing off.",
                retryable: true,
            },
            SubscriptionError::HubStatePoisoned => ErrorBody {
                code: Self::HUB_POISONED,
                message: "The realtime hub is unavailable.",
                retryable: false,
            },
        }
    }

    fn status(&self) -> StatusCode {
        match self.0 {
            SubscriptionError::InvalidCursor(_) => StatusCode::BAD_REQUEST,
            SubscriptionError::ResyncRequired(_) => StatusCode::CONFLICT,
            SubscriptionError::SubscriberLimitReached | SubscriptionError::ShuttingDown => {
                StatusCode::SERVICE_UNAVAILABLE
            }
            SubscriptionError::HubStatePoisoned => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

impl IntoResponse for RealtimeAdmissionError {
    fn into_response(self) -> Response {
        let status = self.status();
        let mut response = (status, Json(self.body())).into_response();
        if self.0 == SubscriptionError::SubscriberLimitReached {
            response.headers_mut().insert(
                axum::http::header::RETRY_AFTER,
                axum::http::HeaderValue::from_static("2"),
            );
        }
        response
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ErrorBody {
    code: &'static str,
    message: &'static str,
    retryable: bool,
}

#[cfg(test)]
mod tests {
    use axum::body::to_bytes;
    use uuid::Uuid;

    use super::super::cursor::MalformedCursor;
    use super::super::event::RealtimeEventKind;
    use super::super::replay::ResyncCause;
    use super::super::stream::ResyncReason;
    use super::*;

    #[test]
    fn invalid_header_bytes_cannot_silently_discard_the_resume_cursor()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut headers = HeaderMap::new();
        headers.insert(
            LAST_EVENT_ID_HEADER,
            axum::http::HeaderValue::from_bytes(&[0x80])?,
        );
        let response = match events_handler(State(RealtimeHub::new()), headers) {
            Ok(_) => return Err("invalid cursor was treated as a fresh stream".into()),
            Err(error) => error.into_response(),
        };
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        Ok(())
    }

    #[tokio::test]
    async fn admission_errors_map_to_typed_http_responses() -> Result<(), Box<dyn std::error::Error>>
    {
        let malformed: RealtimeAdmissionError =
            SubscriptionError::InvalidCursor(CursorInvalid::Malformed(MalformedCursor)).into();
        assert_eq!(malformed.into_response().status(), StatusCode::BAD_REQUEST);

        let resync: RealtimeAdmissionError =
            SubscriptionError::ResyncRequired(ResyncCause::RetentionGap {
                cursor_sequence: 1,
                oldest_available: 5,
            })
            .into();
        assert_eq!(resync.into_response().status(), StatusCode::CONFLICT);

        let limited: RealtimeAdmissionError = SubscriptionError::SubscriberLimitReached.into();
        let response = limited.into_response();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            response
                .headers()
                .get(axum::http::header::RETRY_AFTER)
                .and_then(|value| value.to_str().ok()),
            Some("2")
        );
        let body = to_bytes(response.into_body(), 4096).await?;
        let json: serde_json::Value = serde_json::from_slice(&body)?;
        assert_eq!(json["code"], "REALTIME_SUBSCRIBER_LIMIT");
        assert_eq!(json["retryable"], serde_json::Value::Bool(true));
        Ok(())
    }

    #[test]
    fn wire_projection_uses_the_documented_json_contract() -> Result<(), serde_json::Error> {
        let state_changed = serde_json::to_string(&EventWire {
            epoch: 7,
            sequence: 9,
            kind: RealtimeEventKind::TeamChanged,
            resource_id: &Uuid::nil(),
            resource_revision: 3,
            workspace_id: None,
            occurred_at_ms: 42,
        })?;
        assert_eq!(
            state_changed,
            r#"{"epoch":7,"sequence":9,"kind":"team_changed","resourceId":"00000000-0000-0000-0000-000000000000","resourceRevision":3,"workspaceId":null,"occurredAtMs":42}"#
        );

        let resync = serde_json::to_string(&ResyncNotice {
            reason: ResyncReason::Lag { missed_events: 4 },
            current_epoch: 7,
            last_delivered_sequence: 9,
        })?;
        assert_eq!(
            resync,
            r#"{"reason":{"lag":{"missedEvents":4}},"currentEpoch":7,"lastDeliveredSequence":9}"#
        );
        Ok(())
    }
}
