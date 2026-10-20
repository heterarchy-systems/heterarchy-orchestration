//! Bounded request admission and request-body read deadlines for the internal
//! Axum surface.
//!
//! This module owns transport admission only. Authentication, authorization,
//! mutation ownership, and router composition remain with the parent backend.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use axum::body::{Body, BodyDataStream, Bytes, HttpBody};
use axum::extract::{Request, State};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use futures_core::Stream;
use thiserror::Error;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::time::Sleep;

const ORDINARY_REQUEST_CAPACITY: usize = 32;
const CONTROL_REQUEST_CAPACITY: usize = 4;
const BODY_READ_DEADLINE: Duration = Duration::from_secs(10);

/// Shared bounded admission for ordinary and reserved control requests.
#[derive(Debug, Clone)]
pub(crate) struct RequestAdmission {
    ordinary: Arc<Semaphore>,
    control: Arc<Semaphore>,
}

impl RequestAdmission {
    /// Create one admission owner with independent ordinary/control bounds.
    #[must_use]
    pub(crate) fn new() -> Self {
        Self {
            ordinary: Arc::new(Semaphore::new(ORDINARY_REQUEST_CAPACITY)),
            control: Arc::new(Semaphore::new(CONTROL_REQUEST_CAPACITY)),
        }
    }

    fn try_admit(&self, control: bool) -> Result<OwnedSemaphorePermit, crate::ApiError> {
        let slots = if control {
            &self.control
        } else {
            &self.ordinary
        };
        Arc::clone(slots)
            .try_acquire_owned()
            .map_err(|_| crate::ApiError::Capacity)
    }
}

/// Admit one internal request and install the bounded body-read deadline.
pub(crate) async fn middleware(
    State(admission): State<RequestAdmission>,
    request: Request,
    next: Next,
) -> Response {
    let control = is_control_path(request.uri().path());
    let _permit = match admission.try_admit(control) {
        Ok(permit) => permit,
        Err(error) => return error.into_response(),
    };
    let request = request.map(|body| DeadlineBody::wrap(body, BODY_READ_DEADLINE));
    next.run(request).await
}

fn is_control_path(path: &str) -> bool {
    matches!(
        path,
        "/internal/v1/approval-automatic/disable" | "/internal/v1/approval-expiry/reconcile"
    ) || (path.starts_with("/internal/v1/sessions/") && path.ends_with("/close"))
        || (path.starts_with("/internal/v1/approval-requests/") && path.ends_with("/decision"))
        || (path.starts_with("/internal/v1/workspaces/")
            && path.ends_with("/approval-requests/decision"))
        || (path.starts_with("/internal/v1/workspaces/")
            && path.ends_with("/provision-operations/cancel"))
        || (path.starts_with("/internal/v1/workspaces/")
            && path.ends_with("/provision-operations/retire-unobserved"))
}

/// Body adapter that bounds only the time spent waiting for request-body
/// frames. Once the body reaches EOF and the handler begins, this deadline is
/// no longer involved in the mutation lifecycle.
struct DeadlineBody {
    inner: Pin<Box<BodyDataStream>>,
    deadline: Pin<Box<Sleep>>,
    done: bool,
}

impl DeadlineBody {
    fn wrap<B>(body: B, timeout: Duration) -> Body
    where
        B: HttpBody<Data = Bytes> + Send + 'static,
        B::Error: Into<axum::BoxError>,
    {
        let body = Body::new(body);
        Body::from_stream(Self {
            inner: Box::pin(body.into_data_stream()),
            deadline: Box::pin(tokio::time::sleep(timeout)),
            done: false,
        })
    }
}

impl Stream for DeadlineBody {
    type Item = Result<Bytes, axum::Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.done {
            return Poll::Ready(None);
        }
        if this.deadline.as_mut().poll(cx).is_ready() {
            this.done = true;
            return Poll::Ready(Some(Err(axum::Error::new(BodyReadTimeout))));
        }
        let frame = this.inner.as_mut().poll_next(cx);
        if matches!(frame, Poll::Ready(None | Some(Err(_)))) {
            this.done = true;
        }
        frame
    }
}

#[derive(Debug, Error)]
#[error("request body read timed out")]
struct BodyReadTimeout;

#[cfg(test)]
mod tests {
    use std::future::poll_fn;

    use super::*;

    struct PendingStream;

    impl Stream for PendingStream {
        type Item = Result<Bytes, axum::Error>;

        fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            Poll::Pending
        }
    }

    #[test]
    fn ordinary_saturation_leaves_reserved_control_available()
    -> Result<(), Box<dyn std::error::Error>> {
        let admission = RequestAdmission::new();
        let mut ordinary = Vec::new();
        for _ in 0..ORDINARY_REQUEST_CAPACITY {
            ordinary.push(
                admission
                    .try_admit(false)
                    .map_err(|_| "ordinary capacity closed early")?,
            );
        }
        assert!(matches!(
            admission.try_admit(false),
            Err(crate::ApiError::Capacity)
        ));
        let control = admission
            .try_admit(true)
            .map_err(|_| "reserved control capacity was exhausted")?;
        drop(control);
        drop(ordinary);
        Ok(())
    }

    #[test]
    fn reserved_paths_use_the_control_lane() {
        for path in [
            "/internal/v1/sessions/00000000-0000-0000-0000-000000000001/close",
            "/internal/v1/approval-requests/00000000-0000-0000-0000-000000000001/decision",
            "/internal/v1/workspaces/00000000-0000-0000-0000-000000000001/approval-requests/decision",
            "/internal/v1/approval-automatic/disable",
            "/internal/v1/approval-expiry/reconcile",
            "/internal/v1/workspaces/00000000-0000-0000-0000-000000000001/provision-operations/cancel",
            "/internal/v1/workspaces/00000000-0000-0000-0000-000000000001/provision-operations/retire-unobserved",
        ] {
            assert!(is_control_path(path), "expected reserved path: {path}");
        }
        assert!(!is_control_path("/internal/v1/sessions"));
        assert!(!is_control_path("/internal/v1/approval-requests"));
    }

    #[tokio::test]
    async fn deadline_ends_a_permanently_pending_body() -> Result<(), Box<dyn std::error::Error>> {
        let body = DeadlineBody::wrap(Body::from_stream(PendingStream), Duration::ZERO);
        let mut stream = body.into_data_stream();
        let frame = tokio::time::timeout(
            Duration::from_secs(1),
            poll_fn(|cx| Pin::new(&mut stream).poll_next(cx)),
        )
        .await
        .map_err(|_| "body deadline test stalled")?;
        match frame {
            Some(Err(error)) => assert_eq!(error.to_string(), "request body read timed out"),
            other => return Err(format!("expected timeout error, got {other:?}").into()),
        }
        Ok(())
    }

    #[tokio::test]
    async fn eof_stays_eof() -> Result<(), Box<dyn std::error::Error>> {
        let body = DeadlineBody::wrap(Body::empty(), Duration::from_secs(10));
        let mut stream = body.into_data_stream();
        let frame = tokio::time::timeout(
            Duration::from_secs(1),
            poll_fn(|cx| Pin::new(&mut stream).poll_next(cx)),
        )
        .await
        .map_err(|_| "body EOF test stalled")?;
        assert!(frame.is_none());
        Ok(())
    }
}
