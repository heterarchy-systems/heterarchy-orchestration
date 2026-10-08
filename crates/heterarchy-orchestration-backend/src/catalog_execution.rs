//! Bounded ownership and control admission for catalog operations.

use std::collections::VecDeque;
use std::future::Future;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};

use tokio::sync::{OwnedSemaphorePermit, Semaphore, oneshot};
use tokio_util::task::TaskTracker;
use tracing::warn;

use crate::ApiError;

pub(super) const CATALOG_ADMISSION_CAPACITY: usize = 16;
const CONTROL_ADMISSION_CAPACITY: usize = 4;
const MAX_CONTROL_BURST: usize = 4;

#[derive(Debug, Clone, Copy)]
pub(super) enum CatalogLane {
    Ordinary,
    Control,
}

#[derive(Debug, Clone)]
pub(super) struct CatalogExecution {
    admission_gate: Arc<Mutex<()>>,
    accepting: Arc<AtomicBool>,
    pub(super) admitted_slots: Arc<Semaphore>,
    control_slots: Arc<Semaphore>,
    execution: Arc<ExecutionSlot>,
    pub(super) tasks: TaskTracker,
}

impl CatalogExecution {
    pub(super) fn new() -> Self {
        Self {
            admission_gate: Arc::new(Mutex::new(())),
            accepting: Arc::new(AtomicBool::new(true)),
            admitted_slots: Arc::new(Semaphore::new(CATALOG_ADMISSION_CAPACITY)),
            control_slots: Arc::new(Semaphore::new(CONTROL_ADMISSION_CAPACITY)),
            execution: Arc::new(ExecutionSlot::default()),
            tasks: TaskTracker::new(),
        }
    }

    pub(super) fn try_admit(&self, lane: CatalogLane) -> Result<OwnedSemaphorePermit, ApiError> {
        if !self.accepting.load(Ordering::Acquire) {
            return Err(ApiError::Unavailable);
        }
        let slots = match lane {
            CatalogLane::Ordinary => &self.admitted_slots,
            CatalogLane::Control => &self.control_slots,
        };
        let permit = Arc::clone(slots)
            .try_acquire_owned()
            .map_err(|_| ApiError::Capacity)?;
        if !self.accepting.load(Ordering::Acquire) {
            return Err(ApiError::Unavailable);
        }
        Ok(permit)
    }

    pub(super) async fn execution_slot(&self, lane: CatalogLane) -> Result<CatalogSlot, ApiError> {
        self.execution.acquire(lane).await
    }

    pub(super) fn stop_admission(&self) {
        let _gate = self
            .admission_gate
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.accepting.store(false, Ordering::Release);
        self.admitted_slots.close();
        self.control_slots.close();
    }

    pub(super) fn spawn<T, F>(
        &self,
        lane: CatalogLane,
        operation: F,
    ) -> Result<oneshot::Receiver<Result<T, ApiError>>, ApiError>
    where
        T: Send + 'static,
        F: Future<Output = Result<T, ApiError>> + Send + 'static,
    {
        // Registration and stop share a synchronous gate, never held over await.
        let _gate = self
            .admission_gate
            .lock()
            .map_err(|_| ApiError::Unavailable)?;
        let admitted = self.try_admit(lane)?;
        let (sender, receiver) = oneshot::channel();
        let _tracked = self.tasks.spawn(async move {
            let _admitted = admitted;
            let result = tokio::spawn(operation).await.unwrap_or_else(|error| {
                warn!(?error, "owned Orchestration operation failed to join");
                Err(ApiError::Unavailable)
            });
            // Observer loss does not cancel an admitted mutation or its readback.
            let _disconnected = sender.send(result);
        });
        Ok(receiver)
    }

    pub(super) async fn drain(&self) {
        self.tasks.close();
        self.tasks.wait().await;
    }
}

#[derive(Debug, Default)]
struct ExecutionSlot {
    state: Mutex<SlotState>,
}

#[derive(Debug, Default)]
struct SlotState {
    busy: bool,
    control_burst: usize,
    ordinary: VecDeque<oneshot::Sender<CatalogSlot>>,
    control: VecDeque<oneshot::Sender<CatalogSlot>>,
}

impl ExecutionSlot {
    async fn acquire(self: &Arc<Self>, lane: CatalogLane) -> Result<CatalogSlot, ApiError> {
        let receiver = {
            let mut state = self.state.lock().map_err(|_| ApiError::Unavailable)?;
            if !state.busy {
                state.busy = true;
                state.control_burst = 0;
                return Ok(CatalogSlot {
                    owner: Arc::clone(self),
                    active: true,
                });
            }
            let (sender, receiver) = oneshot::channel();
            match lane {
                CatalogLane::Ordinary => state.ordinary.push_back(sender),
                CatalogLane::Control => state.control.push_back(sender),
            }
            receiver
        };
        receiver.await.map_err(|_| ApiError::Unavailable)
    }

    fn release(self: &Arc<Self>) {
        loop {
            let sender = {
                let mut state = self
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                // Closed observers cannot trap a slot. Each queue is bounded by
                // its admitted tasks; no background dispatcher is needed.
                state.ordinary.retain(|sender| !sender.is_closed());
                state.control.retain(|sender| !sender.is_closed());
                let control = !state.control.is_empty()
                    && (state.ordinary.is_empty() || state.control_burst < MAX_CONTROL_BURST);
                let sender = if control {
                    state.control.pop_front()
                } else {
                    state.ordinary.pop_front()
                };
                let Some(sender) = sender else {
                    state.busy = false;
                    return;
                };
                state.control_burst = if control {
                    (state.control_burst + 1).min(MAX_CONTROL_BURST)
                } else {
                    0
                };
                sender
            };
            // Sending wakes a receiver; do it outside the state mutex. A
            // receiver may immediately drop its delivered guard and re-enter.
            let slot = CatalogSlot {
                owner: Arc::clone(self),
                active: true,
            };
            match sender.send(slot) {
                Ok(()) => return,
                Err(mut unsent) => {
                    // Retain the busy reservation while selecting another waiter.
                    unsent.active = false;
                }
            }
        }
    }
}

#[derive(Debug)]
pub(super) struct CatalogSlot {
    owner: Arc<ExecutionSlot>,
    active: bool,
}

impl Drop for CatalogSlot {
    fn drop(&mut self) {
        if self.active {
            self.owner.release();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::future::poll_fn;
    use std::task::Poll;

    use super::*;

    #[tokio::test]
    async fn control_priority_is_fifo_and_yields_after_a_bounded_burst()
    -> Result<(), Box<dyn std::error::Error>> {
        let execution = CatalogExecution::new();
        let held = execution
            .execution_slot(CatalogLane::Ordinary)
            .await
            .map_err(|_| "slot unavailable")?;
        let mut ordinary = Box::pin(execution.execution_slot(CatalogLane::Ordinary));
        poll_fn(|cx| {
            assert!(ordinary.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        let mut controls = Vec::new();
        for _ in 0..=MAX_CONTROL_BURST {
            let mut control = Box::pin(execution.execution_slot(CatalogLane::Control));
            poll_fn(|cx| {
                assert!(control.as_mut().poll(cx).is_pending());
                Poll::Ready(())
            })
            .await;
            controls.push(control);
        }
        drop(held);
        for _ in 0..MAX_CONTROL_BURST {
            // Later controls stay pending while the FIFO head owns the one slot.
            let slot = controls.remove(0).await.map_err(|_| "control slot lost")?;
            poll_fn(|cx| {
                assert!(ordinary.as_mut().poll(cx).is_pending());
                assert!(controls[0].as_mut().poll(cx).is_pending());
                Poll::Ready(())
            })
            .await;
            drop(slot);
        }
        poll_fn(|cx| {
            assert!(controls[0].as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        let slot = ordinary.await.map_err(|_| "ordinary work starved")?;
        drop(slot);
        drop(controls.remove(0).await.map_err(|_| "last control lost")?);
        Ok(())
    }

    #[tokio::test]
    async fn dropping_queued_and_delivered_grants_cannot_trap_the_single_slot()
    -> Result<(), Box<dyn std::error::Error>> {
        let execution = CatalogExecution::new();
        let held = execution
            .execution_slot(CatalogLane::Ordinary)
            .await
            .map_err(|_| "slot unavailable")?;
        let mut abandoned = Box::pin(execution.execution_slot(CatalogLane::Control));
        poll_fn(|cx| {
            assert!(abandoned.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        drop(abandoned);
        let mut delivered = Box::pin(execution.execution_slot(CatalogLane::Control));
        poll_fn(|cx| {
            assert!(delivered.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        drop(held);
        // The guard is owned by the channel even before its receiver is polled.
        drop(delivered);
        let next = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            execution.execution_slot(CatalogLane::Ordinary),
        )
        .await?;
        drop(next.map_err(|_| "cancelled grant retained the slot")?);
        Ok(())
    }

    #[tokio::test]
    async fn disconnected_control_work_is_retained_and_shutdown_drains_both_lanes()
    -> Result<(), Box<dyn std::error::Error>> {
        let execution = CatalogExecution::new();
        let release = tokio_util::sync::CancellationToken::new();
        let barrier = release.clone();
        let receiver = execution
            .spawn(CatalogLane::Control, async move {
                barrier.cancelled().await;
                Ok(())
            })
            .map_err(|_| "control admission closed")?;
        drop(receiver);
        execution.stop_admission();
        for lane in [CatalogLane::Ordinary, CatalogLane::Control] {
            assert!(matches!(
                execution.spawn(lane, async { Ok(()) }),
                Err(ApiError::Unavailable)
            ));
        }
        let mut drain = Box::pin(execution.drain());
        poll_fn(|cx| {
            assert!(drain.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        release.cancel();
        tokio::time::timeout(std::time::Duration::from_secs(2), drain).await?;
        assert!(execution.tasks.is_empty());
        Ok(())
    }
}
