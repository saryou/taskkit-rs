use futures_util::FutureExt;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;
use tokio::time::sleep;

use crate::service::Service;
use crate::task::TaskGroup;
use crate::worker::Worker;

/// Waits up to `duration`, returning `false` as soon as a stop is requested.
///
/// Always yields at least once: a runner whose work completes without ever
/// suspending (an in-process backend, or a queue that is never empty) would
/// otherwise monopolise its scheduler and starve everything else on it.
async fn wait_or_stop(duration: Duration, should_stop: &AtomicBool) -> bool {
    tokio::task::yield_now().await;

    let start = tokio::time::Instant::now();
    while start.elapsed() < duration {
        if should_stop.load(Ordering::Relaxed) {
            return false;
        }
        sleep(Duration::from_millis(100).min(duration)).await;
    }
    !should_stop.load(Ordering::Relaxed)
}

pub(crate) struct ServiceRunner {
    handle: Option<JoinHandle<()>>,
    should_stop: Arc<AtomicBool>,
}

impl ServiceRunner {
    pub fn spawn<S: Service + 'static>(mut service: S) -> Self {
        let should_stop = Arc::new(AtomicBool::new(false));
        let should_stop_clone = should_stop.clone();
        let service_name = std::any::type_name::<S>();

        let handle = tokio::spawn(async move {
            loop {
                if should_stop_clone.load(Ordering::Relaxed) {
                    break;
                }

                let wait_duration = match AssertUnwindSafe(service.call()).catch_unwind().await {
                    Ok(duration) => duration,
                    Err(_) => {
                        tracing::error!("service `{}` panicked; retrying", service_name);
                        Duration::from_secs(1)
                    }
                };

                if !wait_or_stop(wait_duration, &should_stop_clone).await {
                    return;
                }
            }
        });

        Self {
            handle: Some(handle),
            should_stop,
        }
    }

    pub fn stop(&mut self) {
        self.should_stop.store(true, Ordering::Relaxed);
    }

    pub async fn join(mut self) {
        if let Some(handle) = self.handle.take() {
            let _ = handle.await;
        }
    }
}

/// Upper bound on how long a saturated group blocks before re-checking whether it
/// has been stopped or paused. A freed slot wakes the loop immediately, so this
/// bound does not add latency to task dispatch.
const STOP_CHECK_INTERVAL: Duration = Duration::from_millis(100);

const PAUSE_POLL_INTERVAL: Duration = Duration::from_secs(1);

/// Runs one task group: a single fetch loop feeding a bounded pool of executions.
///
/// One fetch loop per group, rather than one per execution slot, ties polling
/// traffic to the polling interval instead of to the concurrency limit.
pub(crate) struct GroupRunner {
    worker: Arc<Worker>,
    handle: Option<JoinHandle<()>>,
    should_stop: Arc<AtomicBool>,
    paused: Arc<AtomicBool>,
}

impl GroupRunner {
    pub fn spawn(worker: Worker, concurrency: usize, polling_interval: Duration) -> Self {
        let worker = Arc::new(worker);
        let should_stop = Arc::new(AtomicBool::new(false));
        let paused = Arc::new(AtomicBool::new(false));
        let slots = Arc::new(Semaphore::new(concurrency));

        let handle = tokio::spawn({
            let worker = worker.clone();
            let should_stop = should_stop.clone();
            let paused = paused.clone();
            let slots = slots.clone();

            async move {
                while !should_stop.load(Ordering::Relaxed) {
                    if paused.load(Ordering::Relaxed) {
                        if !wait_or_stop(PAUSE_POLL_INTERVAL, &should_stop).await {
                            break;
                        }
                        continue;
                    }

                    // This loop is the only permit consumer, so the free count cannot
                    // shrink between reading it and claiming that many tasks.
                    let free = slots.available_permits();
                    if free == 0 {
                        // Wait on the semaphore rather than polling it, so dispatch
                        // resumes the instant a task finishes. The permit is released
                        // straight away and re-taken below; the timeout only exists so
                        // stop and pause are still observed while fully saturated.
                        match tokio::time::timeout(STOP_CHECK_INTERVAL, slots.acquire()).await {
                            Ok(Ok(permit)) => drop(permit),
                            Ok(Err(_)) => break,
                            Err(_) => {}
                        }
                        continue;
                    }

                    let assigned = match AssertUnwindSafe(worker.assign(free)).catch_unwind().await
                    {
                        Ok(assigned) => assigned,
                        Err(_) => {
                            tracing::error!("[{}] assign panicked; retrying", worker.get_id());
                            Vec::new()
                        }
                    };
                    let claimed = assigned.len();

                    for record in assigned {
                        let permit = slots
                            .clone()
                            .acquire_owned()
                            .await
                            .expect("slot semaphore is never closed");
                        let worker = worker.clone();
                        tokio::spawn(async move {
                            worker.handle_task(record).await;
                            drop(permit);
                        });
                    }

                    // A short batch means the queue ran dry; a full one means there may
                    // be more waiting, so go straight back for it.
                    let backoff = if claimed < free {
                        polling_interval
                    } else {
                        Duration::ZERO
                    };
                    if !wait_or_stop(backoff, &should_stop).await {
                        break;
                    }
                }

                // Let running tasks finish before the runner reports itself as joined.
                let _ = slots.acquire_many(concurrency as u32).await;
            }
        });

        Self {
            worker,
            handle: Some(handle),
            should_stop,
            paused,
        }
    }

    pub fn worker(&self) -> Arc<Worker> {
        self.worker.clone()
    }

    pub fn pause(&self) {
        self.paused.store(true, Ordering::Relaxed);
    }

    pub fn resume(&self) {
        self.paused.store(false, Ordering::Relaxed);
    }

    pub fn stop(&self) {
        self.should_stop.store(true, Ordering::Relaxed);
    }

    pub fn is_alive(&self) -> bool {
        !self.should_stop.load(Ordering::Relaxed)
    }

    pub fn get_group(&self) -> &TaskGroup {
        &self.worker.group
    }

    pub async fn join(mut self) {
        if let Some(handle) = self.handle.take() {
            let _ = handle.await;
        }
    }
}
