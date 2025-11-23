use futures_util::StreamExt;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;

use crate::backend::{
    ControlEvent, EventBridge, LockProvider, ReceiveEventsError, TaskBackend, WorkerTracker,
};
use crate::kit::PollingInterval;
use crate::runner::{GroupRunner, ServiceRunner};
use crate::scheduler::{ScheduleEntry, Scheduler, SchedulerName};
use crate::service::{
    DiscardDisposableTasks, PurgeDeadWorkers, RefreshWorkerLifetime, RestoreAbandonedTasks,
};
use crate::task::{TaskGroup, TaskRegistry};
use crate::worker::Worker;

/// Interval a group waits before polling the backend again after finding it empty.
const DEFAULT_POLLING_INTERVAL: Duration = Duration::from_secs(1);

/// Error type for Runtime::run
#[derive(Error, Debug)]
pub enum RuntimeError {
    #[error("Failed to receive events: {0}")]
    ReceiveEvents(#[from] ReceiveEventsError),
}

pub(crate) struct Runtime {
    concurrency: HashMap<TaskGroup, usize>,
    task_backend: Arc<dyn TaskBackend>,
    worker_tracker: Arc<dyn WorkerTracker>,
    lock_provider: Arc<dyn LockProvider>,
    bridge: Arc<dyn EventBridge>,
    registry: Arc<TaskRegistry>,
    schedule_entries: HashMap<SchedulerName, Vec<ScheduleEntry>>,
    tzinfo: Option<chrono::FixedOffset>,
    polling_interval: HashMap<TaskGroup, Duration>,
    helper_services: bool,
}

impl Runtime {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        concurrency_per_group: HashMap<TaskGroup, usize>,
        task_backend: Arc<dyn TaskBackend>,
        worker_tracker: Arc<dyn WorkerTracker>,
        lock_provider: Arc<dyn LockProvider>,
        bridge: Arc<dyn EventBridge>,
        registry: Arc<TaskRegistry>,
        schedule_entries: HashMap<SchedulerName, Vec<ScheduleEntry>>,
        tzinfo: Option<chrono::FixedOffset>,
        polling_interval: PollingInterval,
        helper_services: bool,
    ) -> Self {
        assert!(
            concurrency_per_group.values().all(|&n| n > 0),
            "All values for concurrency_per_group must be positive int."
        );

        let polling_interval = match polling_interval {
            PollingInterval::Auto => concurrency_per_group
                .keys()
                .map(|group| (group.clone(), DEFAULT_POLLING_INTERVAL))
                .collect(),
            PollingInterval::Fixed(duration) => concurrency_per_group
                .keys()
                .map(|group| (group.clone(), duration))
                .collect(),
            PollingInterval::PerGroup(map) => map,
        };

        Self {
            concurrency: concurrency_per_group,
            task_backend,
            worker_tracker,
            lock_provider,
            bridge,
            registry,
            schedule_entries,
            tzinfo,
            polling_interval,
            helper_services,
        }
    }

    pub async fn run(self) -> Result<(), RuntimeError> {
        let process_id = std::process::id();
        tracing::info!(
            "[{}] taskkit runtime started: {:?}",
            process_id,
            self.concurrency
        );

        // One fetch loop per group, each feeding a pool of at most `n` executions.
        let mut worker_runners = Vec::new();

        for (group, &n) in &self.concurrency {
            let interval = self
                .polling_interval
                .get(group)
                .copied()
                .unwrap_or(DEFAULT_POLLING_INTERVAL);

            let worker = Worker::new(
                group.clone(),
                self.task_backend.clone(),
                self.registry.clone(),
            );
            worker_runners.push(Arc::new(GroupRunner::spawn(worker, n, interval)));
        }

        let refresh_ttl = ServiceRunner::spawn(RefreshWorkerLifetime::new(
            self.worker_tracker.clone(),
            worker_runners.clone(),
        ));

        let mut services = Vec::new();

        for (name, entries) in self.schedule_entries {
            if !entries.is_empty() {
                services.push(ServiceRunner::spawn(Scheduler::new(
                    name,
                    self.task_backend.clone(),
                    self.lock_provider.clone(),
                    entries,
                    self.tzinfo,
                )));
            }
        }

        if self.helper_services {
            services.push(ServiceRunner::spawn(RestoreAbandonedTasks::new(
                self.task_backend.clone(),
                self.worker_tracker.clone(),
                self.lock_provider.clone(),
            )));
            services.push(ServiceRunner::spawn(PurgeDeadWorkers::new(
                self.worker_tracker.clone(),
                self.lock_provider.clone(),
            )));
            services.push(ServiceRunner::spawn(DiscardDisposableTasks::new(
                self.task_backend.clone(),
                self.lock_provider.clone(),
            )));
        }

        let mut alive: HashSet<TaskGroup> = self.concurrency.keys().cloned().collect();

        let mut event_receiver = self.bridge.receive_events().await?;

        let result = tokio::select! {
            _ = async {
                loop {
                    if let Some(event) = event_receiver.next().await {
                        let target_groups: HashSet<TaskGroup> = match event.groups() {
                            None => alive.clone(),
                            Some(groups) => groups.iter().cloned().collect(),
                        };

                        tracing::info!("[{}] control event received: {:?}", process_id, event);

                        match event {
                            ControlEvent::Shutdown { .. } => {
                                for worker_runner in &worker_runners {
                                    if target_groups.contains(worker_runner.get_group()) {
                                        worker_runner.stop();
                                    }
                                }
                                alive.retain(|g| !target_groups.contains(g));
                                if alive.is_empty() {
                                    break;
                                }
                            }
                            ControlEvent::Pause { .. } => {
                                for worker_runner in &worker_runners {
                                    if target_groups.contains(worker_runner.get_group()) {
                                        worker_runner.pause();
                                    }
                                }
                            }
                            ControlEvent::Resume { .. } => {
                                for worker_runner in &worker_runners {
                                    if target_groups.contains(worker_runner.get_group()) {
                                        worker_runner.resume();
                                    }
                                }
                            }
                        }
                    }
                }
            } => Ok(()),
            _ = tokio::signal::ctrl_c() => Ok(()),
        };

        tracing::info!("[{}] shutting down...", process_id);

        for worker_runner in &worker_runners {
            worker_runner.stop();
        }

        for service in &mut services {
            service.stop();
        }

        for service in services {
            service.join().await;
        }

        // Scoped so the service is dropped here: it holds the only other references
        // to the group runners, which must be sole-owned before they can be joined.
        {
            let mut refresh_ttl = refresh_ttl;
            refresh_ttl.stop();
            refresh_ttl.join().await;
        }

        for worker_runner in worker_runners {
            match Arc::try_unwrap(worker_runner) {
                Ok(wr) => wr.join().await,
                Err(_) => {
                    tracing::warn!("Failed to unwrap worker runner Arc, multiple references exist");
                }
            }
        }

        result
    }
}
