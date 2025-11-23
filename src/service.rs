use async_trait::async_trait;
use std::sync::Arc;
use std::time::Duration;

use crate::backend::{LockProvider, TaskBackend, WorkerTracker};
use crate::runner::GroupRunner;
use crate::util::{Timestamp, Ttl};

/// Worker time-to-live (how long worker heartbeats are valid)
pub(crate) const WORKER_TTL: Ttl = Ttl(Duration::from_secs(15));

/// A background job that runs on its own schedule.
///
/// Services that guard their work with a [`Lock`](crate::Lock) must stay correct
/// when two processes run them at once, since a lock can expire mid-run.
#[async_trait]
pub(crate) trait Service: Send + Sync {
    /// Call the service.
    ///
    /// It should return the time interval to wait for next call.
    async fn call(&mut self) -> Duration;
}

pub(crate) struct RefreshWorkerLifetime {
    worker_tracker: Arc<dyn WorkerTracker>,
    workers: Vec<Arc<GroupRunner>>,
}

impl RefreshWorkerLifetime {
    pub fn new(worker_tracker: Arc<dyn WorkerTracker>, workers: Vec<Arc<GroupRunner>>) -> Self {
        Self {
            worker_tracker,
            workers,
        }
    }
}

#[async_trait]
impl Service for RefreshWorkerLifetime {
    async fn call(&mut self) -> Duration {
        let alive_worker_ids = self
            .workers
            .iter()
            .filter(|w| w.is_alive())
            .map(|w| w.worker().get_id().clone())
            .collect();

        let _ = self
            .worker_tracker
            .set_worker_ttl(
                alive_worker_ids,
                Timestamp::now() + WORKER_TTL.as_duration(),
            )
            .await;

        WORKER_TTL.as_duration() / 3
    }
}

pub(crate) struct PurgeDeadWorkers {
    worker_tracker: Arc<dyn WorkerTracker>,
    lock_provider: Arc<dyn LockProvider>,
}

impl PurgeDeadWorkers {
    pub fn new(
        worker_tracker: Arc<dyn WorkerTracker>,
        lock_provider: Arc<dyn LockProvider>,
    ) -> Self {
        Self {
            worker_tracker,
            lock_provider,
        }
    }
}

#[async_trait]
impl Service for PurgeDeadWorkers {
    async fn call(&mut self) -> Duration {
        if let Ok(lock) = self.lock_provider.get_lock("purge_workers").await
            && lock.acquire().await
        {
            if let Ok(workers) = self.worker_tracker.get_workers().await {
                let now = Timestamp::now();
                let _ = self
                    .worker_tracker
                    .purge_workers(
                        workers
                            .into_iter()
                            .filter(|(_, ttl)| ttl < &now)
                            .map(|(wid, _)| wid)
                            .collect(),
                    )
                    .await;
            }
            lock.release().await;
        }
        WORKER_TTL.as_duration()
    }
}

pub(crate) struct RestoreAbandonedTasks {
    task_backend: Arc<dyn TaskBackend>,
    worker_tracker: Arc<dyn WorkerTracker>,
    lock_provider: Arc<dyn LockProvider>,
}

impl RestoreAbandonedTasks {
    pub fn new(
        task_backend: Arc<dyn TaskBackend>,
        worker_tracker: Arc<dyn WorkerTracker>,
        lock_provider: Arc<dyn LockProvider>,
    ) -> Self {
        Self {
            task_backend,
            worker_tracker,
            lock_provider,
        }
    }
}

#[async_trait]
impl Service for RestoreAbandonedTasks {
    async fn call(&mut self) -> Duration {
        if let Ok(lock) = self.lock_provider.get_lock("restore_abandoned_tasks").await
            && lock.acquire().await
        {
            if let Ok(stage_info_list) = self.task_backend.get_stage_info(100).await {
                let now = Timestamp::now();
                if let Ok(workers) = self.worker_tracker.get_workers().await {
                    let active_worker_ids: std::collections::HashSet<_> = workers
                        .into_iter()
                        .filter(|(_, ttl)| ttl >= &now)
                        .map(|(wid, _)| wid)
                        .collect();

                    for stage_info in stage_info_list {
                        if !active_worker_ids.contains(&stage_info.worker_id) {
                            tracing::info!("restore task `{}`", stage_info.task_id);
                            let _ = self.task_backend.restore(stage_info).await;
                        }
                    }
                }
            }
            lock.release().await;
        }
        Duration::from_secs_f64(15.0)
    }
}

pub(crate) struct DiscardDisposableTasks {
    task_backend: Arc<dyn TaskBackend>,
    lock_provider: Arc<dyn LockProvider>,
}

impl DiscardDisposableTasks {
    pub fn new(task_backend: Arc<dyn TaskBackend>, lock_provider: Arc<dyn LockProvider>) -> Self {
        Self {
            task_backend,
            lock_provider,
        }
    }
}

#[async_trait]
impl Service for DiscardDisposableTasks {
    async fn call(&mut self) -> Duration {
        if let Ok(lock) = self
            .lock_provider
            .get_lock("discard_disposable_tasks")
            .await
            && lock.acquire().await
        {
            if let Ok(task_ids) = self.task_backend.get_disposable_task_ids(100).await {
                let count = task_ids.len();
                if count > 0 {
                    let _ = self.task_backend.discard_tasks(&task_ids).await;
                    tracing::info!(
                        "discard {} tasks ({})",
                        count,
                        task_ids
                            .into_iter()
                            .map(|id| id.into())
                            .collect::<Vec<String>>()
                            .join(", ")
                    );
                }
                lock.release().await;

                if count == 100 {
                    return Duration::from_secs(0);
                }
            } else {
                lock.release().await;
            }
        }
        Duration::from_secs(60)
    }
}
