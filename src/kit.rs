use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;

use crate::backend::{
    BackendError, ControlEvent, EventBridge, LockProvider, SendEventError, TaskBackend,
    WorkerTracker,
};
use crate::result::TaskResult;
use crate::runtime::{Runtime, RuntimeError};
use crate::scheduler::{ScheduleEntry, SchedulerName};
use crate::task::{Task, TaskGroup, TaskId, TaskRecord, TaskRegistry};
use crate::util::{Timestamp, Ttl};

/// How long a group waits before polling the backend again after finding it empty
///
/// Each group polls from a single fetch loop, so this is the whole polling load a
/// group puts on the backend regardless of how many tasks it runs concurrently.
#[derive(Debug, Clone, Default)]
pub enum PollingInterval {
    /// Use the built-in default for every group
    #[default]
    Auto,
    /// Use the same polling interval for all groups
    Fixed(Duration),
    /// Use per-group polling intervals
    PerGroup(HashMap<TaskGroup, Duration>),
}

/// Errors that can occur when initiating a task
#[derive(Error, Debug)]
pub enum InitiateTaskError {
    /// Failed to serialize the task input
    #[error("Failed to encode task input")]
    Encode,

    /// Backend error occurred during task queueing
    #[error(transparent)]
    Backend(#[from] BackendError),
}

/// Main interface for task queue operations
///
/// The `Kit` provides methods to:
/// - Initiate tasks for execution
/// - Retrieve task results
/// - Start workers and schedulers
/// - Send control events to workers
///
/// This is the primary entry point for interacting with the task queue system.
pub struct Kit {
    task_backend: Arc<dyn TaskBackend>,
    worker_tracker: Arc<dyn WorkerTracker>,
    lock_provider: Arc<dyn LockProvider>,
    bridge: Arc<dyn EventBridge>,
    registry: Arc<TaskRegistry>,
}

impl Kit {
    /// Create a new Kit instance
    ///
    /// # Arguments
    ///
    /// * `task_backend` - Backend for task storage and queue management
    /// * `worker_tracker` - Backend for worker lifecycle tracking
    /// * `lock_provider` - Provider for distributed locks
    /// * `bridge` - Event bridge for control messaging
    /// * `registry` - Registry of task handlers
    pub fn new(
        task_backend: Arc<dyn TaskBackend>,
        worker_tracker: Arc<dyn WorkerTracker>,
        lock_provider: Arc<dyn LockProvider>,
        bridge: Arc<dyn EventBridge>,
        registry: Arc<TaskRegistry>,
    ) -> Self {
        Self {
            task_backend,
            worker_tracker,
            lock_provider,
            bridge,
            registry,
        }
    }

    /// Start the task queue runtime with workers and schedulers
    ///
    /// This method starts workers for processing tasks and optionally
    /// starts helper services (scheduler, dead worker cleanup, result cleanup).
    ///
    /// # Arguments
    ///
    /// * `concurrency_per_group` - Maximum number of tasks a group runs at once.
    ///   Each group claims work from one fetch loop, so raising this raises how much
    ///   runs in parallel without adding polling traffic.
    /// * `schedule_entries` - Schedule entries for periodic task scheduling
    /// * `tzinfo` - Wall clock schedule entries are read in unless they carry
    ///   their own (defaults to the local timezone)
    /// * `polling_interval` - Custom polling interval per task group (defaults to 1s)
    /// * `helper_services` - Whether to start scheduler and cleanup services
    ///
    /// # Returns
    ///
    /// This method blocks until the runtime is shutdown via a shutdown event.
    pub async fn start(
        &self,
        concurrency_per_group: HashMap<TaskGroup, usize>,
        schedule_entries: HashMap<SchedulerName, Vec<ScheduleEntry>>,
        tzinfo: Option<chrono::FixedOffset>,
        polling_interval: PollingInterval,
        helper_services: bool,
    ) -> Result<(), RuntimeError> {
        let runtime = Runtime::new(
            concurrency_per_group,
            self.task_backend.clone(),
            self.worker_tracker.clone(),
            self.lock_provider.clone(),
            self.bridge.clone(),
            self.registry.clone(),
            schedule_entries,
            tzinfo,
            polling_interval,
            helper_services,
        );

        runtime.run().await
    }

    /// Queue a task for execution with typed input
    ///
    /// Creates a task record from the typed input and adds it to the queue.
    /// Returns a `TaskResult` handle that can be used to retrieve the result.
    ///
    /// # Type Parameters
    ///
    /// * `T` - The task type implementing the `Task` trait
    ///
    /// # Arguments
    ///
    /// * `input` - The typed input for the task
    /// * `due` - When the task should be executed (defaults to now)
    /// * `ttl` - How long to keep the result after completion (defaults to 7 days)
    ///
    /// # Returns
    ///
    /// Returns a `TaskResult<T>` that can be used to wait for and retrieve
    /// the task's output.
    ///
    /// # Errors
    ///
    /// - `InitiateTaskError::Encode` - Failed to serialize the task input
    /// - `InitiateTaskError::Backend` - Backend error occurred during queueing
    pub async fn initiate_task<T: Task + 'static>(
        &self,
        input: T::Input,
        due: Option<impl Into<Timestamp>>,
        ttl: Option<impl Into<Ttl>>,
    ) -> Result<TaskResult<T>, InitiateTaskError> {
        let record = T::prepare(input, due, ttl).map_err(|_| InitiateTaskError::Encode)?;
        let task_id = record.info.id.clone();
        self.task_backend.put_tasks(vec![record]).await?;
        Ok(TaskResult::new(self.task_backend.clone(), task_id))
    }

    /// Queue multiple task records at once
    ///
    /// This is useful for bulk task submission when you have pre-prepared
    /// `TaskRecord` instances.
    ///
    /// # Arguments
    ///
    /// * `tasks` - Vector of task records to queue
    pub async fn initiate_tasks(&self, tasks: Vec<TaskRecord>) -> Result<(), BackendError> {
        self.task_backend.put_tasks(tasks).await
    }

    /// Get a typed result handle for a task by ID
    ///
    /// Creates a `TaskResult` handle for an already-queued task, allowing you
    /// to wait for and retrieve its output.
    ///
    /// # Type Parameters
    ///
    /// * `T` - The task type implementing the `Task` trait
    ///
    /// # Arguments
    ///
    /// * `task_id` - The ID of the task to get results for
    pub fn get_result<T: Task + 'static>(&self, task_id: impl Into<TaskId>) -> TaskResult<T> {
        TaskResult::new(self.task_backend.clone(), task_id.into())
    }

    /// Send a shutdown event to workers
    ///
    /// Requests workers to shutdown gracefully after completing their
    /// current tasks.
    ///
    /// # Arguments
    ///
    /// * `groups` - If `None`, all workers shutdown. If specified, only
    ///   workers processing these groups shutdown.
    pub async fn send_shutdown_event(
        &self,
        groups: Option<Vec<TaskGroup>>,
    ) -> std::result::Result<(), SendEventError> {
        self.bridge
            .send_event(&ControlEvent::Shutdown { groups })
            .await
    }

    /// Send a pause event to workers
    ///
    /// Requests workers to pause processing tasks. Workers will stop
    /// picking up new tasks but continue running.
    ///
    /// # Arguments
    ///
    /// * `groups` - If `None`, all task processing pauses. If specified,
    ///   only processing for these groups pauses.
    pub async fn send_pause_event(
        &self,
        groups: Option<Vec<TaskGroup>>,
    ) -> std::result::Result<(), SendEventError> {
        self.bridge
            .send_event(&ControlEvent::Pause { groups })
            .await
    }

    /// Send a resume event to workers
    ///
    /// Requests workers to resume processing tasks after being paused.
    ///
    /// # Arguments
    ///
    /// * `groups` - If `None`, all task processing resumes. If specified,
    ///   only processing for these groups resumes.
    pub async fn send_resume_event(
        &self,
        groups: Option<Vec<TaskGroup>>,
    ) -> std::result::Result<(), SendEventError> {
        self.bridge
            .send_event(&ControlEvent::Resume { groups })
            .await
    }
}
