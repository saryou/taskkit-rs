use async_trait::async_trait;
use bytes::Bytes;
use futures_util::stream::Stream;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::pin::Pin;
use thiserror::Error;

use crate::scheduler::SchedulerName;
use crate::stage::StageInfo;
use crate::task::{TaskGroup, TaskId, TaskRecord};
use crate::util::Timestamp;
use crate::worker::WorkerId;

/// Errors that can occur during backend operations
///
/// These errors represent failures in the underlying storage or
/// communication layer of the task queue system.
#[derive(Error, Debug)]
pub enum BackendError {
    /// Failed to serialize data for storage
    #[error("Serialization error: {0}")]
    Serialization(String),

    /// Failed to deserialize data from storage
    #[error("Deserialization error: {0}")]
    Deserialization(String),

    /// Backend service is unavailable (connection errors, timeouts, etc.)
    #[error("Backend unavailable: {0}")]
    Unavailable(String),

    /// Backend operation failed
    #[error("Operation failed: {0}")]
    OperationFailed(String),

    /// Unknown or unexpected backend error
    #[error("Unknown backend error: {0}")]
    Unknown(String),
}

/// Errors that can occur when retrieving task results
///
/// These errors provide detailed context about why a result cannot be retrieved,
/// including the task record when available.
#[derive(Error, Debug)]
pub enum GetResultError {
    /// Task does not exist in the backend
    #[error("Task not found")]
    NotFound,

    /// Task exists but has not completed yet
    #[error("No result available for task")]
    NoResult(TaskRecord),

    /// Task execution failed with an error message
    #[error("Task failed: {message}")]
    Failed {
        /// The task record containing metadata and input data
        record: TaskRecord,
        /// Error message from the failed task
        message: String,
    },

    /// Backend error occurred while retrieving the result
    #[error(transparent)]
    Backend(#[from] BackendError),
}

/// Task storage and queue management operations
///
/// This trait defines the core operations for managing tasks in a distributed queue.
/// Implementations handle task persistence, queueing, assignment to workers, and
/// result storage.
///
/// # Atomicity
///
/// Some operations must be atomic to ensure data consistency:
/// - `assign_task`: Atomically moves a task from queue to stage
/// - `succeed`/`fail`: Atomically stores result and removes from stage
/// - `persist_scheduler_state_and_put_tasks`: Atomically updates scheduler state and queues tasks
#[async_trait]
pub trait TaskBackend: Send + Sync {
    /// Add tasks to the queue for execution
    ///
    /// Tasks are queued based on their `due` timestamp and will be available
    /// for workers to pick up when the time comes.
    async fn put_tasks(&self, tasks: Vec<TaskRecord>) -> Result<(), BackendError>;

    /// Retrieve tasks currently in the queue for a specific group
    ///
    /// # Arguments
    ///
    /// * `group` - Task group identifier
    /// * `limit` - Maximum number of tasks to retrieve
    async fn get_queued_tasks(
        &self,
        group: &str,
        limit: usize,
    ) -> Result<Vec<TaskRecord>, BackendError>;

    /// Atomically assign up to `limit` due tasks from the queue to a worker
    ///
    /// This operation moves tasks from the queue to the stage, making them
    /// unavailable to other workers. Tasks are taken in due order, earliest
    /// first, and tasks that are not due yet are left in the queue.
    ///
    /// Returning fewer records than `limit` tells the caller the queue is
    /// drained, so implementations must not pad the result.
    ///
    /// # Arguments
    ///
    /// * `group` - Task group to assign from
    /// * `worker_id` - ID of the worker claiming the tasks
    /// * `limit` - Maximum number of tasks to assign
    async fn assign_tasks(
        &self,
        group: &str,
        worker_id: &str,
        limit: usize,
    ) -> Result<Vec<TaskRecord>, BackendError>;

    /// Atomically assign a single due task from the queue to a worker
    ///
    /// Returns `None` if the queue is empty or no task is due yet.
    async fn assign_task(
        &self,
        group: &str,
        worker_id: &str,
    ) -> Result<Option<TaskRecord>, BackendError> {
        Ok(self
            .assign_tasks(group, worker_id, 1)
            .await?
            .into_iter()
            .next())
    }

    /// Look up multiple tasks by their IDs
    ///
    /// Returns a vector with the same length as `task_ids`, where each element
    /// is `Some(TaskRecord)` if found, or `None` if not found.
    async fn lookup_tasks(
        &self,
        task_ids: &[TaskId],
    ) -> Result<Vec<Option<TaskRecord>>, BackendError>;

    /// Move a task from the stage back to the queue for retry
    ///
    /// This is called when a task execution fails with a retry error.
    /// The task is removed from the stage and re-queued with updated metadata.
    async fn retry_task(&self, record: TaskRecord) -> Result<(), BackendError>;

    /// Permanently remove tasks from the system
    ///
    /// Discards tasks from all queues, stages, and result storage.
    async fn discard_tasks(&self, task_ids: &[TaskId]) -> Result<(), BackendError>;

    /// Mark a task as successfully completed and store its result
    ///
    /// Atomically removes the task from the stage and stores the result.
    /// If the task data is missing in the backend, it should be saved.
    ///
    /// # Arguments
    ///
    /// * `record` - Task record containing metadata and input data
    /// * `result` - Serialized task output
    async fn succeed(&self, record: TaskRecord, result: Bytes) -> Result<(), BackendError>;

    /// Mark a task as failed and store the error message
    ///
    /// Atomically removes the task from the stage and stores the error.
    /// If the task data is missing in the backend, it should be saved.
    ///
    /// # Arguments
    ///
    /// * `record` - Task record containing metadata and input data
    /// * `error` - Serialized error message
    async fn fail(&self, record: TaskRecord, error: Bytes) -> Result<(), BackendError>;

    /// Retrieve the result of a completed task
    ///
    /// # Arguments
    ///
    /// * `task_id` - ID of the task to retrieve
    ///
    /// # Returns
    ///
    /// Returns a tuple of `(TaskRecord, Bytes)` containing the task metadata
    /// and serialized result on success.
    ///
    /// # Errors
    ///
    /// - `GetResultError::NotFound` - Task does not exist
    /// - `GetResultError::Failed` - Task execution failed (includes task record and error message)
    /// - `GetResultError::NoResult` - Task exists but hasn't completed yet (includes task record)
    /// - `GetResultError::Backend` - Backend error occurred
    async fn get_result(&self, task_id: &TaskId) -> Result<(TaskRecord, Bytes), GetResultError>;

    /// Get IDs of tasks that completed within a time range
    ///
    /// Returns task IDs for both successful and failed tasks.
    ///
    /// # Arguments
    ///
    /// * `since` - Start of time range (inclusive), or `None` for no lower bound
    /// * `until` - End of time range (inclusive), or `None` for current time
    /// * `limit` - Maximum number of IDs to return
    async fn get_done_task_ids(
        &self,
        since: Option<Timestamp>,
        until: Option<Timestamp>,
        limit: usize,
    ) -> Result<Vec<TaskId>, BackendError>;

    /// Get IDs of tasks that are ready for cleanup
    ///
    /// Returns tasks that have exceeded their TTL (time-to-live) since
    /// completion. These tasks can be safely removed from the system.
    ///
    /// # Arguments
    ///
    /// * `limit` - Maximum number of task IDs to return
    async fn get_disposable_task_ids(&self, limit: usize) -> Result<Vec<TaskId>, BackendError>;

    /// Get information about tasks currently being executed
    ///
    /// Returns metadata about tasks on the stage (assigned to workers).
    ///
    /// # Arguments
    ///
    /// * `limit` - Maximum number of stage entries to return
    async fn get_stage_info(&self, limit: usize) -> Result<Vec<StageInfo>, BackendError>;

    /// Restore a task from the stage back to the queue
    ///
    /// Used to recover tasks from dead workers or when manually
    /// re-queueing tasks.
    async fn restore(&self, info: StageInfo) -> Result<(), BackendError>;

    /// Atomically update scheduler state and queue scheduled tasks
    ///
    /// This operation must be atomic to ensure scheduler consistency.
    ///
    /// # Arguments
    ///
    /// * `name` - Scheduler identifier
    /// * `data` - Serialized scheduler state
    /// * `tasks` - Tasks to queue
    async fn persist_scheduler_state_and_put_tasks(
        &self,
        name: &SchedulerName,
        data: Bytes,
        tasks: Vec<TaskRecord>,
    ) -> Result<(), BackendError>;

    /// Retrieve the persisted state for a scheduler
    ///
    /// Returns `None` if the scheduler has no saved state.
    ///
    /// # Arguments
    ///
    /// * `name` - Scheduler identifier
    async fn get_scheduler_state(
        &self,
        name: &SchedulerName,
    ) -> Result<Option<Vec<u8>>, BackendError>;
}

/// Worker lifecycle tracking operations
///
/// This trait manages worker registration and health tracking in a distributed
/// environment. Workers periodically update their TTL to indicate they are alive,
/// and expired workers can be detected and purged.
#[async_trait]
pub trait WorkerTracker: Send + Sync {
    /// Update worker lifetimes to keep them active
    ///
    /// Workers should periodically call this method to refresh their TTL
    /// and prevent being marked as dead.
    ///
    /// # Arguments
    ///
    /// * `worker_ids` - Set of worker IDs to update
    /// * `expires_at` - New expiration timestamp for these workers
    async fn set_worker_ttl(
        &self,
        worker_ids: HashSet<WorkerId>,
        expires_at: Timestamp,
    ) -> Result<(), BackendError>;

    /// Get all registered workers with their expiration timestamps
    ///
    /// Returns a list of (worker_id, expires_at) pairs for all known workers.
    /// This can be used to identify expired workers that need cleanup.
    async fn get_workers(&self) -> Result<Vec<(WorkerId, Timestamp)>, BackendError>;

    /// Remove workers from the tracking system
    ///
    /// Called to clean up workers that have been identified as dead
    /// or are being explicitly shut down.
    ///
    /// # Arguments
    ///
    /// * `worker_ids` - Set of worker IDs to remove
    async fn purge_workers(&self, worker_ids: HashSet<WorkerId>) -> Result<(), BackendError>;
}

/// A distributed lock that reduces duplicated work between processes
///
/// # A lock can be lost while it is held
///
/// Implementations backed by a key with a time-to-live hand the lock to someone
/// else once that time elapses, whether or not the holder has finished. There is
/// no way for a holder to notice this.
///
/// Guarded sections must therefore stay correct when two processes run them at the
/// same time: treat a lock as an optimisation that usually avoids repeated work,
/// never as the reason an operation is safe. Every operation taskkit itself guards
/// this way is idempotent, so the worst outcome of a lost lock is wasted effort.
#[async_trait]
pub trait Lock: Send + Sync {
    /// Attempt to acquire the lock
    ///
    /// Returns `true` if the lock was acquired. `false` means the caller should
    /// skip the guarded work for now; it does not distinguish another holder from
    /// a backend that could not be reached.
    async fn acquire(&self) -> bool;

    /// Release the lock
    ///
    /// Best-effort: a failure to reach the backend leaves the lock to expire or to
    /// be freed when the connection drops, so callers cannot treat a return as
    /// proof that the lock is free.
    async fn release(&self);
}

/// Provides distributed locks for coordinating access to shared resources
///
/// This trait enables creation of named locks that can be used to ensure
/// exclusive access to resources across multiple processes or workers.
#[async_trait]
pub trait LockProvider: Send + Sync {
    /// Create a lock for the specified target resource
    ///
    /// Returns a lock object that can be used to acquire and release
    /// exclusive access to the named resource.
    ///
    /// # Arguments
    ///
    /// * `target` - Name identifying the resource to lock
    async fn get_lock(&self, target: &str) -> Result<Box<dyn Lock>, BackendError>;
}

/// Control events for managing worker and task execution lifecycle
///
/// These events allow remote control of workers and task processing,
/// enabling coordinated shutdown, pausing, and resumption of work.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "name", rename_all = "lowercase")]
pub enum ControlEvent {
    /// Request workers to shutdown gracefully
    ///
    /// If `groups` is `None`, all workers should shutdown.
    /// If specified, only workers processing those groups should shutdown.
    Shutdown { groups: Option<Vec<TaskGroup>> },

    /// Request workers to pause processing tasks
    ///
    /// If `groups` is `None`, all task processing should pause.
    /// If specified, only processing for those groups should pause.
    Pause { groups: Option<Vec<TaskGroup>> },

    /// Request workers to resume processing tasks
    ///
    /// If `groups` is `None`, all task processing should resume.
    /// If specified, only processing for those groups should resume.
    Resume { groups: Option<Vec<TaskGroup>> },
}

impl ControlEvent {
    /// Get the task groups affected by this event
    pub fn groups(&self) -> &Option<Vec<TaskGroup>> {
        match self {
            ControlEvent::Shutdown { groups } => groups,
            ControlEvent::Pause { groups } => groups,
            ControlEvent::Resume { groups } => groups,
        }
    }
}

/// Errors that can occur when receiving control events
#[derive(Error, Debug)]
pub enum ReceiveEventsError {
    /// Failed to receive events from the backend
    #[error("Failed to receive events: {0}")]
    ReceiveFailed(String),

    /// Connection to event source was lost or unavailable
    #[error("Connection error: {0}")]
    ConnectionError(String),
}

/// Errors that can occur when sending control events
#[derive(Error, Debug)]
pub enum SendEventError {
    /// Failed to send the event to the backend
    #[error("Failed to send event: {0}")]
    SendFailed(String),

    /// Connection to event destination was lost or unavailable
    #[error("Connection error: {0}")]
    ConnectionError(String),

    /// Failed to serialize the event for transmission
    #[error("Serialization error: {0}")]
    SerializationError(String),
}

/// Event messaging for distributed control of workers and tasks
///
/// This trait provides pub/sub-style messaging for sending control events
/// to workers and receiving events as a stream.
#[async_trait]
pub trait EventBridge: Send + Sync {
    /// Subscribe to control events as a stream
    ///
    /// Returns a stream that yields control events as they are received.
    /// Workers should listen to this stream to respond to control commands.
    ///
    /// # Returns
    ///
    /// A stream of `ControlEvent` items that continues until the connection
    /// is closed or an error occurs.
    async fn receive_events(
        &self,
    ) -> Result<Pin<Box<dyn Stream<Item = ControlEvent> + Send>>, ReceiveEventsError>;

    /// Publish a control event to all listeners
    ///
    /// Sends an event that will be received by all workers subscribed
    /// to the event stream.
    ///
    /// # Arguments
    ///
    /// * `event` - The control event to broadcast
    async fn send_event(&self, event: &ControlEvent) -> Result<(), SendEventError>;
}

/// Combined backend trait providing all distributed queue functionality
///
/// This trait combines all backend capabilities into a single interface:
/// - Task queue management (`TaskBackend`)
/// - Worker health tracking (`WorkerTracker`)
/// - Distributed locking (`LockProvider`)
/// - Control event messaging (`EventBridge`)
///
/// Implementations should provide all these features in a cohesive way,
/// typically backed by a single data store or service.
#[async_trait]
pub trait Backend: TaskBackend + WorkerTracker + LockProvider + EventBridge {
    /// Destroy all backend data
    ///
    /// This method removes all tasks, workers, locks, and other data
    /// associated with this backend. It is primarily used for testing
    /// and should be used with caution in production environments.
    ///
    /// # Warning
    ///
    /// This operation is destructive and irreversible.
    async fn destroy_all(&self) -> Result<(), BackendError>;
}
