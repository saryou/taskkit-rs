use bytes::Bytes;
use std::marker::PhantomData;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use thiserror::Error;
use tokio::time::{Instant, sleep};

use crate::backend::{GetResultError, TaskBackend};
use crate::task::{Task, TaskId};

/// Errors that can occur when retrieving task results
#[derive(Error, Debug)]
pub enum ResultGetError {
    /// Timeout occurred while waiting for the result
    #[error("Result get timed out")]
    TimedOut,

    /// Result retrieval was prevented (usually to avoid deadlock)
    #[error("Result get prevented: {0}")]
    Prevented(String),

    /// Failed to deserialize the result data
    #[error("Invalid data: {0}")]
    InvalidData(String),

    /// Task does not exist in the backend
    #[error("Task not found")]
    NotFound,

    /// Task execution failed with an error
    #[error("Task failed: {message}")]
    Failed { message: String },

    /// Backend error occurred while retrieving the result
    #[error(transparent)]
    Backend(#[from] crate::backend::BackendError),
}

tokio::task_local! {
    static PREVENT_TO_WAIT_RESULT: (bool, String);
}

/// Execute a future with result waiting prevention
///
/// This is used internally by workers to prevent deadlocks when a task
/// tries to wait for another task's result. The prevention can be bypassed
/// by passing `avoid_assertion=true` to `TaskResult::get`.
pub async fn prevent_to_wait_result<F, T, E>(reason: String, f: F) -> Result<T, E>
where
    F: std::future::Future<Output = Result<T, E>>,
{
    PREVENT_TO_WAIT_RESULT.scope((true, reason), f).await
}

fn is_prevented() -> Option<String> {
    PREVENT_TO_WAIT_RESULT
        .try_with(|(prevented, reason)| {
            if *prevented {
                Some(reason.clone())
            } else {
                None
            }
        })
        .ok()
        .flatten()
}

/// Type-safe handle for retrieving task results
///
/// A `TaskResult<T>` represents a pending or completed task result.
/// It can be used to wait for task completion and retrieve the typed output.
///
/// # Type Parameters
///
/// * `T` - The task type implementing the `Task` trait
///
/// # Caching
///
/// Once a result is successfully retrieved, it is cached internally to avoid
/// repeated backend calls.
pub struct TaskResult<T: Task> {
    backend: Arc<dyn TaskBackend>,
    task_id: TaskId,
    cached_bytes: Mutex<Option<Bytes>>,
    _marker: PhantomData<T>,
}

impl<T: Task> TaskResult<T> {
    /// Create a new result handle for a task
    ///
    /// # Arguments
    ///
    /// * `backend` - Backend for retrieving the result
    /// * `task_id` - ID of the task to get results for
    pub fn new(backend: Arc<dyn TaskBackend>, task_id: TaskId) -> Self {
        Self {
            backend,
            task_id,
            cached_bytes: Mutex::new(None),
            _marker: PhantomData,
        }
    }

    /// Create a new result handle with pre-cached result bytes
    ///
    /// This is useful when you already have the serialized result data
    /// and want to avoid a backend fetch.
    ///
    /// # Arguments
    ///
    /// * `backend` - Backend for retrieving the result (if needed)
    /// * `task_id` - ID of the task
    /// * `bytes` - Pre-cached result bytes
    pub fn new_with_bytes(backend: Arc<dyn TaskBackend>, task_id: TaskId, bytes: Bytes) -> Self {
        Self {
            backend,
            task_id,
            cached_bytes: Mutex::new(Some(bytes)),
            _marker: PhantomData,
        }
    }

    /// Wait for and retrieve the task result
    ///
    /// This method polls the backend until the task completes or times out.
    /// Polling starts immediately and backs off linearly, up to one second apart.
    ///
    /// # Arguments
    ///
    /// * `timeout` - Maximum time to wait for the result. `None` means wait indefinitely.
    /// * `avoid_assertion` - If `true`, skip the deadlock prevention check.
    ///   This should only be set to `true` when you're certain there's no risk
    ///   of deadlock (e.g., waiting for tasks from a different group).
    ///
    /// # Returns
    ///
    /// Returns the typed task output on success.
    ///
    /// # Errors
    ///
    /// - `ResultGetError::TimedOut` - Timeout occurred before task completed
    /// - `ResultGetError::Prevented` - Prevention check failed (potential deadlock)
    /// - `ResultGetError::NotFound` - Task does not exist
    /// - `ResultGetError::Failed` - Task execution failed
    /// - `ResultGetError::InvalidData` - Failed to deserialize the result
    /// - `ResultGetError::Backend` - Backend error occurred
    pub async fn get(
        &self,
        timeout: Option<Duration>,
        avoid_assertion: bool,
    ) -> Result<T::Output, ResultGetError> {
        if !avoid_assertion && let Some(reason) = is_prevented() {
            return Err(ResultGetError::Prevented(reason));
        }

        {
            let cache = self.cached_bytes.lock().unwrap();
            if let Some(bytes) = cache.as_ref() {
                let result = T::decode_output(bytes)
                    .map_err(|e| ResultGetError::InvalidData(format!("{}", e)))?;
                return Ok(result);
            }
        }

        if let Some(result) = self.try_get_result().await? {
            return Ok(result);
        }

        let start = Instant::now();
        let mut i = 0;
        loop {
            if let Some(timeout) = timeout
                && start.elapsed() >= timeout
            {
                return Err(ResultGetError::TimedOut);
            }

            sleep(Duration::from_millis((i * 100).min(1000))).await;
            i += 1;

            if let Some(result) = self.try_get_result().await? {
                return Ok(result);
            }
        }
    }

    /// Try to get the result once, returns Some if available, None if not ready yet
    async fn try_get_result(&self) -> Result<Option<T::Output>, ResultGetError> {
        match self.backend.get_result(&self.task_id).await {
            Ok((_record, result_bytes)) => {
                let result = T::decode_output(&result_bytes)
                    .map_err(|e| ResultGetError::InvalidData(format!("{}", e)))?;
                *self.cached_bytes.lock().unwrap() = Some(result_bytes);
                Ok(Some(result))
            }
            Err(GetResultError::NoResult(_)) => Ok(None),
            Err(GetResultError::NotFound) => Err(ResultGetError::NotFound),
            Err(GetResultError::Failed { message, .. }) => Err(ResultGetError::Failed { message }),
            Err(GetResultError::Backend(e)) => Err(ResultGetError::Backend(e)),
        }
    }
}

impl<T: Task> Clone for TaskResult<T> {
    fn clone(&self) -> Self {
        Self {
            backend: self.backend.clone(),
            task_id: self.task_id.clone(),
            cached_bytes: Mutex::new(None),
            _marker: PhantomData,
        }
    }
}
