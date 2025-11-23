use bytes::Bytes;
use futures_util::FutureExt;
use serde::{Deserialize, Serialize};
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

use crate::backend::TaskBackend;
use crate::result::prevent_to_wait_result;
use crate::task::{TaskError, TaskGroup, TaskInfo, TaskRecord, TaskRegistry};
use crate::util::string_newtype;

string_newtype! {
    /// Unique identifier for a worker instance
    WorkerId
}

const REASON_TO_PREVENT_WAIT_RESULT: &str = "Waiting for another task's result from inside a task handler can deadlock. \
     If the wait is known to be safe, pass avoid_assertion = true to TaskResult::get.";

fn describe_panic(panic: &(dyn std::any::Any + Send)) -> String {
    let detail = panic
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| panic.downcast_ref::<String>().map(|s| s.as_str()))
        .unwrap_or("unknown panic");
    format!("task handler panicked: {detail}")
}

pub(crate) struct Worker {
    id: WorkerId,
    pub group: TaskGroup,
    task_backend: Arc<dyn TaskBackend>,
    registry: Arc<TaskRegistry>,
}

impl Worker {
    pub fn new(
        group: TaskGroup,
        task_backend: Arc<dyn TaskBackend>,
        registry: Arc<TaskRegistry>,
    ) -> Self {
        let id = WorkerId::new(format!("wk_{}_{}", group.as_str(), Uuid::new_v4().simple()));

        Self {
            id,
            group,
            task_backend,
            registry,
        }
    }

    pub fn get_id(&self) -> &WorkerId {
        &self.id
    }

    pub async fn handle_task(&self, record: TaskRecord) {
        let TaskRecord { info, data } = record;

        tracing::info!("[{}] handle task ({}: {})", self.id, info.id, info.name);

        let executor = match self.registry.get(info.group.as_str(), info.name.as_str()) {
            Some(e) => e,
            None => {
                tracing::error!(
                    "[{}] No executor found for task type: {}/{}",
                    self.id,
                    info.group,
                    info.name
                );
                let message = format!(
                    "No executor found for task type: {}/{}",
                    info.group, info.name
                );
                let _ = self
                    .task_backend
                    .fail(TaskRecord::new(info, data), Bytes::from(message))
                    .await;
                return;
            }
        };

        // A panicking handler must not take the runner down with it: the task would
        // stay on the stage while this worker keeps heartbeating as alive, so nothing
        // would ever restore it. Treat it as a fatal task error instead.
        let result = AssertUnwindSafe(prevent_to_wait_result(
            REASON_TO_PREVENT_WAIT_RESULT.to_string(),
            async { executor.execute(&info, &data).await },
        ))
        .catch_unwind()
        .await
        .unwrap_or_else(|panic| Err(TaskError::fatal_reason(describe_panic(panic.as_ref()))));

        match result {
            Ok(output_bytes) => {
                let _ = self
                    .task_backend
                    .succeed(TaskRecord::new(info, data), output_bytes)
                    .await;
            }
            Err(TaskError::Discard { reason }) => {
                tracing::info!(
                    "[{}] task was discarded ({}: {}): {:?}",
                    self.id,
                    info.id,
                    info.name,
                    reason
                );
                self.discard_task(&info).await;
            }
            Err(TaskError::Retry {
                reason,
                retry_after,
            }) => {
                tracing::info!(
                    "[{}] retry task ({}: {}) after {:?}: {:?}",
                    self.id,
                    info.id,
                    info.name,
                    retry_after,
                    reason
                );
                self.retry(&info, data, retry_after).await;
            }
            Err(TaskError::Fatal { reason }) => {
                tracing::info!(
                    "[{}] task was failed ({}: {}): {:?}",
                    self.id,
                    info.id,
                    info.name,
                    reason
                );
                let _ = self
                    .task_backend
                    .fail(
                        TaskRecord::new(info, data),
                        Bytes::from(reason.unwrap_or_else(|| "Fatal error".to_string())),
                    )
                    .await;
            }
        }
    }

    async fn discard_task(&self, info: &TaskInfo) {
        let _ = self
            .task_backend
            .discard_tasks(std::slice::from_ref(&info.id))
            .await;
    }

    async fn retry(&self, info: &TaskInfo, data: Bytes, interval: Duration) {
        let _ = self
            .task_backend
            .retry_task(TaskRecord::new(info.clone_for_retry(interval), data))
            .await;
    }

    /// Claim up to `limit` tasks that are due for this worker's group.
    ///
    /// Backend failures are reported as an empty batch: the caller cannot act on
    /// them beyond backing off, which is what an empty batch already means.
    pub async fn assign(&self, limit: usize) -> Vec<TaskRecord> {
        match self
            .task_backend
            .assign_tasks(self.group.as_str(), self.id.as_str(), limit)
            .await
        {
            Ok(records) => records,
            Err(e) => {
                tracing::error!("[{}] failed to assign tasks: {:?}", self.id, e);
                Vec::new()
            }
        }
    }
}
