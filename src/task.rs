use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

use crate::util::{Timestamp, Ttl, string_newtype};

string_newtype! {
    /// Unique identifier for a task
    TaskId
}

string_newtype! {
    /// Name of a task type
    TaskName
}

string_newtype! {
    /// Group identifier for organizing tasks
    TaskGroup
}

/// Namespace for ids derived from a schedule point. Fixed so that the derivation
/// stays stable across processes and releases.
const SCHEDULE_ID_NAMESPACE: Uuid = Uuid::from_u128(0x7a5b_4c21_9f38_4d6e_ae10_2c93_51b7_f0d4);

impl TaskId {
    pub fn generate() -> Self {
        Self(Uuid::new_v4().to_string())
    }

    /// Identifier derived from the schedule point that produced the task.
    ///
    /// Two schedulers that both act on the same point therefore submit the same id,
    /// and the backend keeps only the first — so the queue never gains a duplicate
    /// even though scheduling is not guarded by a lock that is guaranteed to hold.
    pub fn for_schedule_point(scheduler: &str, entry_key: &str, at: Timestamp) -> Self {
        let name = format!("{scheduler}\0{entry_key}\0{:.3}", at.as_secs_f64());
        Self(Uuid::new_v5(&SCHEDULE_ID_NAMESPACE, name.as_bytes()).to_string())
    }
}

/// Default time-to-live for task results (7 days)
pub const DEFAULT_TASK_TTL: Ttl = Ttl(Duration::from_secs(60 * 60 * 24 * 7));

/// Task metadata containing scheduling and lifecycle information
///
/// This structure is persisted to the backend and tracks all metadata
/// about a task including its identity, timing, and retry state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskInfo {
    pub id: TaskId,
    pub group: TaskGroup,
    pub name: TaskName,
    pub due: Timestamp,
    pub created: Timestamp,
    pub scheduled: Option<Timestamp>,
    pub retry_count: u32,
    pub ttl: Ttl,
}

impl TaskInfo {
    /// Create a new task with metadata
    pub fn init(
        group: impl Into<TaskGroup>,
        name: impl Into<TaskName>,
        due: Option<impl Into<Timestamp>>,
        scheduled: Option<impl Into<Timestamp>>,
        ttl: Ttl,
    ) -> Self {
        Self::init_with_id(TaskId::generate(), group, name, due, scheduled, ttl)
    }

    /// As [`TaskInfo::init`], but with a caller-chosen id.
    ///
    /// Reusing an id makes the submission idempotent, since backends keep the task
    /// they already know.
    pub fn init_with_id(
        id: TaskId,
        group: impl Into<TaskGroup>,
        name: impl Into<TaskName>,
        due: Option<impl Into<Timestamp>>,
        scheduled: Option<impl Into<Timestamp>>,
        ttl: Ttl,
    ) -> Self {
        let now = Timestamp::now();
        let due = due.map(Into::into).unwrap_or(now);
        let scheduled = scheduled.map(Into::into);

        Self {
            id,
            group: group.into(),
            name: name.into(),
            due,
            created: now,
            scheduled,
            retry_count: 0,
            ttl,
        }
    }

    /// Create a clone for retry with updated due time
    pub fn clone_for_retry(&self, interval: Duration) -> Self {
        let new_due =
            Timestamp::from_secs_f64(Timestamp::now().as_secs_f64() + interval.as_secs_f64());
        Self {
            id: self.id.clone(),
            group: self.group.clone(),
            name: self.name.clone(),
            due: new_due,
            created: self.created,
            scheduled: None,
            retry_count: self.retry_count + 1,
            ttl: self.ttl,
        }
    }
}

/// A task record combining metadata and input data
///
/// This represents a complete task ready to be queued or executed,
/// containing both the task metadata and the serialized input data.
#[derive(Debug, Clone)]
pub struct TaskRecord {
    /// Task metadata
    pub info: TaskInfo,
    /// Serialized input data
    pub data: Bytes,
}

impl TaskRecord {
    pub fn new(info: TaskInfo, data: Bytes) -> Self {
        Self { info, data }
    }
}

/// Errors that can occur during task execution
///
/// These errors control the task lifecycle:
/// - `Discard`: Remove the task without retrying
/// - `Retry`: Retry the task after a delay
/// - `Fatal`: Mark the task as failed permanently
#[derive(Debug, thiserror::Error)]
pub enum TaskError {
    /// Discard the task without retrying
    #[error("discard")]
    Discard { reason: Option<String> },

    /// Retry the task after a specified duration
    #[error("retry after {retry_after:?}")]
    Retry {
        reason: Option<String>,
        retry_after: Duration,
    },

    #[error("fatal")]
    Fatal { reason: Option<String> },
}

impl TaskError {
    pub fn discard() -> Self {
        TaskError::Discard { reason: None }
    }

    pub fn discard_reason(msg: impl Into<String>) -> Self {
        TaskError::Discard {
            reason: Some(msg.into()),
        }
    }

    pub fn retry(after: Duration) -> Self {
        TaskError::Retry {
            reason: None,
            retry_after: after,
        }
    }

    pub fn retry_reason(after: Duration, msg: impl Into<String>) -> Self {
        TaskError::Retry {
            reason: Some(msg.into()),
            retry_after: after,
        }
    }

    pub fn fatal() -> Self {
        TaskError::Fatal { reason: None }
    }

    pub fn fatal_reason(msg: impl Into<String>) -> Self {
        TaskError::Fatal {
            reason: Some(msg.into()),
        }
    }
}

/// Core trait for defining executable tasks
///
/// This trait provides the foundation for all task types in taskkit.
/// It defines the task's identity, input/output types, and execution logic,
/// as well as serialization methods for network transmission.
///
/// # Type Parameters
///
/// - `Input`: The input type for task execution (must be `Send + 'static`)
/// - `Output`: The return type from task execution (must be `Send + 'static`)
///
/// # Serialization
///
/// Tasks must implement encoding/decoding methods to serialize their
/// input and output for storage and transmission. For JSON serialization,
/// consider using [`JsonTask`] which provides automatic implementations.
///
/// # Examples
///
/// For most use cases, implement [`JsonTask`] instead of `Task` directly:
///
/// ```ignore
/// use taskkit::{JsonTask, TaskInfo, TaskError};
/// use async_trait::async_trait;
/// use serde::{Serialize, Deserialize};
///
/// struct MyTask;
///
/// #[derive(Serialize, Deserialize)]
/// struct MyInput { value: i32 }
///
/// #[derive(Serialize, Deserialize)]
/// struct MyOutput { result: i32 }
///
/// #[async_trait]
/// impl JsonTask for MyTask {
///     const GROUP: &'static str = "my_group";
///     const NAME: &'static str = "my_task";
///
///     type Input = MyInput;
///     type Output = MyOutput;
///
///     async fn handle(&self, info: &TaskInfo, input: Self::Input)
///         -> Result<Self::Output, TaskError>
///     {
///         Ok(MyOutput { result: input.value * 2 })
///     }
/// }
/// ```
#[async_trait]
pub trait Task: Send + Sync + 'static {
    /// Task group identifier for organizing related tasks
    const GROUP: &'static str;

    /// Unique name for this task type within its group
    const NAME: &'static str;

    /// Input type for this task
    type Input: Send + 'static;

    /// Output type returned by this task
    type Output: Send + 'static;

    /// Execute the task with the given input
    ///
    /// # Arguments
    ///
    /// * `info` - Task metadata including ID, timing, and retry count
    /// * `input` - Deserialized task input
    ///
    /// # Returns
    ///
    /// Returns the task output on success, or a `TaskError` to control
    /// the task lifecycle (retry, discard, or fatal failure).
    async fn handle(&self, info: &TaskInfo, input: Self::Input) -> Result<Self::Output, TaskError>;

    fn prepare(
        input: Self::Input,
        due: Option<impl Into<Timestamp>>,
        ttl: Option<impl Into<Ttl>>,
    ) -> Result<TaskRecord, TaskError> {
        Ok(TaskRecord::new(
            TaskInfo::init(
                Self::GROUP,
                Self::NAME,
                due,
                None::<f64>,
                ttl.map(Into::into).unwrap_or(DEFAULT_TASK_TTL),
            ),
            Self::encode_input(&input)?,
        ))
    }

    /// Serialize task input to bytes for storage
    ///
    /// This method is called when queuing a task to convert the input
    /// into a format suitable for network transmission and storage.
    fn encode_input(input: &Self::Input) -> Result<Bytes, TaskError>;

    /// Deserialize task input from bytes
    ///
    /// This method is called by workers to reconstruct the input
    /// from the stored byte representation.
    fn decode_input(input: &Bytes) -> Result<Self::Input, TaskError>;

    /// Serialize task output to bytes for storage
    ///
    /// This method is called after task completion to store the result.
    fn encode_output(output: &Self::Output) -> Result<Bytes, TaskError>;

    /// Deserialize task output from bytes
    ///
    /// This method is called when retrieving task results.
    fn decode_output(input: &Bytes) -> Result<Self::Output, TaskError>;
}

/// JSON-serialized task trait
///
/// A convenience trait for tasks that use JSON serialization for their
/// input and output. This automatically implements the [`Task`] trait
/// with JSON-based encoding/decoding.
///
/// # Benefits
///
/// - Automatic serialization using `serde_json`
/// - No need to manually implement `encode_input`, `decode_input`, etc.
/// - Type-safe with Rust's type system
///
/// # Requirements
///
/// Input and output types must implement `Serialize` and `DeserializeOwned`.
///
/// # Examples
///
/// ```ignore
/// use taskkit::{JsonTask, TaskInfo, TaskError};
/// use async_trait::async_trait;
/// use serde::{Serialize, Deserialize};
///
/// struct AddTask;
///
/// #[derive(Serialize, Deserialize)]
/// struct AddInput {
///     a: i32,
///     b: i32,
/// }
///
/// #[derive(Serialize, Deserialize)]
/// struct AddOutput {
///     sum: i32,
/// }
///
/// #[async_trait]
/// impl JsonTask for AddTask {
///     const GROUP: &'static str = "math";
///     const NAME: &'static str = "add";
///
///     type Input = AddInput;
///     type Output = AddOutput;
///
///     async fn handle(&self, _info: &TaskInfo, input: Self::Input)
///         -> Result<Self::Output, TaskError>
///     {
///         Ok(AddOutput { sum: input.a + input.b })
///     }
/// }
/// ```
#[async_trait]
pub trait JsonTask: Send + Sync + 'static {
    /// Task group identifier for organizing related tasks
    const GROUP: &'static str;

    /// Unique name for this task type within its group
    const NAME: &'static str;

    /// JSON-serializable input type
    type Input: Serialize + DeserializeOwned + Send + 'static;

    /// JSON-serializable output type
    type Output: Serialize + DeserializeOwned + Send + 'static;

    /// Execute the task with the given input
    ///
    /// # Arguments
    ///
    /// * `info` - Task metadata including ID, timing, and retry count
    /// * `input` - Deserialized task input
    ///
    /// # Returns
    ///
    /// Returns the task output on success, or a `TaskError` to control
    /// the task lifecycle (retry, discard, or fatal failure).
    async fn handle(&self, info: &TaskInfo, input: Self::Input) -> Result<Self::Output, TaskError>;
}

/// Blanket implementation: JsonTask → Task
#[async_trait]
impl<T> Task for T
where
    T: JsonTask,
{
    const GROUP: &'static str = T::GROUP;
    const NAME: &'static str = T::NAME;

    type Input = T::Input;
    type Output = T::Output;

    async fn handle(&self, info: &TaskInfo, input: Self::Input) -> Result<Self::Output, TaskError> {
        JsonTask::handle(self, info, input).await
    }

    fn encode_input(input: &Self::Input) -> Result<Bytes, TaskError> {
        Ok(Bytes::from(
            serde_json::to_vec(input).map_err(|e| TaskError::fatal_reason(e.to_string()))?,
        ))
    }

    fn decode_input(input: &Bytes) -> Result<Self::Input, TaskError> {
        serde_json::from_slice(input).map_err(|e| TaskError::fatal_reason(e.to_string()))
    }

    fn encode_output(output: &Self::Output) -> Result<Bytes, TaskError> {
        Ok(Bytes::from(
            serde_json::to_vec(output).map_err(|e| TaskError::fatal_reason(e.to_string()))?,
        ))
    }

    fn decode_output(input: &Bytes) -> Result<Self::Output, TaskError> {
        serde_json::from_slice(input).map_err(|e| TaskError::fatal_reason(e.to_string()))
    }
}

#[async_trait::async_trait]
pub(crate) trait TaskExecutor: Send + Sync + 'static {
    async fn execute(
        &self,
        info: &TaskInfo,
        input: &bytes::Bytes,
    ) -> Result<bytes::Bytes, TaskError>;
}

#[async_trait::async_trait]
impl<T> TaskExecutor for T
where
    T: Task,
{
    async fn execute(
        &self,
        info: &TaskInfo,
        input: &bytes::Bytes,
    ) -> Result<bytes::Bytes, TaskError> {
        let typed_input: T::Input = Self::decode_input(input)?;
        let output: T::Output = self.handle(info, typed_input).await?;
        let encoded = Self::encode_output(&output)?;
        Ok(encoded)
    }
}

/// Registry for task executors
#[derive(Default)]
pub struct TaskRegistry {
    executors: HashMap<(TaskGroup, TaskName), Arc<dyn TaskExecutor>>,
}

impl TaskRegistry {
    pub fn new() -> Self {
        Self {
            executors: HashMap::new(),
        }
    }

    pub fn register<T: Task>(&mut self, task: T) -> &mut Self {
        let key = (TaskGroup::from(T::GROUP), TaskName::from(T::NAME));
        self.executors
            .insert(key, Arc::new(task) as Arc<dyn TaskExecutor>);
        self
    }

    pub(crate) fn get(&self, group: &str, name: &str) -> Option<Arc<dyn TaskExecutor>> {
        self.executors
            .get(&(TaskGroup::from(group), TaskName::from(name)))
            .cloned()
    }
}
