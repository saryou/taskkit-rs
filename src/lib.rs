//! # Taskkit
//!
//! A distributed task queue system for Rust with pluggable backend support.
//!
//! ## Features
//!
//! - **Type-safe tasks**: Define tasks with typed input and output
//! - **Multiple serialization formats**: Built-in JSON support via `JsonTask`, or implement custom serialization
//! - **Pluggable backends**: Designed to support multiple backend implementations
//! - **Distributed execution**: Backend-agnostic task queue with worker management
//! - **Scheduling**: Cron-like task scheduling with timezone support
//! - **Error handling**: Retry, discard, and failure mechanisms
//! - **Result tracking**: Wait for and retrieve task results
//!
//! ## Quick Start
//!
//! ```ignore
//! use taskkit::{JsonTask, Kit, TaskError, TaskInfo};
//! use async_trait::async_trait;
//! use serde::{Deserialize, Serialize};
//!
//! // Define your task
//! struct MyTask;
//!
//! #[derive(Serialize, Deserialize)]
//! struct MyInput {
//!     value: i32,
//! }
//!
//! #[derive(Serialize, Deserialize)]
//! struct MyOutput {
//!     result: i32,
//! }
//!
//! #[async_trait]
//! impl JsonTask for MyTask {
//!     const GROUP: &'static str = "my_group";
//!     const NAME: &'static str = "my_task";
//!
//!     type Input = MyInput;
//!     type Output = MyOutput;
//!
//!     async fn handle(&self, info: &TaskInfo, input: Self::Input) -> Result<Self::Output, TaskError> {
//!         Ok(MyOutput { result: input.value * 2 })
//!     }
//! }
//! ```

mod backend;
mod kit;
mod result;
mod runner;
mod runtime;
mod scheduler;
mod service;
mod stage;
mod task;
mod util;
mod worker;

#[cfg(any(
    feature = "memory",
    feature = "redis",
    feature = "mysql",
    feature = "postgres"
))]
pub mod impls;

pub use backend::{
    Backend, BackendError, ControlEvent, EventBridge, GetResultError, Lock, LockProvider,
    ReceiveEventsError, SendEventError, TaskBackend, WorkerTracker,
};
pub use kit::{InitiateTaskError, Kit, PollingInterval};
pub use result::{ResultGetError, TaskResult};
pub use runtime::RuntimeError;
pub use scheduler::{
    All, Days, DuplicationPolicy, Hours, Minutes, Months, OnlyEarliest, OnlyLatest,
    RegularSchedule, Schedule, ScheduleEntry, ScheduleEntryKey, ScheduleFieldError, SchedulerName,
    Seconds, Weekdays,
};
pub use stage::StageInfo;
pub use task::{
    DEFAULT_TASK_TTL, JsonTask, Task, TaskError, TaskGroup, TaskId, TaskInfo, TaskName, TaskRecord,
    TaskRegistry,
};
pub use util::{Timestamp, Ttl, local_tz};
pub use worker::WorkerId;

#[cfg(feature = "memory")]
pub use impls::MemoryBackend;

#[cfg(feature = "redis")]
pub use impls::RedisBackend;

#[cfg(feature = "mysql")]
pub use impls::MysqlBackend;

#[cfg(feature = "postgres")]
pub use impls::PostgresBackend;
