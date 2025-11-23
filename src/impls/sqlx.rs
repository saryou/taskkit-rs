use async_trait::async_trait;
use bytes::Bytes;
use futures_util::stream::Stream;
use std::collections::HashSet;
use std::pin::Pin;

use crate::backend::{
    Backend, BackendError, ControlEvent, EventBridge, GetResultError, Lock, LockProvider,
    ReceiveEventsError, SendEventError, TaskBackend, WorkerTracker,
};
use crate::scheduler::SchedulerName;
use crate::stage::StageInfo;
use crate::task::{TaskGroup, TaskId, TaskInfo, TaskName, TaskRecord};
use crate::util::{Timestamp, Ttl};
use crate::worker::WorkerId;

/// Database representation of a task
///
/// This struct maps directly to the database schema and is used by both
/// MySQL and PostgreSQL implementations.
#[derive(Debug, Clone)]
pub struct DbTask {
    pub id: String,
    pub group: String,
    pub name: String,
    pub data: Vec<u8>,
    pub due: f64,
    pub created: f64,
    pub scheduled: Option<f64>,
    pub retry_count: u32,
    pub ttl: f64,
    pub assignee_worker_id: Option<String>,
    pub began: Option<f64>,
    pub result: Option<Vec<u8>>,
    pub error_message: Option<String>,
    pub done: Option<f64>,
    pub disposable: Option<f64>,
}

/// Database representation of stage info (in-progress task assignment)
///
/// This struct maps directly to the database schema and is used by both
/// MySQL and PostgreSQL implementations.
#[derive(Debug, Clone)]
pub struct DbStageInfo {
    pub worker_id: String,
    pub task_id: String,
    pub assigned_at: f64,
}

impl DbStageInfo {
    pub fn to_stage_info(&self) -> StageInfo {
        StageInfo {
            worker_id: WorkerId::from(self.worker_id.clone()),
            task_id: TaskId::from(self.task_id.clone()),
            assigned_at: Timestamp::from_secs_f64(self.assigned_at),
        }
    }
}

impl DbTask {
    pub fn from_task_record(record: &TaskRecord) -> Self {
        DbTask {
            id: record.info.id.as_str().to_string(),
            group: record.info.group.as_str().to_string(),
            name: record.info.name.as_str().to_string(),
            data: record.data.to_vec(),
            due: record.info.due.as_secs_f64(),
            created: record.info.created.as_secs_f64(),
            scheduled: record.info.scheduled.map(|t| t.as_secs_f64()),
            retry_count: record.info.retry_count,
            ttl: record.info.ttl.as_secs_f64(),
            assignee_worker_id: None,
            began: None,
            result: None,
            error_message: None,
            done: None,
            disposable: None,
        }
    }

    pub fn to_task_record(&self) -> TaskRecord {
        TaskRecord {
            info: TaskInfo {
                id: TaskId::from(self.id.clone()),
                group: TaskGroup::from(self.group.clone()),
                name: TaskName::from(self.name.clone()),
                due: Timestamp::from_secs_f64(self.due),
                created: Timestamp::from_secs_f64(self.created),
                scheduled: self.scheduled.map(Timestamp::from_secs_f64),
                retry_count: self.retry_count,
                ttl: Ttl::from_secs_f64(self.ttl),
            },
            data: Bytes::from(self.data.clone()),
        }
    }
}

/// Trait defining database-specific query operations
///
/// This trait abstracts all SQL queries that differ between database dialects.
/// Implementations provide dialect-specific SQL while the common layer handles
/// the business logic and algorithm.
#[async_trait]
pub trait SqlQueries: Clone + Send + Sync + 'static {
    type Tx<'tx>: Send
    where
        Self: 'tx;

    async fn begin(&self) -> Result<Self::Tx<'_>, BackendError>;

    async fn commit(&self, tx: Self::Tx<'_>) -> Result<(), BackendError>;

    async fn fetch_task_for_update<'tx>(
        &self,
        tx: &mut Self::Tx<'tx>,
        task_id: &str,
    ) -> Result<Option<DbTask>, BackendError>
    where
        Self: 'tx;

    async fn fetch_task_for_assign<'tx>(
        &self,
        tx: &mut Self::Tx<'tx>,
        task_id: &str,
    ) -> Result<Option<DbTask>, BackendError>
    where
        Self: 'tx;

    async fn update_task_assignment<'tx>(
        &self,
        tx: &mut Self::Tx<'tx>,
        task_id: &str,
        worker_id: &str,
        began: f64,
    ) -> Result<(), BackendError>
    where
        Self: 'tx;

    async fn delete_tasks<'tx>(
        &self,
        tx: &mut Self::Tx<'tx>,
        task_ids: &[TaskId],
    ) -> Result<(), BackendError>
    where
        Self: 'tx;

    async fn upsert_tasks<'tx>(
        &self,
        tx: &mut Self::Tx<'tx>,
        tasks: &[DbTask],
    ) -> Result<(), BackendError>
    where
        Self: 'tx;

    #[allow(clippy::too_many_arguments)]
    async fn update_task_done<'tx>(
        &self,
        tx: &mut Self::Tx<'tx>,
        task_id: &str,
        began: f64,
        done: f64,
        result: Option<Vec<u8>>,
        error_message: Option<String>,
        disposable: f64,
    ) -> Result<(), BackendError>
    where
        Self: 'tx;

    async fn upsert_scheduler_state<'tx>(
        &self,
        tx: &mut Self::Tx<'tx>,
        name: &str,
        data: &[u8],
    ) -> Result<(), BackendError>
    where
        Self: 'tx;

    async fn get_queued_tasks(
        &self,
        group: &str,
        limit: usize,
    ) -> Result<Vec<DbTask>, BackendError>;

    async fn list_due_task_ids(
        &self,
        group: &str,
        now: f64,
        limit: usize,
    ) -> Result<Vec<String>, BackendError>;

    async fn lookup_tasks(&self, task_ids: &[TaskId]) -> Result<Vec<Option<DbTask>>, BackendError>;

    async fn discard_tasks<'tx>(
        &self,
        tx: &mut Self::Tx<'tx>,
        task_ids: &[TaskId],
    ) -> Result<(), BackendError>
    where
        Self: 'tx;

    async fn get_result(&self, task_id: &TaskId) -> Result<Option<DbTask>, BackendError>;

    async fn get_done_task_ids(
        &self,
        since: f64,
        until: f64,
        limit: usize,
    ) -> Result<Vec<TaskId>, BackendError>;

    async fn get_disposable_task_ids(
        &self,
        now: f64,
        limit: usize,
    ) -> Result<Vec<TaskId>, BackendError>;

    async fn get_stage_info(&self, limit: usize) -> Result<Vec<DbStageInfo>, BackendError>;

    async fn restore<'tx>(&self, tx: &mut Self::Tx<'tx>, task_id: &str) -> Result<(), BackendError>
    where
        Self: 'tx;

    async fn get_scheduler_state(
        &self,
        name: &SchedulerName,
    ) -> Result<Option<Vec<u8>>, BackendError>;

    async fn set_worker_ttl(
        &self,
        worker_ids: HashSet<WorkerId>,
        expires_at: f64,
    ) -> Result<(), BackendError>;

    async fn get_workers(&self) -> Result<Vec<(WorkerId, Timestamp)>, BackendError>;

    async fn purge_workers(&self, worker_ids: HashSet<WorkerId>) -> Result<(), BackendError>;

    async fn get_lock(&self, target: &str) -> Result<Box<dyn Lock>, BackendError>;

    async fn fetch_events_since(&self, offset: f64) -> Result<Vec<(f64, Vec<u8>)>, BackendError>;

    async fn insert_event(&self, now: f64, data: &[u8]) -> Result<(), BackendError>;

    async fn destroy_all(&self) -> Result<(), BackendError>;
}

/// Common SQLX backend implementation
///
/// This struct holds the business logic that is shared across all SQL backends.
/// The actual SQL queries are delegated to the SqlQueries trait implementation.
#[derive(Clone)]
pub struct SqlxBackend<Q: SqlQueries> {
    queries: Q,
}

impl<Q: SqlQueries> SqlxBackend<Q> {
    pub fn new(queries: Q) -> Self {
        Self { queries }
    }

    pub fn queries(&self) -> &Q {
        &self.queries
    }
}

#[async_trait]
impl<Q: SqlQueries> TaskBackend for SqlxBackend<Q> {
    async fn put_tasks(&self, tasks: Vec<TaskRecord>) -> Result<(), BackendError> {
        if tasks.is_empty() {
            return Ok(());
        }
        let db_tasks: Vec<_> = tasks.iter().map(DbTask::from_task_record).collect();

        let mut tx = self.queries.begin().await?;
        self.queries.upsert_tasks(&mut tx, &db_tasks).await?;
        self.queries.commit(tx).await
    }

    async fn get_queued_tasks(
        &self,
        group: &str,
        limit: usize,
    ) -> Result<Vec<TaskRecord>, BackendError> {
        let db_tasks = self.queries.get_queued_tasks(group, limit).await?;
        Ok(db_tasks.into_iter().map(|t| t.to_task_record()).collect())
    }

    async fn assign_tasks(
        &self,
        group: &str,
        worker_id: &str,
        limit: usize,
    ) -> Result<Vec<TaskRecord>, BackendError> {
        if limit == 0 {
            return Ok(Vec::new());
        }

        let mut assigned = Vec::with_capacity(limit);
        // Candidates lost to other workers do not count towards the limit, so widen
        // the scan until it either fills the batch or runs out of due tasks.
        let mut scan = limit.max(8);

        loop {
            let pks = self
                .queries
                .list_due_task_ids(group, Timestamp::now().as_secs_f64(), scan)
                .await?;

            for pk in &pks {
                let now = Timestamp::now().as_secs_f64();
                let mut tx = self.queries.begin().await?;
                let db_task_opt = self.queries.fetch_task_for_assign(&mut tx, pk).await?;

                if let Some(mut db_task) = db_task_opt
                    && db_task.began.is_none()
                {
                    self.queries
                        .update_task_assignment(&mut tx, pk, worker_id, now)
                        .await?;
                    self.queries.commit(tx).await?;

                    db_task.assignee_worker_id = Some(worker_id.to_string());
                    db_task.began = Some(now);
                    assigned.push(db_task.to_task_record());

                    if assigned.len() == limit {
                        return Ok(assigned);
                    }
                } else {
                    self.queries.commit(tx).await?;
                }
            }

            if pks.len() < scan {
                return Ok(assigned);
            }
            scan *= 2;
        }
    }

    async fn lookup_tasks(
        &self,
        task_ids: &[TaskId],
    ) -> Result<Vec<Option<TaskRecord>>, BackendError> {
        if task_ids.is_empty() {
            return Ok(Vec::new());
        }
        let db_tasks = self.queries.lookup_tasks(task_ids).await?;
        Ok(db_tasks
            .iter()
            .map(|opt| opt.as_ref().map(|t| t.to_task_record()))
            .collect())
    }

    async fn retry_task(&self, record: TaskRecord) -> Result<(), BackendError> {
        let mut tx = self.queries.begin().await?;

        self.queries
            .delete_tasks(&mut tx, std::slice::from_ref(&record.info.id))
            .await?;

        let db_task = DbTask::from_task_record(&record);
        self.queries.upsert_tasks(&mut tx, &[db_task]).await?;

        self.queries.commit(tx).await
    }

    async fn discard_tasks(&self, task_ids: &[TaskId]) -> Result<(), BackendError> {
        if task_ids.is_empty() {
            return Ok(());
        }
        let mut tx = self.queries.begin().await?;
        self.queries.discard_tasks(&mut tx, task_ids).await?;
        self.queries.commit(tx).await
    }

    async fn succeed(&self, record: TaskRecord, result: Bytes) -> Result<(), BackendError> {
        let now = Timestamp::now().as_secs_f64();
        let disposable = now + record.info.ttl.as_secs_f64();
        let task_id = record.info.id.as_str();

        let mut tx = self.queries.begin().await?;
        let existing = self.queries.fetch_task_for_update(&mut tx, task_id).await?;
        let began = existing.and_then(|db| db.began).unwrap_or(now);
        self.queries
            .update_task_done(
                &mut tx,
                task_id,
                began,
                now,
                Some(result.to_vec()),
                None,
                disposable,
            )
            .await?;

        self.queries.commit(tx).await
    }

    async fn fail(&self, record: TaskRecord, error: Bytes) -> Result<(), BackendError> {
        let now = Timestamp::now().as_secs_f64();
        let disposable = now + record.info.ttl.as_secs_f64();
        let task_id = record.info.id.as_str();
        let error_message = String::from_utf8_lossy(error.as_ref()).to_string();

        let mut tx = self.queries.begin().await?;
        let existing = self.queries.fetch_task_for_update(&mut tx, task_id).await?;
        let began = existing.and_then(|db| db.began).unwrap_or(now);
        self.queries
            .update_task_done(
                &mut tx,
                task_id,
                began,
                now,
                None,
                Some(error_message),
                disposable,
            )
            .await?;

        self.queries.commit(tx).await
    }

    async fn get_result(&self, task_id: &TaskId) -> Result<(TaskRecord, Bytes), GetResultError> {
        let db_task = self.queries.get_result(task_id).await?;

        match db_task {
            None => Err(GetResultError::NotFound),
            Some(db) => {
                let record = db.to_task_record();
                if db.done.is_none() {
                    Err(GetResultError::NoResult(record))
                } else if let Some(result) = db.result {
                    Ok((record, Bytes::from(result)))
                } else {
                    Err(GetResultError::Failed {
                        record,
                        message: db.error_message.unwrap_or_default(),
                    })
                }
            }
        }
    }

    async fn get_done_task_ids(
        &self,
        since: Option<Timestamp>,
        until: Option<Timestamp>,
        limit: usize,
    ) -> Result<Vec<TaskId>, BackendError> {
        let since_val = since.map(|t| t.as_secs_f64()).unwrap_or(0.0);
        let until_val = until
            .map(|t| t.as_secs_f64())
            .unwrap_or_else(|| Timestamp::now().as_secs_f64());

        self.queries
            .get_done_task_ids(since_val, until_val, limit)
            .await
    }

    async fn get_disposable_task_ids(&self, limit: usize) -> Result<Vec<TaskId>, BackendError> {
        let now = Timestamp::now().as_secs_f64();
        self.queries.get_disposable_task_ids(now, limit).await
    }

    async fn get_stage_info(&self, limit: usize) -> Result<Vec<StageInfo>, BackendError> {
        let db_infos = self.queries.get_stage_info(limit).await?;
        Ok(db_infos.into_iter().map(|db| db.to_stage_info()).collect())
    }

    async fn restore(&self, info: StageInfo) -> Result<(), BackendError> {
        let mut tx = self.queries.begin().await?;
        self.queries.restore(&mut tx, info.task_id.as_str()).await?;
        self.queries.commit(tx).await
    }

    async fn persist_scheduler_state_and_put_tasks(
        &self,
        name: &SchedulerName,
        data: Bytes,
        tasks: Vec<TaskRecord>,
    ) -> Result<(), BackendError> {
        let mut tx = self.queries.begin().await?;

        self.queries
            .upsert_scheduler_state(&mut tx, name.as_str(), data.as_ref())
            .await?;

        if !tasks.is_empty() {
            let db_tasks: Vec<_> = tasks.iter().map(DbTask::from_task_record).collect();
            self.queries.upsert_tasks(&mut tx, &db_tasks).await?;
        }

        self.queries.commit(tx).await
    }

    async fn get_scheduler_state(
        &self,
        name: &SchedulerName,
    ) -> Result<Option<Vec<u8>>, BackendError> {
        self.queries.get_scheduler_state(name).await
    }
}

#[async_trait]
impl<Q: SqlQueries> WorkerTracker for SqlxBackend<Q> {
    async fn set_worker_ttl(
        &self,
        worker_ids: HashSet<WorkerId>,
        expires_at: Timestamp,
    ) -> Result<(), BackendError> {
        if worker_ids.is_empty() {
            return Ok(());
        }
        let expires = expires_at.as_secs_f64();
        self.queries.set_worker_ttl(worker_ids, expires).await
    }

    async fn get_workers(&self) -> Result<Vec<(WorkerId, Timestamp)>, BackendError> {
        self.queries.get_workers().await
    }

    async fn purge_workers(&self, worker_ids: HashSet<WorkerId>) -> Result<(), BackendError> {
        if worker_ids.is_empty() {
            return Ok(());
        }
        self.queries.purge_workers(worker_ids).await
    }
}

#[async_trait]
impl<Q: SqlQueries> LockProvider for SqlxBackend<Q> {
    async fn get_lock(&self, target: &str) -> Result<Box<dyn Lock>, BackendError> {
        self.queries.get_lock(target).await
    }
}

#[async_trait]
impl<Q: SqlQueries> EventBridge for SqlxBackend<Q> {
    async fn receive_events(
        &self,
    ) -> Result<Pin<Box<dyn Stream<Item = ControlEvent> + Send>>, ReceiveEventsError> {
        use futures_util::stream::{self, StreamExt};
        use std::sync::Arc;
        use std::time::Duration;
        use tokio::sync::Mutex;

        let queries = self.queries.clone();
        let offset = Arc::new(Mutex::new(Timestamp::now().as_secs_f64()));

        let stream = stream::unfold((queries, offset), |(queries, offset)| async move {
            loop {
                let current_offset = *offset.lock().await;
                let events = queries.fetch_events_since(current_offset).await;

                if let Ok(events) = events {
                    for (sent, data) in events {
                        *offset.lock().await = sent;
                        if let Ok(event) = serde_json::from_slice::<ControlEvent>(&data) {
                            return Some((event, (queries, offset)));
                        }
                    }
                }

                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        })
        .boxed();

        Ok(stream)
    }

    async fn send_event(&self, event: &ControlEvent) -> Result<(), SendEventError> {
        let now = Timestamp::now().as_secs_f64();
        let data = serde_json::to_vec(event)
            .map_err(|e| SendEventError::SerializationError(e.to_string()))?;

        self.queries
            .insert_event(now, &data)
            .await
            .map_err(|e| SendEventError::SendFailed(e.to_string()))
    }
}

#[async_trait]
impl<Q: SqlQueries> Backend for SqlxBackend<Q> {
    async fn destroy_all(&self) -> Result<(), BackendError> {
        self.queries.destroy_all().await
    }
}
