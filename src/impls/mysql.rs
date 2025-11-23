use async_trait::async_trait;
use sqlx::mysql::{MySqlPool, MySqlPoolOptions};
use sqlx::pool::PoolConnection;
use sqlx::{MySql, Transaction};
use std::collections::HashSet;
use tokio::sync::Mutex;

use crate::backend::{BackendError, Lock};
use crate::scheduler::SchedulerName;
use crate::task::TaskId;
use crate::util::Timestamp;
use crate::worker::WorkerId;

use super::sqlx::{DbStageInfo, DbTask, SqlQueries, SqlxBackend};

fn placeholders(n: usize) -> String {
    (0..n).map(|_| "?").collect::<Vec<_>>().join(",")
}

impl sqlx::FromRow<'_, sqlx::mysql::MySqlRow> for DbTask {
    fn from_row(row: &sqlx::mysql::MySqlRow) -> Result<Self, sqlx::Error> {
        use sqlx::Row;
        Ok(DbTask {
            id: row.try_get("id")?,
            group: row.try_get("group")?,
            name: row.try_get("name")?,
            data: row.try_get("data")?,
            due: row.try_get("due")?,
            created: row.try_get("created")?,
            scheduled: row.try_get("scheduled")?,
            retry_count: row.try_get("retry_count")?,
            ttl: row.try_get("ttl")?,
            assignee_worker_id: row.try_get("assignee_worker_id")?,
            began: row.try_get("began")?,
            result: row.try_get("result")?,
            error_message: row.try_get("error_message")?,
            done: row.try_get("done")?,
            disposable: row.try_get("disposable")?,
        })
    }
}

impl sqlx::FromRow<'_, sqlx::mysql::MySqlRow> for DbStageInfo {
    fn from_row(row: &sqlx::mysql::MySqlRow) -> Result<Self, sqlx::Error> {
        use sqlx::Row;
        Ok(DbStageInfo {
            worker_id: row.try_get("assignee_worker_id")?,
            task_id: row.try_get("id")?,
            assigned_at: row.try_get("began")?,
        })
    }
}

/// MySQL backend implementation for Taskkit
///
/// `TaskBackend`, `WorkerTracker`, `LockProvider` and `EventBridge` all come from
/// `SqlxBackend`; only the dialect-specific queries live in this module.
pub type MysqlBackend = SqlxBackend<MysqlQueries>;

pub async fn new(database_url: &str) -> Result<MysqlBackend, BackendError> {
    let queries = MysqlQueries::new(database_url).await?;
    Ok(SqlxBackend::new(queries))
}

pub fn with_pool(pool: sqlx::mysql::MySqlPool) -> MysqlBackend {
    SqlxBackend::new(MysqlQueries::with_pool(pool))
}

pub async fn migrate(backend: &MysqlBackend) -> Result<(), BackendError> {
    backend.queries().migrate().await
}

#[derive(Clone)]
pub struct MysqlQueries {
    pool: MySqlPool,
}

impl MysqlQueries {
    pub async fn new(database_url: &str) -> Result<Self, BackendError> {
        let pool = MySqlPoolOptions::new()
            .max_connections(5)
            .connect(database_url)
            .await
            .map_err(|e| BackendError::Unavailable(e.to_string()))?;

        Ok(Self { pool })
    }

    pub fn with_pool(pool: MySqlPool) -> Self {
        Self { pool }
    }

    pub async fn migrate(&self) -> Result<(), BackendError> {
        sqlx::migrate!("./migrations/mysql")
            .run(&self.pool)
            .await
            .map_err(|e| BackendError::OperationFailed(e.to_string()))?;
        Ok(())
    }
}

#[async_trait]
impl SqlQueries for MysqlQueries {
    type Tx<'tx>
        = Transaction<'tx, MySql>
    where
        Self: 'tx;

    async fn begin(&self) -> Result<Self::Tx<'_>, BackendError> {
        self.pool
            .begin()
            .await
            .map_err(|e| BackendError::OperationFailed(e.to_string()))
    }

    async fn commit(&self, tx: Self::Tx<'_>) -> Result<(), BackendError> {
        tx.commit()
            .await
            .map_err(|e| BackendError::OperationFailed(e.to_string()))
    }

    async fn fetch_task_for_update<'tx>(
        &self,
        tx: &mut Self::Tx<'tx>,
        task_id: &str,
    ) -> Result<Option<DbTask>, BackendError>
    where
        Self: 'tx,
    {
        sqlx::query_as("SELECT * FROM taskkit_task WHERE id = ? FOR UPDATE")
            .bind(task_id)
            .fetch_optional(&mut **tx)
            .await
            .map_err(|e| BackendError::OperationFailed(e.to_string()))
    }

    async fn fetch_task_for_assign<'tx>(
        &self,
        tx: &mut Self::Tx<'tx>,
        task_id: &str,
    ) -> Result<Option<DbTask>, BackendError>
    where
        Self: 'tx,
    {
        sqlx::query_as("SELECT * FROM taskkit_task WHERE id = ? FOR UPDATE SKIP LOCKED")
            .bind(task_id)
            .fetch_optional(&mut **tx)
            .await
            .map_err(|e| BackendError::OperationFailed(e.to_string()))
    }

    async fn update_task_assignment<'tx>(
        &self,
        tx: &mut Self::Tx<'tx>,
        task_id: &str,
        worker_id: &str,
        began: f64,
    ) -> Result<(), BackendError>
    where
        Self: 'tx,
    {
        sqlx::query(
            "UPDATE taskkit_task
             SET assignee_worker_id = ?, began = ?
             WHERE id = ?",
        )
        .bind(worker_id)
        .bind(began)
        .bind(task_id)
        .execute(&mut **tx)
        .await
        .map_err(|e| BackendError::OperationFailed(e.to_string()))?;

        Ok(())
    }

    async fn delete_tasks<'tx>(
        &self,
        tx: &mut Self::Tx<'tx>,
        task_ids: &[TaskId],
    ) -> Result<(), BackendError>
    where
        Self: 'tx,
    {
        if task_ids.is_empty() {
            return Ok(());
        }

        let ids: Vec<&str> = task_ids.iter().map(|id| id.as_str()).collect();
        let query = format!(
            "DELETE FROM taskkit_task WHERE id IN ({})",
            placeholders(ids.len())
        );

        let mut q = sqlx::query(&query);
        for id in &ids {
            q = q.bind(id);
        }
        q.execute(&mut **tx)
            .await
            .map_err(|e| BackendError::OperationFailed(e.to_string()))?;

        Ok(())
    }

    async fn upsert_tasks<'tx>(
        &self,
        tx: &mut Self::Tx<'tx>,
        tasks: &[DbTask],
    ) -> Result<(), BackendError>
    where
        Self: 'tx,
    {
        if tasks.is_empty() {
            return Ok(());
        }

        // Build bulk insert query: INSERT INTO ... VALUES (...), (...), ... ON DUPLICATE KEY UPDATE id = id
        let values_placeholder = "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        let values_clause = vec![values_placeholder; tasks.len()].join(", ");
        let query = format!(
            "INSERT INTO taskkit_task
             (id, `group`, name, data, due, created, scheduled, retry_count, ttl,
              assignee_worker_id, began, result, error_message, done, disposable)
             VALUES {}
             ON DUPLICATE KEY UPDATE id = id",
            values_clause
        );

        let mut q = sqlx::query(&query);
        for task in tasks {
            q = q
                .bind(&task.id)
                .bind(&task.group)
                .bind(&task.name)
                .bind(&task.data)
                .bind(task.due)
                .bind(task.created)
                .bind(task.scheduled)
                .bind(task.retry_count)
                .bind(task.ttl)
                .bind(&task.assignee_worker_id)
                .bind(task.began)
                .bind(&task.result)
                .bind(&task.error_message)
                .bind(task.done)
                .bind(task.disposable);
        }

        q.execute(&mut **tx)
            .await
            .map_err(|e| BackendError::OperationFailed(e.to_string()))?;

        Ok(())
    }

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
        Self: 'tx,
    {
        sqlx::query(
            "UPDATE taskkit_task
             SET done = ?, began = ?, result = ?, error_message = ?, disposable = ?
             WHERE id = ?",
        )
        .bind(done)
        .bind(began)
        .bind(result)
        .bind(error_message)
        .bind(disposable)
        .bind(task_id)
        .execute(&mut **tx)
        .await
        .map_err(|e| BackendError::OperationFailed(e.to_string()))?;

        Ok(())
    }

    async fn upsert_scheduler_state<'tx>(
        &self,
        tx: &mut Self::Tx<'tx>,
        name: &str,
        data: &[u8],
    ) -> Result<(), BackendError>
    where
        Self: 'tx,
    {
        sqlx::query(
            "INSERT INTO taskkit_scheduler_state (id, data) VALUES (?, ?)
             ON DUPLICATE KEY UPDATE data = VALUES(data)",
        )
        .bind(name)
        .bind(data)
        .execute(&mut **tx)
        .await
        .map_err(|e| BackendError::OperationFailed(e.to_string()))?;

        Ok(())
    }

    async fn get_queued_tasks(
        &self,
        group: &str,
        limit: usize,
    ) -> Result<Vec<DbTask>, BackendError> {
        sqlx::query_as(
            "SELECT * FROM taskkit_task
             WHERE began IS NULL AND `group` = ?
             ORDER BY due
             LIMIT ?",
        )
        .bind(group)
        .bind(limit as i32)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| BackendError::OperationFailed(e.to_string()))
    }

    async fn list_due_task_ids(
        &self,
        group: &str,
        now: f64,
        limit: usize,
    ) -> Result<Vec<String>, BackendError> {
        let pks: Vec<String> = sqlx::query_scalar(
            "SELECT id FROM taskkit_task
             WHERE began IS NULL AND `group` = ? AND due < ?
             ORDER BY due
             LIMIT ?",
        )
        .bind(group)
        .bind(now)
        .bind(limit as i32)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| BackendError::OperationFailed(e.to_string()))?;

        Ok(pks)
    }

    async fn lookup_tasks(&self, task_ids: &[TaskId]) -> Result<Vec<Option<DbTask>>, BackendError> {
        let ids: Vec<&str> = task_ids.iter().map(|id| id.as_str()).collect();
        let query = format!(
            "SELECT * FROM taskkit_task WHERE id IN ({})",
            placeholders(ids.len())
        );

        let mut q = sqlx::query_as::<_, DbTask>(&query);
        for id in &ids {
            q = q.bind(id);
        }

        let found_tasks: Vec<DbTask> = q
            .fetch_all(&self.pool)
            .await
            .map_err(|e| BackendError::OperationFailed(e.to_string()))?;

        let task_map: std::collections::HashMap<String, DbTask> =
            found_tasks.into_iter().map(|t| (t.id.clone(), t)).collect();

        Ok(task_ids
            .iter()
            .map(|tid| task_map.get(tid.as_str()).cloned())
            .collect())
    }

    async fn discard_tasks<'tx>(
        &self,
        tx: &mut Self::Tx<'tx>,
        task_ids: &[TaskId],
    ) -> Result<(), BackendError>
    where
        Self: 'tx,
    {
        if task_ids.is_empty() {
            return Ok(());
        }

        let ids: Vec<&str> = task_ids.iter().map(|id| id.as_str()).collect();
        let query = format!(
            "DELETE FROM taskkit_task WHERE id IN ({})",
            placeholders(ids.len())
        );

        let mut q = sqlx::query(&query);
        for id in &ids {
            q = q.bind(id);
        }
        q.execute(&mut **tx)
            .await
            .map_err(|e| BackendError::OperationFailed(e.to_string()))?;

        Ok(())
    }

    async fn get_result(&self, task_id: &TaskId) -> Result<Option<DbTask>, BackendError> {
        sqlx::query_as("SELECT * FROM taskkit_task WHERE id = ?")
            .bind(task_id.as_str())
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| BackendError::OperationFailed(e.to_string()))
    }

    async fn get_done_task_ids(
        &self,
        since: f64,
        until: f64,
        limit: usize,
    ) -> Result<Vec<TaskId>, BackendError> {
        let ids: Vec<String> = sqlx::query_scalar(
            "SELECT id FROM taskkit_task
             WHERE done IS NOT NULL AND done >= ? AND done <= ?
             ORDER BY done
             LIMIT ?",
        )
        .bind(since)
        .bind(until)
        .bind(limit as i32)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| BackendError::OperationFailed(e.to_string()))?;

        Ok(ids.into_iter().map(TaskId::from).collect())
    }

    async fn get_disposable_task_ids(
        &self,
        now: f64,
        limit: usize,
    ) -> Result<Vec<TaskId>, BackendError> {
        let ids: Vec<String> = sqlx::query_scalar(
            "SELECT id FROM taskkit_task
             WHERE disposable IS NOT NULL AND disposable < ?
             ORDER BY disposable
             LIMIT ?",
        )
        .bind(now)
        .bind(limit as i32)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| BackendError::OperationFailed(e.to_string()))?;

        Ok(ids.into_iter().map(TaskId::from).collect())
    }

    async fn get_stage_info(&self, limit: usize) -> Result<Vec<DbStageInfo>, BackendError> {
        sqlx::query_as(
            "SELECT assignee_worker_id, id, began
             FROM taskkit_task
             WHERE done IS NULL AND began IS NOT NULL
             ORDER BY began
             LIMIT ?",
        )
        .bind(limit as i32)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| BackendError::OperationFailed(e.to_string()))
    }

    async fn restore<'tx>(&self, tx: &mut Self::Tx<'tx>, task_id: &str) -> Result<(), BackendError>
    where
        Self: 'tx,
    {
        sqlx::query("UPDATE taskkit_task SET began = NULL WHERE id = ?")
            .bind(task_id)
            .execute(&mut **tx)
            .await
            .map_err(|e| BackendError::OperationFailed(e.to_string()))?;

        Ok(())
    }

    async fn get_scheduler_state(
        &self,
        name: &SchedulerName,
    ) -> Result<Option<Vec<u8>>, BackendError> {
        let data: Option<Vec<u8>> =
            sqlx::query_scalar("SELECT data FROM taskkit_scheduler_state WHERE id = ?")
                .bind(name.as_str())
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| BackendError::OperationFailed(e.to_string()))?;

        Ok(data)
    }

    async fn set_worker_ttl(
        &self,
        worker_ids: HashSet<WorkerId>,
        expires_at: f64,
    ) -> Result<(), BackendError> {
        let ids: Vec<&str> = worker_ids.iter().map(|id| id.as_str()).collect();
        let values_clause = ids.iter().map(|_| "(?, ?)").collect::<Vec<_>>().join(", ");
        let query = format!(
            "INSERT INTO taskkit_worker (id, expires) VALUES {}
             ON DUPLICATE KEY UPDATE expires = VALUES(expires)",
            values_clause
        );

        let mut q = sqlx::query(&query);
        for id in &ids {
            q = q.bind(id).bind(expires_at);
        }
        q.execute(&self.pool)
            .await
            .map_err(|e| BackendError::OperationFailed(e.to_string()))?;

        Ok(())
    }

    async fn get_workers(&self) -> Result<Vec<(WorkerId, Timestamp)>, BackendError> {
        let workers: Vec<(String, f64)> =
            sqlx::query_as("SELECT id, expires FROM taskkit_worker ORDER BY expires")
                .fetch_all(&self.pool)
                .await
                .map_err(|e| BackendError::OperationFailed(e.to_string()))?;

        Ok(workers
            .into_iter()
            .map(|(id, expires)| (WorkerId::from(id), Timestamp::from_secs_f64(expires)))
            .collect())
    }

    async fn purge_workers(&self, worker_ids: HashSet<WorkerId>) -> Result<(), BackendError> {
        let ids: Vec<&str> = worker_ids.iter().map(|id| id.as_str()).collect();
        let query = format!(
            "DELETE FROM taskkit_worker WHERE id IN ({})",
            placeholders(ids.len())
        );

        let mut q = sqlx::query(&query);
        for id in &ids {
            q = q.bind(id);
        }
        q.execute(&self.pool)
            .await
            .map_err(|e| BackendError::OperationFailed(e.to_string()))?;

        Ok(())
    }

    async fn get_lock(&self, target: &str) -> Result<Box<dyn Lock>, BackendError> {
        Ok(Box::new(MysqlLock::new(
            self.pool.clone(),
            target.to_string(),
        )))
    }

    async fn fetch_events_since(&self, offset: f64) -> Result<Vec<(f64, Vec<u8>)>, BackendError> {
        sqlx::query_as("SELECT sent, data FROM taskkit_control_event WHERE sent > ? ORDER BY sent")
            .bind(offset)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| BackendError::OperationFailed(e.to_string()))
    }

    async fn insert_event(&self, now: f64, data: &[u8]) -> Result<(), BackendError> {
        sqlx::query("INSERT INTO taskkit_control_event (sent, data) VALUES (?, ?)")
            .bind(now)
            .bind(data)
            .execute(&self.pool)
            .await
            .map_err(|e| BackendError::OperationFailed(e.to_string()))?;

        Ok(())
    }

    async fn destroy_all(&self) -> Result<(), BackendError> {
        sqlx::query("SET FOREIGN_KEY_CHECKS = 0")
            .execute(&self.pool)
            .await
            .map_err(|e| BackendError::OperationFailed(e.to_string()))?;

        let tables = vec![
            "taskkit_control_event",
            "taskkit_task",
            "taskkit_worker",
            "taskkit_scheduler_state",
        ];

        for table in tables {
            let query = format!("TRUNCATE TABLE {}", table);
            sqlx::query(&query)
                .execute(&self.pool)
                .await
                .map_err(|e| BackendError::OperationFailed(e.to_string()))?;
        }

        sqlx::query("SET FOREIGN_KEY_CHECKS = 1")
            .execute(&self.pool)
            .await
            .map_err(|e| BackendError::OperationFailed(e.to_string()))?;

        Ok(())
    }
}

/// MySQL implementation of distributed lock using GET_LOCK
///
/// GET_LOCK/RELEASE_LOCK are session-scoped in MySQL, so acquire and release
/// must run on the same connection. This struct holds a dedicated connection
/// for the duration of the lock.
struct MysqlLock {
    pool: MySqlPool,
    target: String,
    conn: Mutex<Option<PoolConnection<MySql>>>,
}

impl MysqlLock {
    fn new(pool: MySqlPool, target: String) -> Self {
        Self {
            pool,
            target,
            conn: Mutex::new(None),
        }
    }
}

impl Drop for MysqlLock {
    fn drop(&mut self) {
        // If release() was skipped (a panic unwinding past it), returning the connection
        // to the pool would hand the still-locked session to the next borrower. The lock
        // is session-scoped, so detach the connection and let the drop close it.
        if let Some(conn) = self.conn.get_mut().take() {
            drop(conn.detach());
        }
    }
}

#[async_trait]
impl Lock for MysqlLock {
    async fn acquire(&self) -> bool {
        let mut conn_guard = self.conn.lock().await;
        if conn_guard.is_some() {
            return true;
        }

        let mut conn = match self.pool.acquire().await {
            Ok(c) => c,
            Err(_) => return false,
        };

        let result: Result<Option<i32>, _> = sqlx::query_scalar("SELECT GET_LOCK(?, 0)")
            .bind(&self.target)
            .fetch_one(&mut *conn)
            .await;

        match result {
            Ok(Some(1)) => {
                *conn_guard = Some(conn);
                true
            }
            _ => false,
        }
    }

    async fn release(&self) {
        let mut conn_guard = self.conn.lock().await;
        if let Some(mut conn) = conn_guard.take() {
            let _: Result<Option<i32>, _> = sqlx::query_scalar("SELECT RELEASE_LOCK(?)")
                .bind(&self.target)
                .fetch_one(&mut *conn)
                .await;
        }
    }
}
