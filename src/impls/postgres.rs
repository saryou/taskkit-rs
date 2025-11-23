use async_trait::async_trait;
use sqlx::pool::PoolConnection;
use sqlx::postgres::{PgPool, PgPoolOptions};
use sqlx::{Postgres, Transaction};
use std::collections::HashSet;
use tokio::sync::Mutex;

use crate::backend::{BackendError, Lock};
use crate::scheduler::SchedulerName;
use crate::task::TaskId;
use crate::util::Timestamp;
use crate::worker::WorkerId;

use super::sqlx::{DbStageInfo, DbTask, SqlQueries, SqlxBackend};

fn placeholders(n: usize) -> String {
    (1..=n)
        .map(|i| format!("${i}"))
        .collect::<Vec<_>>()
        .join(",")
}

/// Stable FNV-1a hash returning an i64 suitable for PostgreSQL advisory locks.
///
/// Unlike `DefaultHasher`, this produces the same value across processes and
/// Rust versions, which is required for distributed locking semantics.
fn fnv1a_i64(s: &str) -> i64 {
    const OFFSET: u64 = 14695981039346656037;
    const PRIME: u64 = 1099511628211;
    let mut h = OFFSET;
    for b in s.bytes() {
        h ^= b as u64;
        h = h.wrapping_mul(PRIME);
    }
    h as i64
}

impl sqlx::FromRow<'_, sqlx::postgres::PgRow> for DbTask {
    fn from_row(row: &sqlx::postgres::PgRow) -> Result<Self, sqlx::Error> {
        use sqlx::Row;
        let retry_count_i32: i32 = row.try_get("retry_count")?;
        Ok(DbTask {
            id: row.try_get("id")?,
            group: row.try_get("group")?,
            name: row.try_get("name")?,
            data: row.try_get("data")?,
            due: row.try_get("due")?,
            created: row.try_get("created")?,
            scheduled: row.try_get("scheduled")?,
            retry_count: retry_count_i32 as u32,
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

impl sqlx::FromRow<'_, sqlx::postgres::PgRow> for DbStageInfo {
    fn from_row(row: &sqlx::postgres::PgRow) -> Result<Self, sqlx::Error> {
        use sqlx::Row;
        Ok(DbStageInfo {
            worker_id: row.try_get("assignee_worker_id")?,
            task_id: row.try_get("id")?,
            assigned_at: row.try_get("began")?,
        })
    }
}

/// PostgreSQL backend implementation for Taskkit
///
/// `TaskBackend`, `WorkerTracker`, `LockProvider` and `EventBridge` all come from
/// `SqlxBackend`; only the dialect-specific queries live in this module.
pub type PostgresBackend = SqlxBackend<PostgresQueries>;

pub async fn new(database_url: &str) -> Result<PostgresBackend, BackendError> {
    let queries = PostgresQueries::new(database_url).await?;
    Ok(SqlxBackend::new(queries))
}

pub fn with_pool(pool: sqlx::postgres::PgPool) -> PostgresBackend {
    SqlxBackend::new(PostgresQueries::with_pool(pool))
}

pub async fn migrate(backend: &PostgresBackend) -> Result<(), BackendError> {
    backend.queries().migrate().await
}

#[derive(Clone)]
pub struct PostgresQueries {
    pool: PgPool,
}

impl PostgresQueries {
    pub async fn new(database_url: &str) -> Result<Self, BackendError> {
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(database_url)
            .await
            .map_err(|e| BackendError::Unavailable(e.to_string()))?;

        Ok(Self { pool })
    }

    pub fn with_pool(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn migrate(&self) -> Result<(), BackendError> {
        sqlx::migrate!("./migrations/postgres")
            .run(&self.pool)
            .await
            .map_err(|e| BackendError::OperationFailed(e.to_string()))?;
        Ok(())
    }
}

#[async_trait]
impl SqlQueries for PostgresQueries {
    type Tx<'tx>
        = Transaction<'tx, Postgres>
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
        sqlx::query_as("SELECT * FROM taskkit_task WHERE id = $1 FOR UPDATE")
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
        sqlx::query_as("SELECT * FROM taskkit_task WHERE id = $1 FOR UPDATE SKIP LOCKED")
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
             SET assignee_worker_id = $1, began = $2
             WHERE id = $3",
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

        // Build bulk insert query with numbered placeholders: INSERT INTO ... VALUES ($1, $2, ...), ($16, $17, ...) ON CONFLICT DO NOTHING
        let mut placeholders = Vec::new();
        for i in 0..tasks.len() {
            let base = i * 15 + 1;
            let values = (base..base + 15)
                .map(|n| format!("${}", n))
                .collect::<Vec<_>>()
                .join(", ");
            placeholders.push(format!("({})", values));
        }
        let values_clause = placeholders.join(", ");

        let query = format!(
            r#"INSERT INTO taskkit_task
             (id, "group", name, data, due, created, scheduled, retry_count, ttl,
              assignee_worker_id, began, result, error_message, done, disposable)
             VALUES {}
             ON CONFLICT (id) DO NOTHING"#,
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
                .bind(task.retry_count as i32)
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
             SET done = $1, began = $2, result = $3, error_message = $4, disposable = $5
             WHERE id = $6",
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
            "INSERT INTO taskkit_scheduler_state (id, data) VALUES ($1, $2)
             ON CONFLICT (id) DO UPDATE SET data = EXCLUDED.data",
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
            r#"SELECT * FROM taskkit_task
             WHERE began IS NULL AND "group" = $1
             ORDER BY due
             LIMIT $2"#,
        )
        .bind(group)
        .bind(limit as i64)
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
            r#"SELECT id FROM taskkit_task
             WHERE began IS NULL AND "group" = $1 AND due < $2
             ORDER BY due
             LIMIT $3"#,
        )
        .bind(group)
        .bind(now)
        .bind(limit as i64)
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
        sqlx::query_as("SELECT * FROM taskkit_task WHERE id = $1")
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
             WHERE done IS NOT NULL AND done >= $1 AND done <= $2
             ORDER BY done
             LIMIT $3",
        )
        .bind(since)
        .bind(until)
        .bind(limit as i64)
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
             WHERE disposable IS NOT NULL AND disposable < $1
             ORDER BY disposable
             LIMIT $2",
        )
        .bind(now)
        .bind(limit as i64)
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
             LIMIT $1",
        )
        .bind(limit as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| BackendError::OperationFailed(e.to_string()))
    }

    async fn restore<'tx>(&self, tx: &mut Self::Tx<'tx>, task_id: &str) -> Result<(), BackendError>
    where
        Self: 'tx,
    {
        sqlx::query("UPDATE taskkit_task SET began = NULL WHERE id = $1")
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
            sqlx::query_scalar("SELECT data FROM taskkit_scheduler_state WHERE id = $1")
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
        let values_clause = (0..ids.len())
            .map(|i| format!("(${}, ${})", i * 2 + 1, i * 2 + 2))
            .collect::<Vec<_>>()
            .join(", ");
        let query = format!(
            "INSERT INTO taskkit_worker (id, expires) VALUES {}
             ON CONFLICT (id) DO UPDATE SET expires = EXCLUDED.expires",
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
        Ok(Box::new(PostgresLock::new(
            self.pool.clone(),
            target.to_string(),
        )))
    }

    async fn fetch_events_since(&self, offset: f64) -> Result<Vec<(f64, Vec<u8>)>, BackendError> {
        sqlx::query_as("SELECT sent, data FROM taskkit_control_event WHERE sent > $1 ORDER BY sent")
            .bind(offset)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| BackendError::OperationFailed(e.to_string()))
    }

    async fn insert_event(&self, now: f64, data: &[u8]) -> Result<(), BackendError> {
        sqlx::query("INSERT INTO taskkit_control_event (sent, data) VALUES ($1, $2)")
            .bind(now)
            .bind(data)
            .execute(&self.pool)
            .await
            .map_err(|e| BackendError::OperationFailed(e.to_string()))?;

        Ok(())
    }

    async fn destroy_all(&self) -> Result<(), BackendError> {
        let tables = vec![
            "taskkit_control_event",
            "taskkit_task",
            "taskkit_worker",
            "taskkit_scheduler_state",
        ];

        for table in tables {
            let query = format!("TRUNCATE TABLE {} CASCADE", table);
            sqlx::query(&query)
                .execute(&self.pool)
                .await
                .map_err(|e| BackendError::OperationFailed(e.to_string()))?;
        }

        Ok(())
    }
}

/// PostgreSQL implementation of distributed lock using advisory locks
///
/// pg_try_advisory_lock/pg_advisory_unlock are session-scoped, so acquire and
/// release must run on the same connection. This struct holds a dedicated
/// connection for the duration of the lock.
struct PostgresLock {
    pool: PgPool,
    lock_key: i64,
    conn: Mutex<Option<PoolConnection<Postgres>>>,
}

impl PostgresLock {
    fn new(pool: PgPool, target: String) -> Self {
        Self {
            pool,
            lock_key: fnv1a_i64(&target),
            conn: Mutex::new(None),
        }
    }
}

impl Drop for PostgresLock {
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
impl Lock for PostgresLock {
    async fn acquire(&self) -> bool {
        let mut conn_guard = self.conn.lock().await;
        if conn_guard.is_some() {
            return true;
        }

        let mut conn = match self.pool.acquire().await {
            Ok(c) => c,
            Err(_) => return false,
        };

        let result: Result<Option<bool>, _> = sqlx::query_scalar("SELECT pg_try_advisory_lock($1)")
            .bind(self.lock_key)
            .fetch_one(&mut *conn)
            .await;

        match result {
            Ok(Some(true)) => {
                *conn_guard = Some(conn);
                true
            }
            _ => false,
        }
    }

    async fn release(&self) {
        let mut conn_guard = self.conn.lock().await;
        if let Some(mut conn) = conn_guard.take() {
            let _: Result<Option<bool>, _> = sqlx::query_scalar("SELECT pg_advisory_unlock($1)")
                .bind(self.lock_key)
                .fetch_one(&mut *conn)
                .await;
        }
    }
}
