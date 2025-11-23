use bytes::Bytes;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use taskkit::*;
use tokio::time::sleep;

const GROUP: &str = "test_group";

/// Generic backend test suite that can be run against any Backend implementation
pub async fn test_workers(backend: &Arc<dyn Backend>) -> Result<(), BackendError> {
    backend.destroy_all().await?;

    let workers = backend.get_workers().await?;
    assert_eq!(workers, vec![]);

    let worker_a = WorkerId::from("a");
    let worker_b = WorkerId::from("b");
    let ex_a = Timestamp::now() + Duration::from_secs(60);
    let ex_b = Timestamp::now() + Duration::from_secs(60);

    let mut set_a = HashSet::new();
    set_a.insert(worker_a.clone());
    backend.set_worker_ttl(set_a, ex_a).await?;

    let mut set_b = HashSet::new();
    set_b.insert(worker_b.clone());
    backend.set_worker_ttl(set_b, ex_b).await?;

    let workers = backend.get_workers().await?;
    assert_eq!(
        workers,
        vec![(worker_a.clone(), ex_a), (worker_b.clone(), ex_b)]
    );

    let mut purge_set = HashSet::new();
    purge_set.insert(worker_a.clone());
    backend.purge_workers(purge_set).await?;

    let workers = backend.get_workers().await?;
    assert_eq!(workers, vec![(worker_b.clone(), ex_b)]);

    let mut purge_set = HashSet::new();
    purge_set.insert(worker_b.clone());
    backend.purge_workers(purge_set).await?;

    let workers = backend.get_workers().await?;
    assert_eq!(workers, vec![]);

    Ok(())
}

pub async fn test_tasks(backend: &Arc<dyn Backend>) -> Result<(), BackendError> {
    backend.destroy_all().await?;

    let task_a = TaskRecord::new(
        TaskInfo::init(GROUP, "a", None::<f64>, None::<f64>, DEFAULT_TASK_TTL),
        Bytes::from("a"),
    );
    let task_b = TaskRecord::new(
        TaskInfo::init(
            GROUP,
            "b",
            None::<f64>,
            None::<f64>,
            Ttl::from_secs_f64(0.5),
        ),
        Bytes::from("b"),
    );

    backend
        .put_tasks(vec![task_a.clone(), task_b.clone()])
        .await?;

    let queued = backend.get_queued_tasks(GROUP, 10).await?;
    assert_eq!(queued.len(), 2);
    assert_eq!(queued[0].info.id, task_a.info.id);
    assert_eq!(queued[1].info.id, task_b.info.id);

    let task_ids = vec![
        task_a.info.id.clone(),
        task_b.info.id.clone(),
        TaskId::from("dummy"),
    ];
    let looked_up = backend.lookup_tasks(&task_ids).await?;
    assert_eq!(looked_up.len(), 3);
    assert!(looked_up[0].is_some());
    assert!(looked_up[1].is_some());
    assert!(looked_up[2].is_none());

    let worker_a = "a";
    let worker_b = "b";

    let assigned_a = backend.assign_task(GROUP, worker_a).await?;
    assert!(assigned_a.is_some());
    let assigned_a = assigned_a.unwrap();
    assert_eq!(assigned_a.info.id, task_a.info.id);

    let queued = backend.get_queued_tasks(GROUP, 10).await?;
    assert_eq!(queued.len(), 1);
    assert_eq!(queued[0].info.id, task_b.info.id);

    let assigned_b = backend.assign_task(GROUP, worker_b).await?;
    assert!(assigned_b.is_some());
    let assigned_b = assigned_b.unwrap();
    assert_eq!(assigned_b.info.id, task_b.info.id);

    let queued = backend.get_queued_tasks(GROUP, 10).await?;
    assert_eq!(queued.len(), 0);

    let stage_info = backend.get_stage_info(10).await?;
    assert_eq!(stage_info.len(), 2);
    assert_eq!(stage_info[0].task_id, task_a.info.id);
    assert_eq!(stage_info[0].worker_id.as_str(), worker_a);
    assert_eq!(stage_info[1].task_id, task_b.info.id);
    assert_eq!(stage_info[1].worker_id.as_str(), worker_b);

    let task_c = TaskRecord::new(
        TaskInfo::init(
            GROUP,
            "c",
            Some(Timestamp::now() + Duration::from_millis(500)),
            None::<f64>,
            DEFAULT_TASK_TTL,
        ),
        Bytes::from("c"),
    );
    backend.put_tasks(vec![task_c.clone()]).await?;

    let queued = backend.get_queued_tasks(GROUP, 10).await?;
    assert_eq!(queued.len(), 1);

    // Should not assign yet (not due)
    let assigned = backend.assign_task(GROUP, worker_a).await?;
    assert!(assigned.is_none());

    // Wait for due time
    sleep(Duration::from_millis(500)).await;

    let assigned_c = backend.assign_task(GROUP, worker_a).await?;
    assert!(assigned_c.is_some());
    let assigned_c = assigned_c.unwrap();
    assert_eq!(assigned_c.info.id, task_c.info.id);

    backend.discard_tasks(&[task_c.info.id.clone()]).await?;

    let stage_info = backend.get_stage_info(10).await?;
    assert_eq!(stage_info.len(), 2);

    // retry_task must receive TaskRecord from stage (assigned_a, not task_a)
    backend.retry_task(assigned_a.clone()).await?;

    let stage_info = backend.get_stage_info(10).await?;
    assert_eq!(stage_info.len(), 1);
    assert_eq!(stage_info[0].task_id, task_b.info.id);

    let queued = backend.get_queued_tasks(GROUP, 10).await?;
    assert_eq!(queued.len(), 1);
    assert_eq!(queued[0].info.id, task_a.info.id);

    // retry_task must receive TaskRecord from stage (assigned_b, not task_b)
    backend.retry_task(assigned_b.clone()).await?;

    let stage_info = backend.get_stage_info(10).await?;
    assert_eq!(stage_info.len(), 0);

    let queued = backend.get_queued_tasks(GROUP, 10).await?;
    assert_eq!(queued.len(), 2);

    let _ = backend.assign_task(GROUP, worker_a).await?;
    let _ = backend.assign_task(GROUP, worker_b).await?;

    let queued = backend.get_queued_tasks(GROUP, 10).await?;
    assert_eq!(queued.len(), 0);

    let stage_info = backend.get_stage_info(10).await?;
    assert_eq!(stage_info.len(), 2);

    backend.restore(stage_info[1].clone()).await?;
    let queued = backend.get_queued_tasks(GROUP, 10).await?;
    assert_eq!(queued.len(), 1);
    assert_eq!(queued[0].info.id, task_b.info.id);

    backend.restore(stage_info[0].clone()).await?;
    let queued = backend.get_queued_tasks(GROUP, 10).await?;
    assert_eq!(queued.len(), 2);

    let assigned_a = backend.assign_task(GROUP, worker_a).await?.unwrap();

    let queued = backend.get_queued_tasks(GROUP, 10).await?;
    assert_eq!(queued.len(), 1);
    assert_eq!(queued[0].info.id, task_b.info.id);

    let stage_info = backend.get_stage_info(10).await?;
    assert_eq!(stage_info.len(), 1);
    assert_eq!(stage_info[0].task_id, task_a.info.id);

    let result = backend.get_result(&task_a.info.id).await;
    match result {
        Err(GetResultError::NoResult(record)) => {
            assert_eq!(record.info.id, task_a.info.id);
        }
        _ => panic!("Expected NoResult error"),
    }

    // succeed must be given the TaskRecord returned by assign_task, not the original
    let result_data = Bytes::from("result");
    backend
        .succeed(assigned_a.clone(), result_data.clone())
        .await?;

    let (record, data) = backend.get_result(&task_a.info.id).await.unwrap();
    assert_eq!(record.info.id, task_a.info.id);
    assert_eq!(data, result_data);

    let done_ids = backend.get_done_task_ids(None, None, 10).await?;
    assert_eq!(done_ids.len(), 1);
    assert_eq!(done_ids[0], task_a.info.id);

    let disposable = backend.get_disposable_task_ids(10).await?;
    assert_eq!(disposable.len(), 0);

    // fail likewise needs the staged TaskRecord, so task_b must be assigned first
    let assigned_b = backend.assign_task(GROUP, worker_b).await?.unwrap();

    let error_message = Bytes::from("error");
    backend
        .fail(assigned_b.clone(), error_message.clone())
        .await?;

    let result = backend.get_result(&task_b.info.id).await;
    match result {
        Err(GetResultError::Failed { record, message }) => {
            assert_eq!(record.info.id, task_b.info.id);
            assert_eq!(message, String::from_utf8_lossy(&error_message));
        }
        _ => panic!("Expected Failed error"),
    }

    let done_ids = backend.get_done_task_ids(None, None, 10).await?;
    assert_eq!(
        done_ids,
        vec![task_a.info.id.clone(), task_b.info.id.clone()]
    );

    // task_b's ttl is 0.5
    let disposable = backend.get_disposable_task_ids(10).await?;
    assert_eq!(disposable.len(), 0);

    sleep(Duration::from_millis(500)).await;

    let disposable = backend.get_disposable_task_ids(10).await?;
    assert_eq!(disposable.len(), 1);
    assert_eq!(disposable[0], task_b.info.id);

    let queued = backend.get_queued_tasks(GROUP, 10).await?;
    assert_eq!(queued.len(), 0);

    let stage_info = backend.get_stage_info(10).await?;
    assert_eq!(stage_info.len(), 0);

    let result = backend.get_result(&TaskId::from("invalid_id")).await;
    match result {
        Err(GetResultError::NotFound) => {}
        _ => panic!("Expected NotFound error"),
    }

    let task_ids = vec![task_a.info.id.clone(), task_b.info.id.clone()];
    let looked_up = backend.lookup_tasks(&task_ids).await?;
    assert!(looked_up[0].is_some());
    assert!(looked_up[1].is_some());

    backend.discard_tasks(&[task_a.info.id.clone()]).await?;

    let looked_up = backend.lookup_tasks(&task_ids).await?;
    assert!(looked_up[0].is_none());
    assert!(looked_up[1].is_some());

    let result = backend.get_result(&task_a.info.id).await;
    match result {
        Err(GetResultError::NotFound) => {}
        _ => panic!("Expected NotFound error"),
    }

    backend.discard_tasks(&[task_b.info.id.clone()]).await?;

    let looked_up = backend.lookup_tasks(&task_ids).await?;
    assert!(looked_up[0].is_none());
    assert!(looked_up[1].is_none());

    let result = backend.get_result(&task_b.info.id).await;
    match result {
        Err(GetResultError::NotFound) => {}
        _ => panic!("Expected NotFound error"),
    }

    Ok(())
}

pub async fn test_assign_tasks(backend: &Arc<dyn Backend>) -> Result<(), BackendError> {
    backend.destroy_all().await?;

    let base = Timestamp::now() - Duration::from_secs(60);
    let due: Vec<TaskRecord> = (0..5)
        .map(|i| {
            TaskRecord::new(
                TaskInfo::init(
                    GROUP,
                    "batch",
                    Some(base + Duration::from_secs(i)),
                    None::<f64>,
                    DEFAULT_TASK_TTL,
                ),
                Bytes::from(format!("d{i}")),
            )
        })
        .collect();
    let later = TaskRecord::new(
        TaskInfo::init(
            GROUP,
            "later",
            Some(Timestamp::now() + Duration::from_secs(300)),
            None::<f64>,
            DEFAULT_TASK_TTL,
        ),
        Bytes::from("later"),
    );

    let mut all = due.clone();
    all.push(later.clone());
    backend.put_tasks(all).await?;

    let ids = |records: &[TaskRecord]| -> Vec<TaskId> {
        records.iter().map(|r| r.info.id.clone()).collect()
    };

    assert!(backend.assign_tasks(GROUP, "w0", 0).await?.is_empty());

    let first = backend.assign_tasks(GROUP, "w1", 3).await?;
    assert_eq!(ids(&first), ids(&due[..3]));
    assert_eq!(first[0].data, due[0].data);

    let staged = backend.get_stage_info(10).await?;
    assert_eq!(staged.len(), 3);
    assert!(staged.iter().all(|s| s.worker_id.as_str() == "w1"));

    // Fewer records than requested is how a caller learns the queue is drained.
    let second = backend.assign_tasks(GROUP, "w2", 10).await?;
    assert_eq!(ids(&second), ids(&due[3..]));

    // The task that is not due yet stays queued rather than padding the batch.
    assert!(backend.assign_tasks(GROUP, "w3", 10).await?.is_empty());
    let queued = backend.get_queued_tasks(GROUP, 10).await?;
    assert_eq!(ids(&queued), vec![later.info.id.clone()]);

    Ok(())
}

pub async fn test_put_tasks_ignores_known_ids(
    backend: &Arc<dyn Backend>,
) -> Result<(), BackendError> {
    backend.destroy_all().await?;

    let task = TaskRecord::new(
        TaskInfo::init(GROUP, "dup", None::<f64>, None::<f64>, DEFAULT_TASK_TTL),
        Bytes::from("original"),
    );
    backend.put_tasks(vec![task.clone()]).await?;

    let resubmitted = TaskRecord::new(task.info.clone(), Bytes::from("resubmitted"));
    backend.put_tasks(vec![resubmitted.clone()]).await?;

    let queued = backend.get_queued_tasks(GROUP, 10).await?;
    assert_eq!(queued.len(), 1, "a known id must not be queued twice");
    assert_eq!(
        queued[0].data, task.data,
        "the stored task is left as it was"
    );

    // Once a task is on the stage, putting it again must not make it runnable again.
    let assigned = backend.assign_task(GROUP, "w1").await?.expect("due task");
    assert_eq!(assigned.info.id, task.info.id);

    backend.put_tasks(vec![resubmitted]).await?;

    assert!(backend.get_queued_tasks(GROUP, 10).await?.is_empty());
    assert!(backend.assign_task(GROUP, "w2").await?.is_none());
    assert_eq!(backend.get_stage_info(10).await?.len(), 1);

    Ok(())
}

pub async fn test_get_lock(backend: &Arc<dyn Backend>) -> Result<(), BackendError> {
    backend.destroy_all().await?;

    let lock_a = backend.get_lock("a").await?;
    assert!(lock_a.acquire().await);

    lock_a.release().await;
    assert!(lock_a.acquire().await);

    // Test from another task (simulating another thread/process)
    let backend_clone = backend.clone();
    let handle = tokio::spawn(async move {
        let lock_a = backend_clone.get_lock("a").await.unwrap();
        let lock_b = backend_clone.get_lock("b").await.unwrap();

        assert!(!lock_a.acquire().await);
        assert!(lock_b.acquire().await);
        lock_b.release().await;
    });

    handle.await.unwrap();
    lock_a.release().await;

    Ok(())
}

pub async fn test_scheduler(backend: &Arc<dyn Backend>) -> Result<(), BackendError> {
    backend.destroy_all().await?;

    let scheduled_a = Timestamp::now();
    let task_a = TaskRecord::new(
        TaskInfo::init(GROUP, "a", None::<f64>, Some(scheduled_a), DEFAULT_TASK_TTL),
        Bytes::from("a"),
    );

    let scheduled_b = Timestamp::now();
    let task_b = TaskRecord::new(
        TaskInfo::init(GROUP, "b", None::<f64>, Some(scheduled_b), DEFAULT_TASK_TTL),
        Bytes::from("b"),
    );

    let name = SchedulerName::from("scheduler_name");
    let scheduler_state_data = Bytes::from("test");

    backend
        .persist_scheduler_state_and_put_tasks(
            &name,
            scheduler_state_data.clone(),
            vec![task_a.clone(), task_b.clone()],
        )
        .await?;

    let state = backend.get_scheduler_state(&name).await?;
    assert_eq!(state, Some(scheduler_state_data.to_vec()));

    let tasks = backend.get_queued_tasks(GROUP, 10).await?;
    assert_eq!(tasks.len(), 2);
    assert_eq!(tasks[0].info.id, task_a.info.id);
    assert_eq!(tasks[1].info.id, task_b.info.id);

    Ok(())
}

#[cfg(feature = "redis")]
mod redis_tests {
    use super::*;
    use taskkit::RedisBackend;
    use tokio::time::timeout;

    async fn create_redis_backend(db: i64) -> Option<Arc<dyn Backend>> {
        let base_url =
            std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1".to_string());

        let redis_url = format!("{}/{}", base_url.trim_end_matches('/'), db);

        let client = match redis::Client::open(redis_url.as_str()) {
            Ok(c) => c,
            Err(e) => {
                eprintln!("Failed to create Redis client: {}", e);
                return None;
            }
        };

        match timeout(Duration::from_secs(2), RedisBackend::new(client)).await {
            Ok(Ok(backend)) => Some(Arc::new(backend)),
            Ok(Err(e)) => {
                eprintln!("Failed to connect to Redis: {}", e);
                None
            }
            Err(_) => {
                eprintln!("Redis connection timed out. Is Redis running?");
                None
            }
        }
    }

    #[tokio::test]
    async fn redis_test_workers() {
        let backend = create_redis_backend(0)
            .await
            .expect("Redis not available. Please start Redis server.");
        test_workers(&backend).await.unwrap();
    }

    #[tokio::test]
    async fn redis_test_tasks() {
        let backend = create_redis_backend(1)
            .await
            .expect("Redis not available. Please start Redis server.");
        test_tasks(&backend).await.unwrap();
    }

    #[tokio::test]
    async fn redis_test_get_lock() {
        let backend = create_redis_backend(2)
            .await
            .expect("Redis not available. Please start Redis server.");
        test_get_lock(&backend).await.unwrap();
    }

    #[tokio::test]
    async fn redis_test_scheduler() {
        let backend = create_redis_backend(3)
            .await
            .expect("Redis not available. Please start Redis server.");
        test_scheduler(&backend).await.unwrap();
    }

    #[tokio::test]
    async fn redis_test_assign_tasks() {
        let backend = create_redis_backend(4)
            .await
            .expect("Redis not available. Please start Redis server.");
        test_assign_tasks(&backend).await.unwrap();
    }

    #[tokio::test]
    async fn redis_test_put_tasks_ignores_known_ids() {
        let backend = create_redis_backend(5)
            .await
            .expect("Redis not available. Please start Redis server.");
        test_put_tasks_ignores_known_ids(&backend).await.unwrap();
    }
}

#[cfg(feature = "mysql")]
mod mysql_tests {
    use super::*;
    use sqlx::MySqlPool;
    use taskkit::impls::mysql;
    use tokio::time::timeout;

    struct MysqlTestContext {
        backend: Arc<dyn Backend>,
        server_pool: MySqlPool,
        db_name: String,
    }

    impl MysqlTestContext {
        async fn cleanup(self) {
            // The database cannot be dropped while the backend still holds connections.
            drop(self.backend);

            tokio::time::sleep(Duration::from_millis(200)).await;

            let drop_db = format!("DROP DATABASE IF EXISTS {}", self.db_name);
            match sqlx::query(&drop_db).execute(&self.server_pool).await {
                Ok(_) => eprintln!("✓ Cleaned up database: {}", self.db_name),
                Err(e) => eprintln!("✗ Failed to cleanup database {}: {}", self.db_name, e),
            }
        }
    }

    async fn create_mysql_backend() -> Option<MysqlTestContext> {
        use std::sync::atomic::{AtomicU64, Ordering};
        use std::time::{SystemTime, UNIX_EPOCH};
        static TEST_DB_COUNTER: AtomicU64 = AtomicU64::new(0);

        let base_url = std::env::var("MYSQL_URL")
            .unwrap_or_else(|_| "mysql://root:password@127.0.0.1:3306".to_string());

        let test_id = TEST_DB_COUNTER.fetch_add(1, Ordering::SeqCst);
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let db_name = format!("taskkit_test_{}_{}", timestamp, test_id);

        // Connect to MySQL server (without database)
        let server_pool = match timeout(Duration::from_secs(5), MySqlPool::connect(&base_url)).await
        {
            Ok(Ok(pool)) => pool,
            Ok(Err(e)) => {
                eprintln!("Failed to connect to MySQL: {}", e);
                eprintln!("Make sure MySQL is running at: {}", base_url);
                return None;
            }
            Err(_) => {
                eprintln!("MySQL connection timed out. Is MySQL running?");
                return None;
            }
        };

        let create_db = format!("CREATE DATABASE {}", db_name);
        if let Err(e) = sqlx::query(&create_db).execute(&server_pool).await {
            eprintln!("Failed to create test database: {}", e);
            return None;
        }

        let test_db_url = format!("{}/{}", base_url.trim_end_matches('/'), db_name);
        let test_pool =
            match timeout(Duration::from_secs(5), MySqlPool::connect(&test_db_url)).await {
                Ok(Ok(pool)) => pool,
                Ok(Err(e)) => {
                    eprintln!("Failed to connect to test database: {}", e);
                    let drop_db = format!("DROP DATABASE IF EXISTS {}", db_name);
                    let _ = sqlx::query(&drop_db).execute(&server_pool).await;
                    return None;
                }
                Err(_) => {
                    eprintln!("Test database connection timed out");
                    let drop_db = format!("DROP DATABASE IF EXISTS {}", db_name);
                    let _ = sqlx::query(&drop_db).execute(&server_pool).await;
                    return None;
                }
            };

        match timeout(
            Duration::from_secs(10),
            sqlx::migrate!("./migrations/mysql").run(&test_pool),
        )
        .await
        {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => {
                eprintln!("Failed to run migrations: {}", e);
                test_pool.close().await;
                let drop_db = format!("DROP DATABASE IF EXISTS {}", db_name);
                let _ = sqlx::query(&drop_db).execute(&server_pool).await;
                return None;
            }
            Err(_) => {
                eprintln!("Migration timed out");
                test_pool.close().await;
                let drop_db = format!("DROP DATABASE IF EXISTS {}", db_name);
                let _ = sqlx::query(&drop_db).execute(&server_pool).await;
                return None;
            }
        }

        let backend = Arc::new(mysql::with_pool(test_pool));

        Some(MysqlTestContext {
            backend,
            server_pool,
            db_name,
        })
    }

    #[tokio::test]
    async fn mysql_test_workers() {
        let ctx = create_mysql_backend()
            .await
            .expect("MySQL not available. Please start MySQL server.");
        test_workers(&ctx.backend).await.unwrap();
        ctx.cleanup().await;
    }

    #[tokio::test]
    async fn mysql_test_tasks() {
        let ctx = create_mysql_backend()
            .await
            .expect("MySQL not available. Please start MySQL server.");
        test_tasks(&ctx.backend).await.unwrap();
        ctx.cleanup().await;
    }

    #[tokio::test]
    async fn mysql_test_get_lock() {
        let ctx = create_mysql_backend()
            .await
            .expect("MySQL not available. Please start MySQL server.");
        test_get_lock(&ctx.backend).await.unwrap();
        ctx.cleanup().await;
    }

    #[tokio::test]
    async fn mysql_test_scheduler() {
        let ctx = create_mysql_backend()
            .await
            .expect("MySQL not available. Please start MySQL server.");
        test_scheduler(&ctx.backend).await.unwrap();
        ctx.cleanup().await;
    }

    #[tokio::test]
    async fn mysql_test_assign_tasks() {
        let ctx = create_mysql_backend()
            .await
            .expect("MySQL not available. Please start MySQL server.");
        test_assign_tasks(&ctx.backend).await.unwrap();
        ctx.cleanup().await;
    }

    #[tokio::test]
    async fn mysql_test_put_tasks_ignores_known_ids() {
        let ctx = create_mysql_backend()
            .await
            .expect("MySQL not available. Please start MySQL server.");
        test_put_tasks_ignores_known_ids(&ctx.backend)
            .await
            .unwrap();
        ctx.cleanup().await;
    }
}

#[cfg(feature = "postgres")]
mod postgres_tests {
    use super::*;
    use sqlx::PgPool;
    use taskkit::impls::postgres;
    use tokio::time::timeout;

    struct PostgresTestContext {
        backend: Arc<dyn Backend>,
        server_pool: PgPool,
        db_name: String,
    }

    impl PostgresTestContext {
        async fn cleanup(self) {
            // The database cannot be dropped while the backend still holds connections.
            drop(self.backend);

            tokio::time::sleep(Duration::from_millis(200)).await;

            let drop_db = format!("DROP DATABASE IF EXISTS {}", self.db_name);
            match sqlx::query(&drop_db).execute(&self.server_pool).await {
                Ok(_) => eprintln!("✓ Cleaned up database: {}", self.db_name),
                Err(e) => eprintln!("✗ Failed to cleanup database {}: {}", self.db_name, e),
            }
        }
    }

    async fn create_postgres_backend() -> Option<PostgresTestContext> {
        use std::sync::atomic::{AtomicU64, Ordering};
        use std::time::{SystemTime, UNIX_EPOCH};
        static TEST_DB_COUNTER: AtomicU64 = AtomicU64::new(0);

        let base_url = std::env::var("POSTGRES_URL")
            .unwrap_or_else(|_| "postgresql://postgres:password@127.0.0.1:5432".to_string());

        let test_id = TEST_DB_COUNTER.fetch_add(1, Ordering::SeqCst);
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let db_name = format!("taskkit_test_{}_{}", timestamp, test_id);

        // Connect to PostgreSQL server (default postgres database)
        let server_url = format!("{}/postgres", base_url.trim_end_matches('/'));
        let server_pool = match timeout(Duration::from_secs(5), PgPool::connect(&server_url)).await
        {
            Ok(Ok(pool)) => pool,
            Ok(Err(e)) => {
                eprintln!("Failed to connect to PostgreSQL: {}", e);
                eprintln!("Make sure PostgreSQL is running at: {}", base_url);
                return None;
            }
            Err(_) => {
                eprintln!("PostgreSQL connection timed out. Is PostgreSQL running?");
                return None;
            }
        };

        let create_db = format!("CREATE DATABASE {}", db_name);
        if let Err(e) = sqlx::query(&create_db).execute(&server_pool).await {
            eprintln!("Failed to create test database: {}", e);
            return None;
        }

        let test_db_url = format!("{}/{}", base_url.trim_end_matches('/'), db_name);
        let test_pool = match timeout(Duration::from_secs(5), PgPool::connect(&test_db_url)).await {
            Ok(Ok(pool)) => pool,
            Ok(Err(e)) => {
                eprintln!("Failed to connect to test database: {}", e);
                let drop_db = format!("DROP DATABASE IF EXISTS {}", db_name);
                let _ = sqlx::query(&drop_db).execute(&server_pool).await;
                return None;
            }
            Err(_) => {
                eprintln!("Test database connection timed out");
                let drop_db = format!("DROP DATABASE IF EXISTS {}", db_name);
                let _ = sqlx::query(&drop_db).execute(&server_pool).await;
                return None;
            }
        };

        match timeout(
            Duration::from_secs(10),
            sqlx::migrate!("./migrations/postgres").run(&test_pool),
        )
        .await
        {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => {
                eprintln!("Failed to run migrations: {}", e);
                test_pool.close().await;
                let drop_db = format!("DROP DATABASE IF EXISTS {}", db_name);
                let _ = sqlx::query(&drop_db).execute(&server_pool).await;
                return None;
            }
            Err(_) => {
                eprintln!("Migration timed out");
                test_pool.close().await;
                let drop_db = format!("DROP DATABASE IF EXISTS {}", db_name);
                let _ = sqlx::query(&drop_db).execute(&server_pool).await;
                return None;
            }
        }

        let backend = Arc::new(postgres::with_pool(test_pool));

        Some(PostgresTestContext {
            backend,
            server_pool,
            db_name,
        })
    }

    #[tokio::test]
    async fn postgres_test_workers() {
        let ctx = create_postgres_backend()
            .await
            .expect("PostgreSQL not available. Please start PostgreSQL server.");
        test_workers(&ctx.backend).await.unwrap();
        ctx.cleanup().await;
    }

    #[tokio::test]
    async fn postgres_test_tasks() {
        let ctx = create_postgres_backend()
            .await
            .expect("PostgreSQL not available. Please start PostgreSQL server.");
        test_tasks(&ctx.backend).await.unwrap();
        ctx.cleanup().await;
    }

    #[tokio::test]
    async fn postgres_test_get_lock() {
        let ctx = create_postgres_backend()
            .await
            .expect("PostgreSQL not available. Please start PostgreSQL server.");
        test_get_lock(&ctx.backend).await.unwrap();
        ctx.cleanup().await;
    }

    #[tokio::test]
    async fn postgres_test_scheduler() {
        let ctx = create_postgres_backend()
            .await
            .expect("PostgreSQL not available. Please start PostgreSQL server.");
        test_scheduler(&ctx.backend).await.unwrap();
        ctx.cleanup().await;
    }

    #[tokio::test]
    async fn postgres_test_assign_tasks() {
        let ctx = create_postgres_backend()
            .await
            .expect("PostgreSQL not available. Please start PostgreSQL server.");
        test_assign_tasks(&ctx.backend).await.unwrap();
        ctx.cleanup().await;
    }

    #[tokio::test]
    async fn postgres_test_put_tasks_ignores_known_ids() {
        let ctx = create_postgres_backend()
            .await
            .expect("PostgreSQL not available. Please start PostgreSQL server.");
        test_put_tasks_ignores_known_ids(&ctx.backend)
            .await
            .unwrap();
        ctx.cleanup().await;
    }
}

#[cfg(feature = "memory")]
mod memory_tests {
    use super::*;
    use taskkit::MemoryBackend;

    fn backend() -> Arc<dyn Backend> {
        Arc::new(MemoryBackend::new())
    }

    #[tokio::test]
    async fn memory_test_workers() {
        test_workers(&backend()).await.unwrap();
    }

    #[tokio::test]
    async fn memory_test_tasks() {
        test_tasks(&backend()).await.unwrap();
    }

    #[tokio::test]
    async fn memory_test_get_lock() {
        test_get_lock(&backend()).await.unwrap();
    }

    #[tokio::test]
    async fn memory_test_scheduler() {
        test_scheduler(&backend()).await.unwrap();
    }

    #[tokio::test]
    async fn memory_test_assign_tasks() {
        test_assign_tasks(&backend()).await.unwrap();
    }

    #[tokio::test]
    async fn memory_test_put_tasks_ignores_known_ids() {
        test_put_tasks_ignores_known_ids(&backend()).await.unwrap();
    }
}
