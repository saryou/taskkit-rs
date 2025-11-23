use async_trait::async_trait;
use bytes::Bytes;
use futures_util::stream::{self, Stream, StreamExt};
use redis::aio::ConnectionManager;
use redis::{AsyncCommands, RedisError, Script};
use serde_json;
use std::collections::{HashMap, HashSet};
use std::pin::Pin;
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::backend::{
    Backend, BackendError, ControlEvent, EventBridge, GetResultError, Lock, LockProvider,
    ReceiveEventsError, SendEventError, TaskBackend, WorkerTracker,
};
use crate::scheduler::SchedulerName;
use crate::stage::StageInfo;
use crate::task::{TaskGroup, TaskId, TaskInfo, TaskRecord};
use crate::util::Timestamp;
use crate::worker::WorkerId;

const KEY_PREFIX: &str = "taskkit.backend";
const EVENT_CHANNEL: &str = "taskkit.redis_bridge.channel";

pub struct RedisBackend {
    client: redis::Client,
    conn: ConnectionManager,
}

/// Whether a task id the backend already knows should be re-queued.
enum OnKnownId {
    /// Leave the stored task alone. A task that is already staged or finished must
    /// not be pushed back onto the queue by a repeated submission.
    Keep,
    /// Drop what is stored and queue the task again, as a retry does.
    Replace,
}

impl RedisBackend {
    pub async fn new(client: redis::Client) -> Result<Self, RedisError> {
        let conn = client.get_connection_manager().await?;
        Ok(Self { client, conn })
    }

    fn workers_key(&self) -> String {
        format!("{KEY_PREFIX}.workers")
    }

    fn queue_key(&self, group: &str) -> String {
        format!("{KEY_PREFIX}.{group}.queue")
    }

    fn data_store_key(&self) -> String {
        format!("{KEY_PREFIX}.data_store")
    }

    fn task_info_key(&self) -> String {
        format!("{KEY_PREFIX}.task_info")
    }

    fn stage_queue_key(&self) -> String {
        format!("{KEY_PREFIX}.stage_queue")
    }

    fn stage_info_key(&self) -> String {
        format!("{KEY_PREFIX}.stage_info")
    }

    fn done_queue_key(&self) -> String {
        format!("{KEY_PREFIX}.done_queue")
    }

    fn disposable_queue_key(&self) -> String {
        format!("{KEY_PREFIX}.disposable_queue")
    }

    fn result_key(&self) -> String {
        format!("{KEY_PREFIX}.result")
    }

    fn error_message_key(&self) -> String {
        format!("{KEY_PREFIX}.error_message")
    }

    fn scheduler_data_key(&self, name: &SchedulerName) -> String {
        format!("{KEY_PREFIX}.scheduler.{}", name.as_str())
    }

    fn lock_key_prefix(&self, target: &str) -> String {
        format!("{KEY_PREFIX}.locks.{target}")
    }

    fn convert_redis_error(e: RedisError) -> BackendError {
        if e.is_connection_refusal()
            || e.is_connection_dropped()
            || e.is_timeout()
            || e.is_io_error()
        {
            BackendError::Unavailable(e.to_string())
        } else {
            BackendError::OperationFailed(e.to_string())
        }
    }

    fn encode_task_info(&self, info: &TaskInfo) -> Result<String, BackendError> {
        serde_json::to_string(info).map_err(|e| BackendError::Serialization(e.to_string()))
    }

    fn decode_task_info(&self, data: &str) -> Result<TaskInfo, BackendError> {
        serde_json::from_str(data).map_err(|e| BackendError::Deserialization(e.to_string()))
    }

    /// Writes tasks onto their group queue, optionally alongside a scheduler state
    /// write, in one atomic step.
    async fn enqueue_tasks(
        &self,
        scheduler_state: Option<(&SchedulerName, Bytes)>,
        tasks: &[TaskRecord],
        on_known_id: OnKnownId,
    ) -> Result<(), BackendError> {
        let script = Script::new(
            r#"
local task_info_key   = KEYS[1]
local data_store_key  = KEYS[2]
local stage_queue_key = KEYS[3]
local stage_info_key  = KEYS[4]
local state_key       = KEYS[5]

local state   = ARGV[1]
local replace = ARGV[2]

-- An empty state key means this call carries no scheduler state.
if state_key ~= '' then
    redis.call('SET', state_key, state)
end

for i = 3, #ARGV, 5 do
    local queue_key = ARGV[i]
    local task_id   = ARGV[i + 1]
    local due       = ARGV[i + 2]
    local info      = ARGV[i + 3]
    local data      = ARGV[i + 4]

    -- Forgetting the task first is what lets it pass the guard below, so the guard
    -- itself stays unconditional.
    if replace == '1' then
        redis.call('ZREM', stage_queue_key, task_id)
        redis.call('HDEL', stage_info_key, task_id)
        redis.call('HDEL', task_info_key, task_id)
    end

    if redis.call('HSETNX', task_info_key, task_id, info) == 1 then
        redis.call('HSET', data_store_key, task_id, data)
        redis.call('ZADD', queue_key, due, task_id)
    end
end
"#,
        );

        let mut invocation = script.prepare_invoke();
        invocation
            .key(self.task_info_key())
            .key(self.data_store_key())
            .key(self.stage_queue_key())
            .key(self.stage_info_key());

        match scheduler_state {
            Some((name, data)) => {
                invocation
                    .key(self.scheduler_data_key(name))
                    .arg(data.as_ref());
            }
            None => {
                invocation.key("").arg("");
            }
        }

        invocation.arg(match on_known_id {
            OnKnownId::Keep => "0",
            OnKnownId::Replace => "1",
        });

        for task in tasks {
            invocation
                .arg(self.queue_key(task.info.group.as_str()))
                .arg(task.info.id.as_str())
                .arg(task.info.due.as_secs_f64())
                .arg(self.encode_task_info(&task.info)?)
                .arg(task.data.as_ref());
        }

        invocation
            .invoke_async::<()>(&mut self.conn.clone())
            .await
            .map_err(Self::convert_redis_error)
    }
}

#[async_trait]
impl WorkerTracker for RedisBackend {
    async fn set_worker_ttl(
        &self,
        worker_ids: HashSet<WorkerId>,
        expires_at: Timestamp,
    ) -> Result<(), BackendError> {
        if worker_ids.is_empty() {
            return Ok(());
        }

        let mut conn = self.conn.clone();
        let items: Vec<(f64, String)> = worker_ids
            .into_iter()
            .map(|id| (expires_at.as_secs_f64(), id.as_str().to_string()))
            .collect();

        conn.zadd_multiple::<_, _, _, ()>(self.workers_key(), &items)
            .await
            .map_err(Self::convert_redis_error)?;

        Ok(())
    }

    async fn get_workers(&self) -> Result<Vec<(WorkerId, Timestamp)>, BackendError> {
        let mut conn = self.conn.clone();

        let items: Vec<(String, f64)> = conn
            .zrange_withscores(self.workers_key(), 0, -1)
            .await
            .map_err(Self::convert_redis_error)?;

        Ok(items
            .into_iter()
            .map(|(id, ttl)| (WorkerId::new(id), Timestamp::from_secs_f64(ttl)))
            .collect())
    }

    async fn purge_workers(&self, worker_ids: HashSet<WorkerId>) -> Result<(), BackendError> {
        if worker_ids.is_empty() {
            return Ok(());
        }

        let mut conn = self.conn.clone();
        let ids: Vec<&str> = worker_ids.iter().map(|id| id.as_str()).collect();

        conn.zrem::<_, _, ()>(self.workers_key(), ids)
            .await
            .map_err(Self::convert_redis_error)?;

        Ok(())
    }
}

#[async_trait]
impl TaskBackend for RedisBackend {
    async fn put_tasks(&self, tasks: Vec<TaskRecord>) -> Result<(), BackendError> {
        if tasks.is_empty() {
            return Ok(());
        }
        self.enqueue_tasks(None, &tasks, OnKnownId::Keep).await
    }

    async fn retry_task(&self, record: TaskRecord) -> Result<(), BackendError> {
        self.enqueue_tasks(None, &[record], OnKnownId::Replace)
            .await
    }

    async fn get_queued_tasks(
        &self,
        group: &str,
        limit: usize,
    ) -> Result<Vec<TaskRecord>, BackendError> {
        let mut conn = self.conn.clone();

        let ids: Vec<String> = conn
            .zrange(self.queue_key(group), 0, limit as isize)
            .await
            .map_err(Self::convert_redis_error)?;

        let task_ids: Vec<TaskId> = ids.iter().map(|s| TaskId::new(s.clone())).collect();
        let tasks = self.lookup_tasks(&task_ids).await?;

        Ok(ids
            .into_iter()
            .filter_map(|id| {
                tasks
                    .iter()
                    .find(|t| t.as_ref().map(|t| t.info.id.as_str()) == Some(id.as_str()))
                    .and_then(|t| t.clone())
            })
            .collect())
    }

    async fn lookup_tasks(
        &self,
        task_ids: &[TaskId],
    ) -> Result<Vec<Option<TaskRecord>>, BackendError> {
        if task_ids.is_empty() {
            return Ok(vec![]);
        }

        let mut conn = self.conn.clone();
        let ids: Vec<&str> = task_ids.iter().map(|id| id.as_str()).collect();

        let infos: Vec<Option<String>> = conn
            .hget(self.task_info_key(), &ids)
            .await
            .map_err(Self::convert_redis_error)?;

        let data: Vec<Option<Vec<u8>>> = conn
            .hget(self.data_store_key(), &ids)
            .await
            .map_err(Self::convert_redis_error)?;

        Ok(task_ids
            .iter()
            .zip(infos.iter().zip(data.iter()))
            .map(|(_, (info, data))| match (info, data) {
                (Some(info_str), Some(bytes)) => {
                    let info = self.decode_task_info(info_str).ok()?;
                    Some(TaskRecord::new(info, Bytes::from(bytes.clone())))
                }
                _ => None,
            })
            .collect())
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

        let mut conn = self.conn.clone();
        let now = Timestamp::now();

        let placeholder_stage = StageInfo {
            worker_id: WorkerId::new(worker_id),
            task_id: TaskId::new("${TASK_ID}"),
            assigned_at: now,
        };
        let placeholder_json = placeholder_stage.to_json();

        let script = Script::new(
            r#"
local queue_key       = KEYS[1]
local task_info_key   = KEYS[2]
local data_store_key  = KEYS[3]
local stage_queue_key = KEYS[4]
local stage_info_key  = KEYS[5]

local now             = tonumber(ARGV[1])
local json_template   = ARGV[2]
local limit           = tonumber(ARGV[3])

local assigned = {}
local candidates = redis.call('ZRANGE', queue_key, 0, limit - 1, 'WITHSCORES')

for i = 1, #candidates, 2 do
    local task_id = candidates[i]
    local score   = tonumber(candidates[i + 1])

    if score > now then
        break
    end

    local info = redis.call('HGET', task_info_key, task_id)
    local data = redis.call('HGET', data_store_key, task_id)

    if info and data then
        redis.call('ZREM', queue_key, task_id)
        redis.call('ZADD', stage_queue_key, score, task_id)

        local stage_json = string.gsub(json_template, "${TASK_ID}", task_id)
        redis.call('HSET', stage_info_key, task_id, stage_json)

        assigned[#assigned + 1] = info
        assigned[#assigned + 1] = data
    end
end

return assigned
"#,
        );

        let flattened: Vec<Vec<u8>> = script
            .key(self.queue_key(group))
            .key(self.task_info_key())
            .key(self.data_store_key())
            .key(self.stage_queue_key())
            .key(self.stage_info_key())
            .arg(now.as_secs_f64())
            .arg(placeholder_json)
            .arg(limit)
            .invoke_async(&mut conn)
            .await
            .map_err(Self::convert_redis_error)?;

        // The script returns info and data interleaved, one pair per assigned task.
        flattened
            .chunks_exact(2)
            .map(|pair| {
                let info = std::str::from_utf8(&pair[0])
                    .map_err(|e| BackendError::Deserialization(e.to_string()))?;
                Ok(TaskRecord::new(
                    self.decode_task_info(info)?,
                    Bytes::from(pair[1].clone()),
                ))
            })
            .collect()
    }

    async fn discard_tasks(&self, task_ids: &[TaskId]) -> Result<(), BackendError> {
        if task_ids.is_empty() {
            return Ok(());
        }

        let mut conn = self.conn.clone();

        let tasks = self.lookup_tasks(task_ids).await?;
        let mut pipe = redis::pipe();
        pipe.atomic();

        let ids: Vec<&str> = task_ids.iter().map(|t| t.as_str()).collect();

        let mut grouped: HashMap<TaskGroup, Vec<&str>> = HashMap::new();
        for (task, id) in tasks.iter().zip(ids.iter()) {
            if let Some(task) = task {
                grouped
                    .entry(task.info.group.clone())
                    .or_default()
                    .push(*id);
            }
        }

        for (group, ids) in grouped {
            pipe.zrem(self.queue_key(group.as_str()), ids.clone());
        }

        pipe.zrem(self.stage_queue_key(), ids.clone());
        pipe.hdel(self.stage_info_key(), ids.clone());
        pipe.hdel(self.data_store_key(), ids.clone());
        pipe.hdel(self.task_info_key(), ids.clone());
        pipe.zrem(self.done_queue_key(), ids.clone());
        pipe.zrem(self.disposable_queue_key(), ids.clone());
        pipe.hdel(self.result_key(), ids.clone());
        pipe.hdel(self.error_message_key(), ids);

        pipe.query_async::<()>(&mut conn)
            .await
            .map_err(Self::convert_redis_error)?;

        Ok(())
    }

    async fn succeed(&self, record: TaskRecord, result: Bytes) -> Result<(), BackendError> {
        let mut conn = self.conn.clone();
        let mut pipe = redis::pipe();
        pipe.atomic();

        let done = Timestamp::now().as_secs_f64();
        let disposable_at = done + record.info.ttl.as_duration().as_secs_f64();

        pipe.zrem(
            self.queue_key(record.info.group.as_str()),
            record.info.id.as_str(),
        );
        pipe.zrem(self.stage_queue_key(), record.info.id.as_str());
        pipe.hdel(self.stage_info_key(), record.info.id.as_str());
        pipe.zadd(self.done_queue_key(), record.info.id.as_str(), done);
        pipe.zadd(
            self.disposable_queue_key(),
            record.info.id.as_str(),
            disposable_at,
        );
        pipe.hset(self.result_key(), record.info.id.as_str(), result.as_ref());

        pipe.hset(
            self.data_store_key(),
            record.info.id.as_str(),
            record.data.as_ref(),
        );
        pipe.hset(
            self.task_info_key(),
            record.info.id.as_str(),
            self.encode_task_info(&record.info)?,
        );

        pipe.query_async::<()>(&mut conn)
            .await
            .map_err(Self::convert_redis_error)?;
        Ok(())
    }

    async fn fail(&self, record: TaskRecord, error: Bytes) -> Result<(), BackendError> {
        let mut conn = self.conn.clone();
        let mut pipe = redis::pipe();
        pipe.atomic();

        let done = Timestamp::now().as_secs_f64();
        let disposable_at = done + record.info.ttl.as_duration().as_secs_f64();

        pipe.zrem(
            self.queue_key(record.info.group.as_str()),
            record.info.id.as_str(),
        );
        pipe.zrem(self.stage_queue_key(), record.info.id.as_str());
        pipe.hdel(self.stage_info_key(), record.info.id.as_str());
        pipe.zadd(self.done_queue_key(), record.info.id.as_str(), done);
        pipe.zadd(
            self.disposable_queue_key(),
            record.info.id.as_str(),
            disposable_at,
        );
        pipe.hset(
            self.error_message_key(),
            record.info.id.as_str(),
            error.as_ref(),
        );

        pipe.hset(
            self.data_store_key(),
            record.info.id.as_str(),
            record.data.as_ref(),
        );
        pipe.hset(
            self.task_info_key(),
            record.info.id.as_str(),
            self.encode_task_info(&record.info)?,
        );

        pipe.query_async::<()>(&mut conn)
            .await
            .map_err(Self::convert_redis_error)?;
        Ok(())
    }

    async fn get_result(&self, task_id: &TaskId) -> Result<(TaskRecord, Bytes), GetResultError> {
        let tasks = self.lookup_tasks(std::slice::from_ref(task_id)).await?;
        let Some(record) = tasks.into_iter().next().flatten() else {
            return Err(GetResultError::NotFound);
        };

        let mut conn = self.conn.clone();

        if let Some(data) = conn
            .hget::<_, _, Option<Vec<u8>>>(self.result_key(), task_id.as_str())
            .await
            .map_err(|e| GetResultError::Backend(Self::convert_redis_error(e)))?
        {
            return Ok((record, Bytes::from(data)));
        }

        if let Some(err) = conn
            .hget::<_, _, Option<Vec<u8>>>(self.error_message_key(), task_id.as_str())
            .await
            .map_err(|e| GetResultError::Backend(Self::convert_redis_error(e)))?
        {
            let msg = String::from_utf8_lossy(&err).to_string();
            return Err(GetResultError::Failed {
                record,
                message: msg,
            });
        }

        Err(GetResultError::NoResult(record))
    }

    async fn get_done_task_ids(
        &self,
        since: Option<Timestamp>,
        until: Option<Timestamp>,
        limit: usize,
    ) -> Result<Vec<TaskId>, BackendError> {
        let mut conn = self.conn.clone();

        let min = since.map(|t| t.as_secs_f64()).unwrap_or(0.0);
        let max = until
            .map(|t| t.as_secs_f64())
            .unwrap_or_else(|| Timestamp::now().as_secs_f64());

        let ids: Vec<String> = conn
            .zrangebyscore_limit(self.done_queue_key(), min, max, 0, limit as isize)
            .await
            .map_err(Self::convert_redis_error)?;

        Ok(ids.into_iter().map(TaskId::new).collect())
    }

    async fn get_disposable_task_ids(&self, limit: usize) -> Result<Vec<TaskId>, BackendError> {
        let mut conn = self.conn.clone();
        let now = Timestamp::now().as_secs_f64();

        let ids: Vec<String> = conn
            .zrangebyscore_limit(self.disposable_queue_key(), 0.0, now, 0, limit as isize)
            .await
            .map_err(Self::convert_redis_error)?;

        Ok(ids.into_iter().map(TaskId::new).collect())
    }

    async fn get_stage_info(&self, limit: usize) -> Result<Vec<StageInfo>, BackendError> {
        let mut conn = self.conn.clone();

        let task_ids: Vec<String> = conn
            .zrange(self.stage_queue_key(), 0, limit as isize)
            .await
            .map_err(Self::convert_redis_error)?;

        if task_ids.is_empty() {
            return Ok(vec![]);
        }

        let ids: Vec<&str> = task_ids.iter().map(|s| s.as_str()).collect();
        let info_data: Vec<Option<String>> = conn
            .hget(self.stage_info_key(), ids)
            .await
            .map_err(Self::convert_redis_error)?;

        Ok(info_data
            .into_iter()
            .filter_map(|v| v.and_then(|s| StageInfo::from_json(&s)))
            .collect())
    }

    async fn restore(&self, info: StageInfo) -> Result<(), BackendError> {
        let tasks = self
            .lookup_tasks(std::slice::from_ref(&info.task_id))
            .await?;
        let Some(task) = tasks.first().and_then(|t| t.as_ref()) else {
            return Ok(());
        };

        let mut conn = self.conn.clone();
        let mut pipe = redis::pipe();
        pipe.atomic();

        pipe.zrem(self.stage_queue_key(), task.info.id.as_str());
        pipe.hdel(self.stage_info_key(), task.info.id.as_str());
        pipe.zadd(
            self.queue_key(task.info.group.as_str()),
            task.info.id.as_str(),
            task.info.due.as_secs_f64(),
        );

        pipe.query_async::<()>(&mut conn)
            .await
            .map_err(Self::convert_redis_error)?;

        Ok(())
    }

    async fn persist_scheduler_state_and_put_tasks(
        &self,
        name: &SchedulerName,
        data: Bytes,
        tasks: Vec<TaskRecord>,
    ) -> Result<(), BackendError> {
        self.enqueue_tasks(Some((name, data)), &tasks, OnKnownId::Keep)
            .await
    }

    async fn get_scheduler_state(
        &self,
        name: &SchedulerName,
    ) -> Result<Option<Vec<u8>>, BackendError> {
        let mut conn = self.conn.clone();

        let data: Option<Vec<u8>> = conn
            .get(self.scheduler_data_key(name))
            .await
            .map_err(Self::convert_redis_error)?;

        Ok(data)
    }
}

#[async_trait]
impl LockProvider for RedisBackend {
    async fn get_lock(&self, target: &str) -> Result<Box<dyn Lock>, BackendError> {
        Ok(Box::new(RedisLock::new(
            self.conn.clone(),
            self.lock_key_prefix(target),
        )))
    }
}

/// Lifetime of a lock key. The holder may still be working when it elapses, so
/// callers must not rely on the lock for correctness, only to reduce duplicate work.
const LOCK_TTL_SECS: u64 = 10;

/// Redis lock built on `SET NX EX` with a per-acquisition token.
///
/// The token is verified inside the release script so that a lock which already
/// expired and was re-acquired by another process is never deleted by this one.
struct RedisLock {
    conn: ConnectionManager,
    key: String,
    token: Mutex<Option<String>>,
}

impl RedisLock {
    fn new(conn: ConnectionManager, key: String) -> Self {
        Self {
            conn,
            key,
            token: Mutex::new(None),
        }
    }
}

#[async_trait]
impl Lock for RedisLock {
    async fn acquire(&self) -> bool {
        let mut token = self.token.lock().await;
        let new_token = Uuid::new_v4().to_string();

        // A single SET keeps key and expiry atomic; a separate EXPIRE would leave
        // the key without a TTL forever if this process died in between.
        let result: Result<Option<String>, RedisError> = redis::cmd("SET")
            .arg(&self.key)
            .arg(&new_token)
            .arg("NX")
            .arg("EX")
            .arg(LOCK_TTL_SECS)
            .query_async(&mut self.conn.clone())
            .await;

        match result {
            Ok(Some(_)) => {
                *token = Some(new_token);
                true
            }
            _ => false,
        }
    }

    async fn release(&self) {
        let Some(token) = self.token.lock().await.take() else {
            return;
        };

        let script = Script::new(
            r#"
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('DEL', KEYS[1])
end
return 0
"#,
        );

        let _: Result<i64, RedisError> = script
            .key(&self.key)
            .arg(token)
            .invoke_async(&mut self.conn.clone())
            .await;
    }
}

#[async_trait]
impl EventBridge for RedisBackend {
    async fn receive_events(
        &self,
    ) -> Result<Pin<Box<dyn Stream<Item = ControlEvent> + Send>>, ReceiveEventsError> {
        let mut pubsub = self
            .client
            .get_async_pubsub()
            .await
            .map_err(|e| ReceiveEventsError::ConnectionError(e.to_string()))?;

        pubsub
            .subscribe(EVENT_CHANNEL)
            .await
            .map_err(|e| ReceiveEventsError::ConnectionError(e.to_string()))?;

        let stream = stream::unfold(pubsub, |mut pubsub| async move {
            loop {
                let msg = pubsub.on_message().next().await?;
                let payload: Vec<u8> = msg.get_payload().ok()?;

                if let Ok(event) = serde_json::from_slice::<ControlEvent>(&payload) {
                    return Some((event, pubsub));
                }
            }
        })
        .boxed();

        Ok(stream)
    }

    async fn send_event(&self, event: &ControlEvent) -> Result<(), SendEventError> {
        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| SendEventError::ConnectionError(e.to_string()))?;

        let data = serde_json::to_vec(event)
            .map_err(|e| SendEventError::SerializationError(e.to_string()))?;

        conn.publish::<_, _, ()>(EVENT_CHANNEL, data)
            .await
            .map_err(|e| SendEventError::SendFailed(e.to_string()))?;

        Ok(())
    }
}

#[async_trait]
impl Backend for RedisBackend {
    async fn destroy_all(&self) -> Result<(), BackendError> {
        let mut conn = self.conn.clone();

        let pattern = format!("{KEY_PREFIX}.*");
        let keys: Vec<String> = conn
            .keys(pattern)
            .await
            .map_err(Self::convert_redis_error)?;

        if !keys.is_empty() {
            conn.del::<_, ()>(keys)
                .await
                .map_err(Self::convert_redis_error)?;
        }

        Ok(())
    }
}
