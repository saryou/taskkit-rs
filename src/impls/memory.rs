//! In-process backend intended for tests and local development.
//!
//! It implements the full [`Backend`] contract against plain in-memory maps, so
//! application tasks can be exercised end to end without a Redis or SQL server.
//! Nothing is persisted and nothing is shared between processes.

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::stream::{self, Stream, StreamExt};
use std::collections::{HashMap, HashSet};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::broadcast;

use crate::backend::{
    Backend, BackendError, ControlEvent, EventBridge, GetResultError, Lock, LockProvider,
    ReceiveEventsError, SendEventError, TaskBackend, WorkerTracker,
};
use crate::scheduler::SchedulerName;
use crate::stage::StageInfo;
use crate::task::{TaskId, TaskInfo, TaskRecord};
use crate::util::Timestamp;
use crate::worker::WorkerId;

const EVENT_CAPACITY: usize = 64;

#[derive(Clone)]
struct Entry {
    /// Insertion order, used to break ties between tasks sharing a `due` value so
    /// that queue and stage ordering stay stable across calls.
    seq: u64,
    info: TaskInfo,
    data: Bytes,
    began: Option<Timestamp>,
    assignee: Option<WorkerId>,
    done: Option<Timestamp>,
    disposable: Option<Timestamp>,
    result: Option<Bytes>,
    error: Option<String>,
}

impl Entry {
    fn new(seq: u64, record: TaskRecord) -> Self {
        Self {
            seq,
            info: record.info,
            data: record.data,
            began: None,
            assignee: None,
            done: None,
            disposable: None,
            result: None,
            error: None,
        }
    }

    fn record(&self) -> TaskRecord {
        TaskRecord::new(self.info.clone(), self.data.clone())
    }

    fn is_queued(&self) -> bool {
        self.began.is_none() && self.done.is_none()
    }
}

#[derive(Default)]
struct State {
    seq: u64,
    tasks: HashMap<TaskId, Entry>,
    /// Kept ordered by registration so `get_workers` is deterministic.
    workers: Vec<(WorkerId, Timestamp)>,
    scheduler_states: HashMap<SchedulerName, Vec<u8>>,
    held_locks: HashSet<String>,
}

impl State {
    fn next_seq(&mut self) -> u64 {
        self.seq += 1;
        self.seq
    }

    fn put(&mut self, record: TaskRecord) {
        if self.tasks.contains_key(&record.info.id) {
            return;
        }
        let seq = self.next_seq();
        self.tasks
            .insert(record.info.id.clone(), Entry::new(seq, record));
    }

    /// Entries matching `pred`, ordered by due time then insertion order.
    fn ordered(&self, pred: impl Fn(&Entry) -> bool) -> Vec<&Entry> {
        let mut entries: Vec<&Entry> = self.tasks.values().filter(|e| pred(e)).collect();
        entries.sort_by(|a, b| {
            a.info
                .due
                .as_secs_f64()
                .total_cmp(&b.info.due.as_secs_f64())
                .then(a.seq.cmp(&b.seq))
        });
        entries
    }

    fn finish(&mut self, record: TaskRecord, result: Option<Bytes>, error: Option<String>) {
        let id = record.info.id.clone();
        self.put(record);
        let now = Timestamp::now();
        if let Some(entry) = self.tasks.get_mut(&id) {
            entry.began = None;
            entry.assignee = None;
            entry.done = Some(now);
            entry.disposable = Some(now + entry.info.ttl.as_duration());
            entry.result = result;
            entry.error = error;
        }
    }
}

pub struct MemoryBackend {
    state: Arc<Mutex<State>>,
    events: broadcast::Sender<ControlEvent>,
    assign_calls: AtomicU64,
}

impl Default for MemoryBackend {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryBackend {
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(State::default())),
            events: broadcast::channel(EVENT_CAPACITY).0,
            assign_calls: AtomicU64::new(0),
        }
    }

    /// Number of `assign_tasks` calls this backend has served.
    ///
    /// Exposed so tests can assert on polling load, which should follow the polling
    /// interval rather than how many tasks a group runs concurrently.
    pub fn assign_call_count(&self) -> u64 {
        self.assign_calls.load(Ordering::Relaxed)
    }

    fn state(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().expect("memory backend state poisoned")
    }
}

#[async_trait]
impl TaskBackend for MemoryBackend {
    async fn put_tasks(&self, tasks: Vec<TaskRecord>) -> Result<(), BackendError> {
        let mut state = self.state();
        for task in tasks {
            state.put(task);
        }
        Ok(())
    }

    async fn get_queued_tasks(
        &self,
        group: &str,
        limit: usize,
    ) -> Result<Vec<TaskRecord>, BackendError> {
        let state = self.state();
        Ok(state
            .ordered(|e| e.is_queued() && e.info.group.as_str() == group)
            .into_iter()
            .take(limit)
            .map(Entry::record)
            .collect())
    }

    async fn assign_tasks(
        &self,
        group: &str,
        worker_id: &str,
        limit: usize,
    ) -> Result<Vec<TaskRecord>, BackendError> {
        self.assign_calls.fetch_add(1, Ordering::Relaxed);

        let mut state = self.state();
        let now = Timestamp::now();
        let due: Vec<TaskId> = state
            .ordered(|e| {
                e.is_queued()
                    && e.info.group.as_str() == group
                    && e.info.due.as_secs_f64() <= now.as_secs_f64()
            })
            .into_iter()
            .take(limit)
            .map(|e| e.info.id.clone())
            .collect();

        Ok(due
            .into_iter()
            .map(|id| {
                let entry = state.tasks.get_mut(&id).expect("just located");
                entry.began = Some(now);
                entry.assignee = Some(WorkerId::new(worker_id));
                entry.record()
            })
            .collect())
    }

    async fn lookup_tasks(
        &self,
        task_ids: &[TaskId],
    ) -> Result<Vec<Option<TaskRecord>>, BackendError> {
        let state = self.state();
        Ok(task_ids
            .iter()
            .map(|id| state.tasks.get(id).map(Entry::record))
            .collect())
    }

    async fn retry_task(&self, record: TaskRecord) -> Result<(), BackendError> {
        let mut state = self.state();
        let id = record.info.id.clone();
        let seq = match state.tasks.remove(&id) {
            Some(previous) => previous.seq,
            None => state.next_seq(),
        };
        state.tasks.insert(id, Entry::new(seq, record));
        Ok(())
    }

    async fn discard_tasks(&self, task_ids: &[TaskId]) -> Result<(), BackendError> {
        let mut state = self.state();
        for id in task_ids {
            state.tasks.remove(id);
        }
        Ok(())
    }

    async fn succeed(&self, record: TaskRecord, result: Bytes) -> Result<(), BackendError> {
        self.state().finish(record, Some(result), None);
        Ok(())
    }

    async fn fail(&self, record: TaskRecord, error: Bytes) -> Result<(), BackendError> {
        let message = String::from_utf8_lossy(&error).into_owned();
        self.state().finish(record, None, Some(message));
        Ok(())
    }

    async fn get_result(&self, task_id: &TaskId) -> Result<(TaskRecord, Bytes), GetResultError> {
        let state = self.state();
        let Some(entry) = state.tasks.get(task_id) else {
            return Err(GetResultError::NotFound);
        };

        if let Some(message) = &entry.error {
            return Err(GetResultError::Failed {
                record: entry.record(),
                message: message.clone(),
            });
        }
        match &entry.result {
            Some(result) => Ok((entry.record(), result.clone())),
            None => Err(GetResultError::NoResult(entry.record())),
        }
    }

    async fn get_done_task_ids(
        &self,
        since: Option<Timestamp>,
        until: Option<Timestamp>,
        limit: usize,
    ) -> Result<Vec<TaskId>, BackendError> {
        let until = until.unwrap_or_else(Timestamp::now).as_secs_f64();
        let since = since.map(|s| s.as_secs_f64());

        let state = self.state();
        let mut done: Vec<&Entry> = state
            .tasks
            .values()
            .filter(|e| match e.done {
                Some(done) => {
                    done.as_secs_f64() <= until && since.is_none_or(|s| done.as_secs_f64() >= s)
                }
                None => false,
            })
            .collect();
        done.sort_by(|a, b| {
            a.done
                .expect("filtered")
                .as_secs_f64()
                .total_cmp(&b.done.expect("filtered").as_secs_f64())
                .then(a.seq.cmp(&b.seq))
        });
        Ok(done
            .into_iter()
            .take(limit)
            .map(|e| e.info.id.clone())
            .collect())
    }

    async fn get_disposable_task_ids(&self, limit: usize) -> Result<Vec<TaskId>, BackendError> {
        let now = Timestamp::now().as_secs_f64();
        let state = self.state();
        let mut disposable: Vec<&Entry> = state
            .tasks
            .values()
            .filter(|e| e.disposable.is_some_and(|d| d.as_secs_f64() < now))
            .collect();
        disposable.sort_by(|a, b| {
            a.disposable
                .expect("filtered")
                .as_secs_f64()
                .total_cmp(&b.disposable.expect("filtered").as_secs_f64())
                .then(a.seq.cmp(&b.seq))
        });
        Ok(disposable
            .into_iter()
            .take(limit)
            .map(|e| e.info.id.clone())
            .collect())
    }

    async fn get_stage_info(&self, limit: usize) -> Result<Vec<StageInfo>, BackendError> {
        let state = self.state();
        Ok(state
            .ordered(|e| e.began.is_some())
            .into_iter()
            .take(limit)
            .map(|e| StageInfo {
                worker_id: e.assignee.clone().unwrap_or_else(|| WorkerId::new("")),
                task_id: e.info.id.clone(),
                assigned_at: e.began.expect("filtered"),
            })
            .collect())
    }

    async fn restore(&self, info: StageInfo) -> Result<(), BackendError> {
        let mut state = self.state();
        if let Some(entry) = state.tasks.get_mut(&info.task_id) {
            entry.began = None;
            entry.assignee = None;
        }
        Ok(())
    }

    async fn persist_scheduler_state_and_put_tasks(
        &self,
        name: &SchedulerName,
        data: Bytes,
        tasks: Vec<TaskRecord>,
    ) -> Result<(), BackendError> {
        let mut state = self.state();
        state.scheduler_states.insert(name.clone(), data.to_vec());
        for task in tasks {
            state.put(task);
        }
        Ok(())
    }

    async fn get_scheduler_state(
        &self,
        name: &SchedulerName,
    ) -> Result<Option<Vec<u8>>, BackendError> {
        Ok(self.state().scheduler_states.get(name).cloned())
    }
}

#[async_trait]
impl WorkerTracker for MemoryBackend {
    async fn set_worker_ttl(
        &self,
        worker_ids: HashSet<WorkerId>,
        expires_at: Timestamp,
    ) -> Result<(), BackendError> {
        let mut state = self.state();
        let mut added: Vec<WorkerId> = worker_ids
            .into_iter()
            .filter(
                |id| match state.workers.iter_mut().find(|(known, _)| known == id) {
                    Some((_, expires)) => {
                        *expires = expires_at;
                        false
                    }
                    None => true,
                },
            )
            .collect();
        added.sort_by(|a, b| a.as_str().cmp(b.as_str()));
        state
            .workers
            .extend(added.into_iter().map(|id| (id, expires_at)));
        Ok(())
    }

    async fn get_workers(&self) -> Result<Vec<(WorkerId, Timestamp)>, BackendError> {
        Ok(self.state().workers.clone())
    }

    async fn purge_workers(&self, worker_ids: HashSet<WorkerId>) -> Result<(), BackendError> {
        self.state()
            .workers
            .retain(|(id, _)| !worker_ids.contains(id));
        Ok(())
    }
}

#[async_trait]
impl LockProvider for MemoryBackend {
    async fn get_lock(&self, target: &str) -> Result<Box<dyn Lock>, BackendError> {
        Ok(Box::new(MemoryLock {
            state: self.state.clone(),
            target: target.to_string(),
            held: Mutex::new(false),
        }))
    }
}

struct MemoryLock {
    state: Arc<Mutex<State>>,
    target: String,
    held: Mutex<bool>,
}

#[async_trait]
impl Lock for MemoryLock {
    async fn acquire(&self) -> bool {
        let mut held = self.held.lock().expect("memory lock poisoned");
        if *held {
            return true;
        }
        *held = self
            .state
            .lock()
            .expect("memory backend state poisoned")
            .held_locks
            .insert(self.target.clone());
        *held
    }

    async fn release(&self) {
        let mut held = self.held.lock().expect("memory lock poisoned");
        if *held {
            self.state
                .lock()
                .expect("memory backend state poisoned")
                .held_locks
                .remove(&self.target);
            *held = false;
        }
    }
}

impl Drop for MemoryLock {
    fn drop(&mut self) {
        if *self.held.get_mut().expect("memory lock poisoned") {
            self.state
                .lock()
                .expect("memory backend state poisoned")
                .held_locks
                .remove(&self.target);
        }
    }
}

#[async_trait]
impl EventBridge for MemoryBackend {
    async fn receive_events(
        &self,
    ) -> Result<Pin<Box<dyn Stream<Item = ControlEvent> + Send>>, ReceiveEventsError> {
        let receiver = self.events.subscribe();
        Ok(stream::unfold(receiver, |mut receiver| async move {
            loop {
                match receiver.recv().await {
                    Ok(event) => return Some((event, receiver)),
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => return None,
                }
            }
        })
        .boxed())
    }

    async fn send_event(&self, event: &ControlEvent) -> Result<(), SendEventError> {
        // An error only means nobody is subscribed right now, which matches the
        // fire-and-forget semantics of the pub/sub backed bridges.
        let _ = self.events.send(event.clone());
        Ok(())
    }
}

#[async_trait]
impl Backend for MemoryBackend {
    async fn destroy_all(&self) -> Result<(), BackendError> {
        *self.state() = State::default();
        Ok(())
    }
}
