//! End-to-end tests covering the Kit -> Runtime -> Worker -> result path
//! against the in-memory backend.

#![cfg(feature = "memory")]

use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;
use taskkit::*;

const GROUP: &str = "test";
const TIMEOUT: Duration = Duration::from_secs(5);

/// Runs `body` with a live runtime, then shuts the runtime down and joins it.
async fn with_runtime<F, Fut>(registry: TaskRegistry, body: F)
where
    F: FnOnce(Arc<Kit>) -> Fut,
    Fut: Future<Output = ()>,
{
    with_runtime_config(
        registry,
        2,
        Duration::from_millis(20),
        HashMap::new(),
        |kit, _| body(kit),
    )
    .await
}

/// As [`with_runtime`], but with control over concurrency and polling, and with the
/// backend handed to the body so it can observe polling load.
async fn with_runtime_config<F, Fut>(
    registry: TaskRegistry,
    concurrency: usize,
    polling_interval: Duration,
    schedule_entries: HashMap<SchedulerName, Vec<ScheduleEntry>>,
    body: F,
) where
    F: FnOnce(Arc<Kit>, Arc<MemoryBackend>) -> Fut,
    Fut: Future<Output = ()>,
{
    let backend = Arc::new(MemoryBackend::new());
    let kit = Arc::new(Kit::new(
        backend.clone(),
        backend.clone(),
        backend.clone(),
        backend.clone(),
        Arc::new(registry),
    ));

    let runtime_kit = kit.clone();
    let handle = tokio::spawn(async move {
        runtime_kit
            .start(
                HashMap::from([(TaskGroup::from(GROUP), concurrency)]),
                schedule_entries,
                None,
                PollingInterval::Fixed(polling_interval),
                false,
            )
            .await
            .unwrap();
    });

    body(kit.clone(), backend.clone()).await;

    // The bridge only delivers to current subscribers, so keep signalling until
    // the runtime has actually torn down.
    tokio::time::timeout(TIMEOUT, async {
        while !handle.is_finished() {
            kit.send_shutdown_event(None).await.unwrap();
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("runtime did not shut down");
    handle.await.unwrap();
}

#[derive(Debug, Serialize, Deserialize)]
struct Value {
    value: i32,
}

struct Echo;

#[async_trait]
impl JsonTask for Echo {
    const GROUP: &'static str = GROUP;
    const NAME: &'static str = "echo";
    type Input = Value;
    type Output = Value;

    async fn handle(&self, _info: &TaskInfo, input: Value) -> Result<Value, TaskError> {
        Ok(Value {
            value: input.value * 2,
        })
    }
}

struct Flaky {
    attempts: Arc<AtomicU32>,
}

#[async_trait]
impl JsonTask for Flaky {
    const GROUP: &'static str = GROUP;
    const NAME: &'static str = "flaky";
    type Input = ();
    type Output = u32;

    async fn handle(&self, info: &TaskInfo, _input: ()) -> Result<u32, TaskError> {
        if self.attempts.fetch_add(1, Ordering::SeqCst) == 0 {
            return Err(TaskError::retry_reason(
                Duration::from_millis(20),
                "first attempt always fails",
            ));
        }
        Ok(info.retry_count)
    }
}

struct Doomed;

#[async_trait]
impl JsonTask for Doomed {
    const GROUP: &'static str = GROUP;
    const NAME: &'static str = "doomed";
    type Input = ();
    type Output = ();

    async fn handle(&self, _info: &TaskInfo, _input: ()) -> Result<(), TaskError> {
        Err(TaskError::fatal_reason("nope"))
    }
}

struct Dropped;

#[async_trait]
impl JsonTask for Dropped {
    const GROUP: &'static str = GROUP;
    const NAME: &'static str = "dropped";
    type Input = ();
    type Output = ();

    async fn handle(&self, _info: &TaskInfo, _input: ()) -> Result<(), TaskError> {
        Err(TaskError::discard_reason("not worth keeping"))
    }
}

struct Exploding;

#[async_trait]
impl JsonTask for Exploding {
    const GROUP: &'static str = GROUP;
    const NAME: &'static str = "exploding";
    type Input = ();
    type Output = ();

    async fn handle(&self, _info: &TaskInfo, _input: ()) -> Result<(), TaskError> {
        panic!("boom");
    }
}

fn registry_with<T: Task>(task: T) -> TaskRegistry {
    let mut registry = TaskRegistry::new();
    registry.register(task);
    registry
}

#[tokio::test]
async fn task_runs_and_returns_its_output() {
    with_runtime(registry_with(Echo), |kit| async move {
        let result = kit
            .initiate_task::<Echo>(Value { value: 21 }, None::<f64>, None::<f64>)
            .await
            .unwrap();

        assert_eq!(result.get(Some(TIMEOUT), false).await.unwrap().value, 42);
    })
    .await;
}

#[tokio::test]
async fn retry_reschedules_the_task() {
    let attempts = Arc::new(AtomicU32::new(0));
    let registry = registry_with(Flaky {
        attempts: attempts.clone(),
    });

    with_runtime(registry, |kit| async move {
        let result = kit
            .initiate_task::<Flaky>((), None::<f64>, None::<f64>)
            .await
            .unwrap();

        // The handler reports the retry_count it observed, so a value of 1 proves
        // the task came back through the queue rather than being retried in place.
        assert_eq!(result.get(Some(TIMEOUT), false).await.unwrap(), 1);
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
    })
    .await;
}

#[tokio::test]
async fn fatal_error_surfaces_as_failure() {
    with_runtime(registry_with(Doomed), |kit| async move {
        let result = kit
            .initiate_task::<Doomed>((), None::<f64>, None::<f64>)
            .await
            .unwrap();

        match result.get(Some(TIMEOUT), false).await {
            Err(ResultGetError::Failed { message }) => assert_eq!(message, "nope"),
            other => panic!("expected Failed, got {other:?}"),
        }
    })
    .await;
}

#[tokio::test]
async fn discarded_task_disappears() {
    with_runtime(registry_with(Dropped), |kit| async move {
        let result = kit
            .initiate_task::<Dropped>((), None::<f64>, None::<f64>)
            .await
            .unwrap();

        match result.get(Some(TIMEOUT), false).await {
            Err(ResultGetError::NotFound) => {}
            other => panic!("expected NotFound, got {other:?}"),
        }
    })
    .await;
}

#[tokio::test]
async fn panicking_handler_fails_the_task_instead_of_killing_the_worker() {
    with_runtime(registry_with(Exploding), |kit| async move {
        let result = kit
            .initiate_task::<Exploding>((), None::<f64>, None::<f64>)
            .await
            .unwrap();

        match result.get(Some(TIMEOUT), false).await {
            Err(ResultGetError::Failed { message }) => {
                assert!(
                    message.contains("panicked"),
                    "unexpected message: {message}"
                );
                assert!(message.contains("boom"), "unexpected message: {message}");
            }
            other => panic!("expected Failed, got {other:?}"),
        }
    })
    .await;
}

#[tokio::test]
async fn worker_survives_a_panicking_task_and_keeps_serving() {
    let mut registry = TaskRegistry::new();
    registry.register(Exploding);
    registry.register(Echo);

    with_runtime(registry, |kit| async move {
        let exploding = kit
            .initiate_task::<Exploding>((), None::<f64>, None::<f64>)
            .await
            .unwrap();
        assert!(exploding.get(Some(TIMEOUT), false).await.is_err());

        let echo = kit
            .initiate_task::<Echo>(Value { value: 3 }, None::<f64>, None::<f64>)
            .await
            .unwrap();
        assert_eq!(echo.get(Some(TIMEOUT), false).await.unwrap().value, 6);
    })
    .await;
}

#[tokio::test]
async fn unregistered_task_is_failed() {
    with_runtime(registry_with(Echo), |kit| async move {
        let info = TaskInfo::init(
            GROUP,
            "not_registered",
            None::<f64>,
            None::<f64>,
            DEFAULT_TASK_TTL,
        );
        let task_id = info.id.clone();
        kit.initiate_tasks(vec![TaskRecord::new(info, Bytes::from("{}"))])
            .await
            .unwrap();

        match kit
            .get_result::<Echo>(task_id)
            .get(Some(TIMEOUT), false)
            .await
        {
            Err(ResultGetError::Failed { message }) => {
                assert!(message.contains("No executor"), "unexpected: {message}");
            }
            other => panic!("expected Failed, got {other:?}"),
        }
    })
    .await;
}

#[tokio::test]
async fn pause_stops_processing_until_resume() {
    with_runtime(registry_with(Echo), |kit| async move {
        // The bridge only reaches subscribers that are already listening, and the
        // runtime subscribes after spawning its workers, so keep pausing until a
        // probe task is actually left unprocessed.
        let paused = tokio::time::timeout(TIMEOUT, async {
            loop {
                kit.send_pause_event(None).await.unwrap();

                let probe = kit
                    .initiate_task::<Echo>(Value { value: 1 }, None::<f64>, None::<f64>)
                    .await
                    .unwrap();

                if let Err(ResultGetError::TimedOut) =
                    probe.get(Some(Duration::from_millis(200)), false).await
                {
                    break probe;
                }
            }
        })
        .await
        .expect("workers never paused");

        kit.send_resume_event(None).await.unwrap();
        assert_eq!(paused.get(Some(TIMEOUT), false).await.unwrap().value, 2);
    })
    .await;
}

struct Slow {
    in_flight: Arc<AtomicU32>,
    peak: Arc<AtomicU32>,
}

#[async_trait]
impl JsonTask for Slow {
    const GROUP: &'static str = GROUP;
    const NAME: &'static str = "slow";
    type Input = ();
    type Output = ();

    async fn handle(&self, _info: &TaskInfo, _input: ()) -> Result<(), TaskError> {
        let running = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(running, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(100)).await;
        self.in_flight.fetch_sub(1, Ordering::SeqCst);
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_group_runs_up_to_its_concurrency_at_once() {
    const CONCURRENCY: usize = 4;
    const TASKS: usize = 8;

    let in_flight = Arc::new(AtomicU32::new(0));
    let peak = Arc::new(AtomicU32::new(0));
    let registry = registry_with(Slow {
        in_flight: in_flight.clone(),
        peak: peak.clone(),
    });

    with_runtime_config(
        registry,
        CONCURRENCY,
        Duration::from_millis(20),
        HashMap::new(),
        |kit, _| async move {
            let mut pending = Vec::new();
            for _ in 0..TASKS {
                pending.push(
                    kit.initiate_task::<Slow>((), None::<f64>, None::<f64>)
                        .await
                        .unwrap(),
                );
            }
            for task in pending {
                task.get(Some(TIMEOUT), false).await.unwrap();
            }

            assert_eq!(
                peak.load(Ordering::SeqCst) as usize,
                CONCURRENCY,
                "a full batch should be claimed and run in parallel, never exceeding the limit"
            );
        },
    )
    .await;
}

#[tokio::test]
async fn polling_load_does_not_grow_with_concurrency() {
    // With one fetch loop per group, an idle group polls at the polling interval no
    // matter how many tasks it is allowed to run at once.
    async fn idle_polls(concurrency: usize) -> u64 {
        let counted = Arc::new(std::sync::Mutex::new(0u64));
        let sink = counted.clone();

        with_runtime_config(
            registry_with(Echo),
            concurrency,
            Duration::from_millis(100),
            HashMap::new(),
            |_kit, backend| async move {
                tokio::time::sleep(Duration::from_millis(500)).await;
                *sink.lock().unwrap() = backend.assign_call_count();
            },
        )
        .await;

        *counted.lock().unwrap()
    }

    let few = idle_polls(1).await;
    let many = idle_polls(16).await;

    // ~5 polls are expected either way (500ms of idling at a 100ms interval).
    // Guard the lower bound too, so the comparison cannot pass on two zeroes.
    assert!(
        few >= 3,
        "expected the idle group to poll at all, got {few}"
    );
    assert!(
        many <= few * 2,
        "polling scaled with concurrency: {few} polls at concurrency 1, {many} at 16"
    );
}

struct Quick {
    completed: Arc<AtomicU32>,
}

#[async_trait]
impl JsonTask for Quick {
    const GROUP: &'static str = GROUP;
    const NAME: &'static str = "quick";
    type Input = ();
    type Output = ();

    async fn handle(&self, _info: &TaskInfo, _input: ()) -> Result<(), TaskError> {
        tokio::time::sleep(Duration::from_millis(1)).await;
        self.completed.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_freed_slot_is_refilled_without_waiting_for_the_polling_interval() {
    const CONCURRENCY: usize = 8;
    const TASKS: u32 = 80;

    let completed = Arc::new(AtomicU32::new(0));
    let registry = registry_with(Quick {
        completed: completed.clone(),
    });

    // Polling is kept short so it contributes only once, when the first batch is
    // picked up. Everything after that is slot refilling, which is what is measured.
    with_runtime_config(
        registry,
        CONCURRENCY,
        Duration::from_millis(20),
        HashMap::new(),
        |kit, _| async move {
            // Enqueued in one call: draining the queue mid-enqueue would trip the
            // "queue ran dry" backoff and measure the polling interval instead.
            let records = (0..TASKS)
                .map(|_| <Quick as Task>::prepare((), None::<f64>, None::<f64>).unwrap())
                .collect();

            let start = tokio::time::Instant::now();
            kit.initiate_tasks(records).await.unwrap();

            tokio::time::timeout(TIMEOUT, async {
                while completed.load(Ordering::SeqCst) < TASKS {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            })
            .await
            .expect("tasks never finished");

            // 10 dispatch rounds of 1ms tasks. Polling for free slots instead of
            // waiting on them would add a fixed delay to every one of those rounds.
            let elapsed = start.elapsed();
            assert!(
                elapsed < Duration::from_millis(400),
                "saturated dispatch was throttled: {TASKS} tasks took {elapsed:?}"
            );
        },
    )
    .await;
}

struct Scheduled {
    runs: Arc<AtomicU32>,
}

#[async_trait]
impl JsonTask for Scheduled {
    const GROUP: &'static str = GROUP;
    const NAME: &'static str = "scheduled";
    type Input = ();
    type Output = ();

    async fn handle(&self, info: &TaskInfo, _input: ()) -> Result<(), TaskError> {
        assert!(
            info.scheduled.is_some(),
            "a task created by the scheduler carries the point it was scheduled for"
        );
        self.runs.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[tokio::test]
async fn a_schedule_entry_produces_a_task_that_the_workers_run() {
    let runs = Arc::new(AtomicU32::new(0));
    let entry = ScheduleEntry {
        key: ScheduleEntryKey::from("every-point"),
        schedule: Arc::new(
            RegularSchedule::new(All, All, All, All, All, All, None, Arc::new(OnlyLatest)).unwrap(),
        ),
        group: TaskGroup::from(GROUP),
        name: TaskName::from(<Scheduled as JsonTask>::NAME),
        data: Bytes::from("null"),
        result_ttl: None,
    };

    with_runtime_config(
        registry_with(Scheduled { runs: runs.clone() }),
        2,
        Duration::from_millis(20),
        HashMap::from([(SchedulerName::from("s"), vec![entry])]),
        |_kit, _| async move {
            tokio::time::timeout(TIMEOUT, async {
                while runs.load(Ordering::SeqCst) == 0 {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("the scheduled task was never executed");
        },
    )
    .await;
}
