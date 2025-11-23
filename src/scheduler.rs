use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Datelike, FixedOffset, Timelike, Utc};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use crate::backend::{BackendError, LockProvider, TaskBackend};
use crate::service::Service;
use crate::task::{DEFAULT_TASK_TTL, TaskGroup, TaskId, TaskInfo, TaskName, TaskRecord};
use crate::util::{Timestamp, Ttl, local_tz, string_newtype};

string_newtype! {
    /// Unique identifier for a scheduler instance
    SchedulerName
}

string_newtype! {
    /// Unique key for a schedule entry
    ScheduleEntryKey
}

/// Interval between schedule evaluation points in seconds
pub const SCHEDULE_POINT_INTERVAL: f64 = 5.0;

/// Marker type to represent "all valid values" for schedule fields
///
/// This can be used with schedule field constructors to indicate that
/// all valid values for that field should be included.
#[derive(Debug, Clone, Copy)]
pub struct All;

/// Error returned when a schedule field value is invalid
///
/// Schedule fields have specific ranges and intervals. For example,
/// minutes must be 0-59, hours must be 0-23, etc.
#[derive(Debug, thiserror::Error)]
pub struct ScheduleFieldError {
    field: &'static str,
    value: u32,
    min: u32,
    max: u32,
    interval: u32,
}

impl std::fmt::Display for ScheduleFieldError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.interval == 1 {
            write!(
                f,
                "Invalid value for {}: {} is not in range {}-{}",
                self.field, self.value, self.min, self.max
            )
        } else {
            write!(
                f,
                "Invalid value for {}: {} (expected values: {}, {}, ..., {})",
                self.field,
                self.value,
                self.min,
                self.min + self.interval,
                self.max
            )
        }
    }
}

macro_rules! schedule_field {
    // With interval and custom default
    ($name:ident, $min:expr, $max:expr, $interval:expr, default = $default:expr) => {
        #[derive(Debug, Clone)]
        pub struct $name(HashSet<u32>);

        impl $name {
            pub fn all() -> Self {
                Self(($min..=$max).step_by($interval).collect())
            }

            pub fn new(values: impl IntoIterator<Item = u32>) -> Result<Self, ScheduleFieldError> {
                let values: HashSet<u32> = values.into_iter().collect();
                if let Some(&invalid) = values
                    .iter()
                    .find(|&&v| !($min..=$max).contains(&v) || (v - $min) % ($interval as u32) != 0)
                {
                    return Err(ScheduleFieldError {
                        field: stringify!($name),
                        value: invalid,
                        min: $min,
                        max: $max,
                        interval: $interval as u32,
                    });
                }
                Ok(Self(values))
            }

            pub fn single(value: u32) -> Result<Self, ScheduleFieldError> {
                Self::new([value])
            }

            pub fn contains(&self, value: u32) -> bool {
                self.0.contains(&value)
            }
        }

        impl Default for $name {
            fn default() -> Self {
                $default
            }
        }

        // TryFrom implementations for ergonomic API
        impl TryFrom<All> for $name {
            type Error = ScheduleFieldError;

            fn try_from(_: All) -> Result<Self, Self::Error> {
                Ok(Self::all())
            }
        }

        impl TryFrom<u32> for $name {
            type Error = ScheduleFieldError;

            fn try_from(value: u32) -> Result<Self, Self::Error> {
                Self::single(value)
            }
        }

        impl TryFrom<Vec<u32>> for $name {
            type Error = ScheduleFieldError;

            fn try_from(values: Vec<u32>) -> Result<Self, Self::Error> {
                Self::new(values)
            }
        }

        impl TryFrom<HashSet<u32>> for $name {
            type Error = ScheduleFieldError;

            fn try_from(values: HashSet<u32>) -> Result<Self, Self::Error> {
                Self::new(values)
            }
        }

        impl<const N: usize> TryFrom<[u32; N]> for $name {
            type Error = ScheduleFieldError;

            fn try_from(values: [u32; N]) -> Result<Self, Self::Error> {
                Self::new(values)
            }
        }
    };
    // With interval (default to all)
    ($name:ident, $min:expr, $max:expr, $interval:expr) => {
        schedule_field!($name, $min, $max, $interval, default = Self::all());
    };
    // Without interval (defaults to 1)
    ($name:ident, $min:expr, $max:expr) => {
        schedule_field!($name, $min, $max, 1);
    };
}

schedule_field!(
    Seconds,
    0,
    59,
    SCHEDULE_POINT_INTERVAL as usize,
    default = Seconds::single(0).unwrap()
);
schedule_field!(Minutes, 0, 59);
schedule_field!(Hours, 0, 23);
schedule_field!(Days, 1, 31);
schedule_field!(Weekdays, 0, 6);
schedule_field!(Months, 1, 12);

/// Trait for defining task schedules
///
/// Implementors of this trait can define custom scheduling logic for tasks.
/// The scheduler calls this trait's methods to determine when tasks should
/// be executed.
pub trait Schedule: Send + Sync {
    /// Timezone whose wall clock this schedule is expressed in.
    ///
    /// `None` defers to the scheduler-wide timezone.
    fn get_timezone(&self) -> Option<FixedOffset>;

    /// Determine which schedule points should trigger task execution
    ///
    /// # Arguments
    ///
    /// * `schedule_points` - Candidate execution times to evaluate
    /// * `last_scheduled_at` - The last time a task was scheduled (if any)
    ///
    /// # Returns
    ///
    /// Returns a vector of timestamps when tasks should be executed. Both the
    /// candidates and the returned points carry the schedule's own timezone, so
    /// implementations read wall-clock fields directly off them.
    fn call(
        &self,
        schedule_points: &[DateTime<FixedOffset>],
        last_scheduled_at: Option<DateTime<FixedOffset>>,
    ) -> Vec<DateTime<FixedOffset>>;
}

/// Policy for handling multiple schedule points
///
/// When multiple schedule points are due (e.g., after downtime), this policy
/// determines which ones should actually trigger task execution.
pub trait DuplicationPolicy: Send + Sync {
    /// Filter schedule points according to the policy
    ///
    /// # Arguments
    ///
    /// * `schedule_points` - All schedule points that are due
    /// * `last_scheduled_at` - The last time a task was scheduled (if any)
    ///
    /// # Returns
    ///
    /// Returns the filtered schedule points to execute.
    fn call(
        &self,
        schedule_points: &[DateTime<FixedOffset>],
        last_scheduled_at: Option<DateTime<FixedOffset>>,
    ) -> Vec<DateTime<FixedOffset>>;
}

/// Duplication policy that keeps only the earliest schedule point
///
/// When multiple schedule points are due, only the earliest one is executed.
pub struct OnlyEarliest;

impl DuplicationPolicy for OnlyEarliest {
    fn call(
        &self,
        schedule_points: &[DateTime<FixedOffset>],
        _last_scheduled_at: Option<DateTime<FixedOffset>>,
    ) -> Vec<DateTime<FixedOffset>> {
        schedule_points.first().copied().into_iter().collect()
    }
}

/// Duplication policy that keeps only the latest schedule point
///
/// When multiple schedule points are due, only the latest one is executed.
/// This is useful to skip missed executions and only run the most recent.
pub struct OnlyLatest;

impl DuplicationPolicy for OnlyLatest {
    fn call(
        &self,
        schedule_points: &[DateTime<FixedOffset>],
        _last_scheduled_at: Option<DateTime<FixedOffset>>,
    ) -> Vec<DateTime<FixedOffset>> {
        schedule_points.last().copied().into_iter().collect()
    }
}

/// Cron-like regular schedule for task execution
///
/// Defines when a task should run based on time fields (seconds, minutes,
/// hours, days, weekdays, months). Similar to cron syntax but uses Rust types.
///
/// # Examples
///
/// ```ignore
/// // Run every minute
/// let schedule = RegularSchedule::new(
///     0,              // seconds: 0
///     All,            // minutes: all
///     All,            // hours: all
///     All,            // days: all
///     All,            // weekdays: all
///     All,            // months: all
///     None,           // timezone: inherit from the scheduler
///     Arc::new(OnlyLatest),
/// )?;
/// ```
pub struct RegularSchedule {
    seconds: Seconds,
    minutes: Minutes,
    hours: Hours,
    days: Days,
    weekdays: Weekdays,
    months: Months,
    tzinfo: Option<chrono::FixedOffset>,
    duplication_policy: Arc<dyn DuplicationPolicy>,
}

impl RegularSchedule {
    /// Create a new regular schedule
    ///
    /// # Arguments
    ///
    /// * `seconds` - Seconds when the task should run (0-59, multiples of SCHEDULE_POINT_INTERVAL)
    /// * `minutes` - Minutes when the task should run (0-59)
    /// * `hours` - Hours when the task should run (0-23)
    /// * `days` - Days of month when the task should run (1-31)
    /// * `weekdays` - Days of week when the task should run (0=Monday through 6=Sunday)
    /// * `months` - Months when the task should run (1-12)
    /// * `tzinfo` - Wall clock the time fields are read in (`None` inherits the
    ///   scheduler-wide timezone, which itself falls back to the local one)
    /// * `duplication_policy` - Policy for handling multiple due schedule points
    ///
    /// # Returns
    ///
    /// Returns `Err` if any field value is out of range or doesn't match the field's interval.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        seconds: impl TryInto<Seconds, Error = ScheduleFieldError>,
        minutes: impl TryInto<Minutes, Error = ScheduleFieldError>,
        hours: impl TryInto<Hours, Error = ScheduleFieldError>,
        days: impl TryInto<Days, Error = ScheduleFieldError>,
        weekdays: impl TryInto<Weekdays, Error = ScheduleFieldError>,
        months: impl TryInto<Months, Error = ScheduleFieldError>,
        tzinfo: Option<chrono::FixedOffset>,
        duplication_policy: Arc<dyn DuplicationPolicy>,
    ) -> Result<Self, ScheduleFieldError> {
        Ok(Self {
            seconds: seconds.try_into()?,
            minutes: minutes.try_into()?,
            hours: hours.try_into()?,
            days: days.try_into()?,
            weekdays: weekdays.try_into()?,
            months: months.try_into()?,
            tzinfo,
            duplication_policy,
        })
    }

    fn filter(&self, schedule_point: &DateTime<FixedOffset>) -> bool {
        if !self.seconds.contains(schedule_point.second()) {
            return false;
        }
        if !self.minutes.contains(schedule_point.minute()) {
            return false;
        }
        if !self.hours.contains(schedule_point.hour()) {
            return false;
        }
        if !self.days.contains(schedule_point.day()) {
            return false;
        }
        if !self
            .weekdays
            .contains(schedule_point.weekday().num_days_from_monday())
        {
            return false;
        }
        if !self.months.contains(schedule_point.month()) {
            return false;
        }
        true
    }
}

impl Schedule for RegularSchedule {
    fn get_timezone(&self) -> Option<FixedOffset> {
        self.tzinfo
    }

    fn call(
        &self,
        schedule_points: &[DateTime<FixedOffset>],
        last_scheduled_at: Option<DateTime<FixedOffset>>,
    ) -> Vec<DateTime<FixedOffset>> {
        let filtered: Vec<_> = schedule_points
            .iter()
            .copied()
            .filter(|p| self.filter(p))
            .collect();

        self.duplication_policy.call(&filtered, last_scheduled_at)
    }
}

#[derive(Clone)]
/// Entry defining a scheduled task
///
/// A `ScheduleEntry` associates a schedule with a specific task to be executed.
/// The scheduler uses these entries to determine when to queue tasks.
pub struct ScheduleEntry {
    /// Unique key identifying this schedule entry
    pub key: ScheduleEntryKey,
    /// The schedule determining when tasks are queued
    pub schedule: Arc<dyn Schedule>,
    pub group: TaskGroup,
    pub name: TaskName,
    /// Serialized input data for the task
    pub data: Bytes,
    /// Optional TTL override for task results (defaults to task's TTL)
    pub result_ttl: Option<Ttl>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SchedulerState {
    last_run_at: Timestamp,
    last_scheduled_at: HashMap<ScheduleEntryKey, Timestamp>,
}

/// Snaps a timestamp down to the schedule point grid.
fn round(ts: Timestamp) -> Timestamp {
    Timestamp::from_secs_f64(
        (ts.as_secs_f64() / SCHEDULE_POINT_INTERVAL).floor() * SCHEDULE_POINT_INTERVAL,
    )
}

/// Every schedule point from the one after `state.last_run_at` up to `now`.
///
/// Returning the whole range rather than just the latest point is what lets a
/// scheduler that was down for a while see the points it missed; deciding which of
/// them still deserve a task is left to each entry's [`DuplicationPolicy`].
fn list_schedule_points(state: Option<&SchedulerState>, now: Timestamp) -> Vec<Timestamp> {
    let at = round(now);

    let Some(state) = state else {
        return vec![at];
    };

    if state.last_run_at >= at {
        return Vec::new();
    }

    let elapsed = at - state.last_run_at;
    let points = (elapsed.as_secs_f64() / SCHEDULE_POINT_INTERVAL) as usize;
    (1..=points)
        .map(|i| state.last_run_at + (i as f64 * SCHEDULE_POINT_INTERVAL))
        .collect()
}

pub(crate) struct Scheduler {
    name: SchedulerName,
    task_backend: Arc<dyn TaskBackend>,
    lock_provider: Arc<dyn LockProvider>,
    entries: Vec<ScheduleEntry>,
    tzinfo: Option<chrono::FixedOffset>,
}

impl Scheduler {
    pub fn new(
        name: SchedulerName,
        task_backend: Arc<dyn TaskBackend>,
        lock_provider: Arc<dyn LockProvider>,
        entries: Vec<ScheduleEntry>,
        tzinfo: Option<chrono::FixedOffset>,
    ) -> Self {
        let keys: HashSet<_> = entries.iter().map(|e| &e.key).collect();
        assert_eq!(
            keys.len(),
            entries.len(),
            "All entries must have unique keys"
        );

        Self {
            name,
            task_backend,
            lock_provider,
            entries,
            tzinfo,
        }
    }

    async fn get_state(&self) -> Result<Option<SchedulerState>, BackendError> {
        if let Some(data) = self.task_backend.get_scheduler_state(&self.name).await? {
            match serde_json::from_slice(&data) {
                Ok(state) => Ok(Some(state)),
                Err(e) => {
                    tracing::warn!(
                        "Failed to deserialize scheduler state for '{}': {}. Starting with fresh state.",
                        self.name,
                        e
                    );
                    Ok(None)
                }
            }
        } else {
            Ok(None)
        }
    }

    fn encode_state(&self, state: &SchedulerState) -> Vec<u8> {
        serde_json::to_vec(state).unwrap()
    }

    async fn schedule_entries(&mut self, now: Timestamp) -> Result<(), BackendError> {
        let state = self.get_state().await?;
        let schedule_points = list_schedule_points(state.as_ref(), now);

        if schedule_points.is_empty() {
            return Ok(());
        }

        let mut new_state = SchedulerState {
            last_run_at: *schedule_points.last().unwrap(),
            last_scheduled_at: HashMap::new(),
        };

        let mut tasks = Vec::new();

        for entry in &self.entries {
            let last = state
                .as_ref()
                .and_then(|s| s.last_scheduled_at.get(&entry.key).copied());
            if let Some(last_ts) = last {
                new_state
                    .last_scheduled_at
                    .insert(entry.key.clone(), last_ts);
            }

            let tz = entry
                .schedule
                .get_timezone()
                .or(self.tzinfo)
                .unwrap_or_else(local_tz);
            let points: Vec<DateTime<FixedOffset>> = schedule_points
                .iter()
                .map(|sp| sp.to_datetime(tz))
                .collect();
            let scheduled = entry
                .schedule
                .call(&points, last.map(|ts| ts.to_datetime(tz)));

            for sp in scheduled {
                let sp_utc = sp.with_timezone(&Utc);
                new_state
                    .last_scheduled_at
                    .insert(entry.key.clone(), sp_utc.into());

                let info = TaskInfo::init_with_id(
                    TaskId::for_schedule_point(
                        self.name.as_str(),
                        entry.key.as_str(),
                        sp_utc.into(),
                    ),
                    entry.group.as_str(),
                    entry.name.as_str(),
                    Some(sp_utc),
                    Some(sp_utc),
                    entry.result_ttl.unwrap_or(DEFAULT_TASK_TTL),
                );

                tracing::info!("schedule task at {} ({}: {})", sp_utc, info.id, info.name);
                tasks.push(TaskRecord::new(info, entry.data.clone()));
            }
        }

        self.task_backend
            .persist_scheduler_state_and_put_tasks(
                &self.name,
                Bytes::from(self.encode_state(&new_state)),
                tasks,
            )
            .await?;

        Ok(())
    }
}

#[async_trait]
impl Service for Scheduler {
    /// It schedules entries and returns time interval indicating when
    /// should this method be called next time.
    async fn call(&mut self) -> Duration {
        let start = round(Timestamp::now());

        if !self.entries.is_empty()
            && let Ok(lock) = self
                .lock_provider
                .get_lock(&format!("scheduler.{}", self.name))
                .await
            && lock.acquire().await
        {
            let _ = self.schedule_entries(Timestamp::now()).await;
            lock.release().await;
        }

        let now = Timestamp::now();
        (start + SCHEDULE_POINT_INTERVAL).max(now) - now
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn ts(secs: f64) -> Timestamp {
        Timestamp::from_secs_f64(secs)
    }

    const UTC: FixedOffset = match FixedOffset::east_opt(0) {
        Some(tz) => tz,
        None => unreachable!(),
    };

    fn tz(hours: i32) -> FixedOffset {
        FixedOffset::east_opt(hours * 3600).unwrap()
    }

    /// A schedule point expressed in `offset`'s wall clock.
    fn at(
        offset: FixedOffset,
        y: i32,
        mo: u32,
        d: u32,
        h: u32,
        mi: u32,
        s: u32,
    ) -> DateTime<FixedOffset> {
        offset.with_ymd_and_hms(y, mo, d, h, mi, s).unwrap()
    }

    fn utc(y: i32, mo: u32, d: u32, h: u32, mi: u32, s: u32) -> DateTime<FixedOffset> {
        at(UTC, y, mo, d, h, mi, s)
    }

    fn state(last_run_at: f64) -> SchedulerState {
        SchedulerState {
            last_run_at: ts(last_run_at),
            last_scheduled_at: HashMap::new(),
        }
    }

    #[test]
    fn schedule_fields_reject_values_outside_their_range() {
        assert!(Minutes::single(59).is_ok());
        assert!(Minutes::single(60).is_err());
        assert!(Hours::single(23).is_ok());
        assert!(Hours::single(24).is_err());
        assert!(Days::single(1).is_ok());
        assert!(Days::single(0).is_err());
        assert!(Days::single(32).is_err());
        assert!(Weekdays::single(6).is_ok());
        assert!(Weekdays::single(7).is_err());
        assert!(Months::single(12).is_ok());
        assert!(Months::single(0).is_err());
        assert!(Minutes::new([0, 30, 60]).is_err());
    }

    #[test]
    fn seconds_are_limited_to_the_schedule_point_grid() {
        assert!(Seconds::single(0).is_ok());
        assert!(Seconds::single(55).is_ok());
        assert!(Seconds::single(3).is_err());
        assert_eq!(Seconds::all().0.len(), 12);
        assert!(Seconds::all().contains(45));
        assert!(!Seconds::all().contains(46));
    }

    #[test]
    fn seconds_default_to_the_top_of_the_minute() {
        assert!(Seconds::default().contains(0));
        assert!(!Seconds::default().contains(5));
        // Every other field defaults to matching anything.
        assert!(Minutes::default().contains(37));
        assert!(Weekdays::default().contains(6));
    }

    #[test]
    fn rounding_snaps_down_to_the_schedule_point_grid() {
        assert_eq!(round(ts(100.0)), ts(100.0));
        assert_eq!(round(ts(104.9)), ts(100.0));
        assert_eq!(round(ts(105.0)), ts(105.0));
    }

    #[test]
    fn a_scheduler_without_state_considers_only_the_current_point() {
        assert_eq!(list_schedule_points(None, ts(107.0)), vec![ts(105.0)]);
    }

    #[test]
    fn points_already_covered_by_the_previous_run_are_not_repeated() {
        assert!(list_schedule_points(Some(&state(105.0)), ts(105.0)).is_empty());
        assert!(list_schedule_points(Some(&state(105.0)), ts(109.9)).is_empty());
        // A state from the future (clock skew) must not produce points either.
        assert!(list_schedule_points(Some(&state(200.0)), ts(105.0)).is_empty());
    }

    #[test]
    fn missed_points_are_all_reported_after_downtime() {
        let points = list_schedule_points(Some(&state(100.0)), ts(122.0));
        assert_eq!(
            points,
            vec![ts(105.0), ts(110.0), ts(115.0), ts(120.0)],
            "every point between the last run and now should be offered"
        );
    }

    #[test]
    fn only_earliest_and_only_latest_pick_one_point_each() {
        let points = vec![utc(2026, 1, 1, 0, 0, 0), utc(2026, 1, 1, 0, 0, 5)];

        assert_eq!(OnlyEarliest.call(&points, None), vec![points[0]]);
        assert_eq!(OnlyLatest.call(&points, None), vec![points[1]]);
        assert!(OnlyEarliest.call(&[], None).is_empty());
        assert!(OnlyLatest.call(&[], None).is_empty());
    }

    fn hourly_at(minute: u32) -> RegularSchedule {
        RegularSchedule::new(0, minute, All, All, All, All, None, Arc::new(OnlyLatest)).unwrap()
    }

    #[test]
    fn a_regular_schedule_matches_only_its_own_time_fields() {
        let points = vec![
            utc(2026, 1, 1, 9, 29, 55),
            utc(2026, 1, 1, 9, 30, 0),
            utc(2026, 1, 1, 9, 30, 5),
            utc(2026, 1, 1, 10, 30, 0),
        ];

        // 09:30:05 is filtered out by the seconds field, so the latest match is 10:30.
        assert_eq!(
            hourly_at(30).call(&points, None),
            vec![utc(2026, 1, 1, 10, 30, 0)]
        );
    }

    #[test]
    fn weekdays_are_counted_from_monday() {
        // 2026-01-01 is a Thursday, so weekday 3.
        let schedule =
            RegularSchedule::new(0, 0, 0, All, 3, All, None, Arc::new(OnlyLatest)).unwrap();

        assert_eq!(
            schedule.call(&[utc(2026, 1, 1, 0, 0, 0), utc(2026, 1, 2, 0, 0, 0)], None),
            vec![utc(2026, 1, 1, 0, 0, 0)]
        );
    }

    #[test]
    fn day_and_month_fields_narrow_the_match() {
        let schedule =
            RegularSchedule::new(0, 0, 0, 2, All, 3, None, Arc::new(OnlyLatest)).unwrap();

        let points = vec![
            utc(2026, 3, 1, 0, 0, 0),
            utc(2026, 3, 2, 0, 0, 0),
            utc(2026, 4, 2, 0, 0, 0),
        ];
        assert_eq!(schedule.call(&points, None), vec![utc(2026, 3, 2, 0, 0, 0)]);
    }

    #[test]
    fn a_schedule_with_no_matching_point_yields_nothing() {
        assert!(
            hourly_at(30)
                .call(&[utc(2026, 1, 1, 9, 0, 0)], None)
                .is_empty()
        );
    }

    #[test]
    fn time_fields_are_read_in_the_schedules_own_timezone() {
        let jst = tz(9);
        // 09:00 JST is 00:00 UTC; reading the fields in UTC would match the wrong instant.
        let midnight_utc = Timestamp::from_secs_f64(utc(2026, 1, 1, 0, 0, 0).timestamp() as f64);
        let nine_utc = Timestamp::from_secs_f64(utc(2026, 1, 1, 9, 0, 0).timestamp() as f64);

        let schedule =
            RegularSchedule::new(0, 0, 9, All, All, All, Some(jst), Arc::new(OnlyLatest)).unwrap();

        assert_eq!(schedule.get_timezone(), Some(jst));
        assert_eq!(
            schedule.call(&[midnight_utc.to_datetime(jst)], None),
            vec![at(jst, 2026, 1, 1, 9, 0, 0)]
        );
        assert!(schedule.call(&[nine_utc.to_datetime(jst)], None).is_empty());

        // The same instant expressed in UTC has hour 0, so handing points over in
        // UTC rather than the schedule's own zone would fire at the wrong time.
        assert!(schedule.call(&[utc(2026, 1, 1, 0, 0, 0)], None).is_empty());
        assert_eq!(
            schedule.call(&[utc(2026, 1, 1, 9, 0, 0)], None),
            vec![utc(2026, 1, 1, 9, 0, 0)],
            "a UTC-offset point matches on its UTC fields, which is the wrong instant here"
        );
    }
}

#[cfg(all(test, feature = "memory"))]
mod scheduler_tests {
    use super::*;
    use crate::impls::MemoryBackend;
    use bytes::Bytes;

    const GROUP: &str = "sched";

    struct Fixture {
        backend: Arc<MemoryBackend>,
        scheduler: Scheduler,
    }

    impl Fixture {
        fn new(entries: Vec<ScheduleEntry>, tzinfo: Option<FixedOffset>) -> Self {
            let backend = Arc::new(MemoryBackend::new());
            let scheduler = Scheduler::new(
                SchedulerName::from("s"),
                backend.clone(),
                backend.clone(),
                entries,
                tzinfo,
            );
            Self { backend, scheduler }
        }

        async fn run_at(&mut self, now: Timestamp) {
            self.scheduler.schedule_entries(now).await.unwrap();
        }

        async fn queued(&self) -> Vec<TaskRecord> {
            self.backend.get_queued_tasks(GROUP, 100).await.unwrap()
        }

        async fn state(&self) -> SchedulerState {
            let raw = self
                .backend
                .get_scheduler_state(&SchedulerName::from("s"))
                .await
                .unwrap()
                .expect("state should have been persisted");
            serde_json::from_slice(&raw).unwrap()
        }

        async fn seed_state(&self, state: &SchedulerState) {
            self.backend
                .persist_scheduler_state_and_put_tasks(
                    &SchedulerName::from("s"),
                    Bytes::from(serde_json::to_vec(state).unwrap()),
                    vec![],
                )
                .await
                .unwrap();
        }
    }

    /// Matches every schedule point, so tests observe the point selection alone.
    fn always(policy: Arc<dyn DuplicationPolicy>) -> Arc<dyn Schedule> {
        Arc::new(RegularSchedule::new(All, All, All, All, All, All, None, policy).unwrap())
    }

    fn entry(key: &str, schedule: Arc<dyn Schedule>) -> ScheduleEntry {
        ScheduleEntry {
            key: ScheduleEntryKey::from(key),
            schedule,
            group: TaskGroup::from(GROUP),
            name: TaskName::from("t"),
            data: Bytes::from("d"),
            result_ttl: None,
        }
    }

    #[tokio::test]
    async fn the_first_run_schedules_the_current_point_and_records_it() {
        let mut f = Fixture::new(vec![entry("e", always(Arc::new(OnlyLatest)))], None);
        f.run_at(Timestamp::from_secs_f64(1000.0)).await;

        let queued = f.queued().await;
        assert_eq!(queued.len(), 1);
        assert_eq!(queued[0].info.due, Timestamp::from_secs_f64(1000.0));
        assert_eq!(
            queued[0].info.scheduled,
            Some(Timestamp::from_secs_f64(1000.0)),
            "a scheduled task must record the point it came from"
        );

        let state = f.state().await;
        assert_eq!(state.last_run_at, Timestamp::from_secs_f64(1000.0));
        assert_eq!(
            state.last_scheduled_at[&ScheduleEntryKey::from("e")],
            Timestamp::from_secs_f64(1000.0)
        );
    }

    #[tokio::test]
    async fn running_again_inside_the_same_point_schedules_nothing() {
        let mut f = Fixture::new(vec![entry("e", always(Arc::new(OnlyLatest)))], None);
        f.run_at(Timestamp::from_secs_f64(1000.0)).await;
        f.run_at(Timestamp::from_secs_f64(1004.0)).await;

        assert_eq!(f.queued().await.len(), 1);
    }

    #[tokio::test]
    async fn only_latest_collapses_a_backlog_into_one_task() {
        let mut f = Fixture::new(vec![entry("e", always(Arc::new(OnlyLatest)))], None);
        f.seed_state(&SchedulerState {
            last_run_at: Timestamp::from_secs_f64(1000.0),
            last_scheduled_at: HashMap::new(),
        })
        .await;

        f.run_at(Timestamp::from_secs_f64(1060.0)).await;

        let queued = f.queued().await;
        assert_eq!(queued.len(), 1, "12 missed points must not become 12 tasks");
        assert_eq!(queued[0].info.due, Timestamp::from_secs_f64(1060.0));
    }

    #[tokio::test]
    async fn only_earliest_replays_the_oldest_missed_point() {
        let mut f = Fixture::new(vec![entry("e", always(Arc::new(OnlyEarliest)))], None);
        f.seed_state(&SchedulerState {
            last_run_at: Timestamp::from_secs_f64(1000.0),
            last_scheduled_at: HashMap::new(),
        })
        .await;

        f.run_at(Timestamp::from_secs_f64(1060.0)).await;

        let queued = f.queued().await;
        assert_eq!(queued.len(), 1);
        assert_eq!(queued[0].info.due, Timestamp::from_secs_f64(1005.0));
    }

    #[tokio::test]
    async fn each_entry_keeps_its_own_last_scheduled_point() {
        let never =
            Arc::new(RegularSchedule::new(0, 0, 0, 1, All, 1, None, Arc::new(OnlyLatest)).unwrap())
                as Arc<dyn Schedule>;
        let mut f = Fixture::new(
            vec![
                entry("hot", always(Arc::new(OnlyLatest))),
                entry("cold", never),
            ],
            None,
        );

        f.run_at(Timestamp::from_secs_f64(1000.0)).await;
        f.run_at(Timestamp::from_secs_f64(2000.0)).await;

        let state = f.state().await;
        assert_eq!(
            state.last_scheduled_at[&ScheduleEntryKey::from("hot")],
            Timestamp::from_secs_f64(2000.0)
        );
        assert!(
            !state
                .last_scheduled_at
                .contains_key(&ScheduleEntryKey::from("cold")),
            "an entry that never matched must not gain a last-scheduled point"
        );
    }

    #[tokio::test]
    async fn an_entry_timezone_overrides_the_scheduler_wide_one() {
        // 1970-01-01T00:00:00Z is 09:00 in +09:00 and 00:00 in UTC.
        let jst = FixedOffset::east_opt(9 * 3600).unwrap();
        let utc = FixedOffset::east_opt(0).unwrap();
        let at_nine = Arc::new(
            RegularSchedule::new(0, 0, 9, All, All, All, Some(jst), Arc::new(OnlyLatest)).unwrap(),
        ) as Arc<dyn Schedule>;

        let mut f = Fixture::new(vec![entry("e", at_nine)], Some(utc));
        f.run_at(Timestamp::from_secs_f64(0.0)).await;

        assert_eq!(
            f.queued().await.len(),
            1,
            "the entry's own timezone must win over the scheduler's"
        );
    }

    #[tokio::test]
    async fn the_scheduler_timezone_applies_when_an_entry_has_none() {
        let jst = FixedOffset::east_opt(9 * 3600).unwrap();
        let at_nine = Arc::new(
            RegularSchedule::new(0, 0, 9, All, All, All, None, Arc::new(OnlyLatest)).unwrap(),
        ) as Arc<dyn Schedule>;

        let mut f = Fixture::new(vec![entry("e", at_nine)], Some(jst));
        f.run_at(Timestamp::from_secs_f64(0.0)).await;

        assert_eq!(f.queued().await.len(), 1);
    }

    #[tokio::test]
    async fn re_scheduling_the_same_point_does_not_duplicate_the_task() {
        let mut f = Fixture::new(vec![entry("e", always(Arc::new(OnlyLatest)))], None);

        f.run_at(Timestamp::from_secs_f64(1000.0)).await;
        let first = f.queued().await;
        assert_eq!(first.len(), 1);

        // Rewinding the state is what a scheduler on another process sees when the
        // lock it relied on expired mid-run.
        f.seed_state(&SchedulerState {
            last_run_at: Timestamp::from_secs_f64(995.0),
            last_scheduled_at: HashMap::new(),
        })
        .await;
        f.run_at(Timestamp::from_secs_f64(1000.0)).await;

        let second = f.queued().await;
        assert_eq!(
            second.len(),
            1,
            "the same schedule point must yield one task"
        );
        assert_eq!(second[0].info.id, first[0].info.id);
    }

    #[tokio::test]
    async fn derived_ids_separate_entries_schedulers_and_points() {
        let at = Timestamp::from_secs_f64(1000.0);
        let id = TaskId::for_schedule_point("s", "e", at);

        assert_eq!(id, TaskId::for_schedule_point("s", "e", at));
        assert_ne!(id, TaskId::for_schedule_point("s", "other", at));
        assert_ne!(id, TaskId::for_schedule_point("other", "e", at));
        assert_ne!(
            id,
            TaskId::for_schedule_point("s", "e", Timestamp::from_secs_f64(1005.0))
        );
    }

    #[tokio::test]
    async fn the_entry_result_ttl_reaches_the_queued_task() {
        let mut e = entry("e", always(Arc::new(OnlyLatest)));
        e.result_ttl = Some(Ttl::from_secs(42));
        let mut f = Fixture::new(vec![e], None);

        f.run_at(Timestamp::from_secs_f64(1000.0)).await;
        assert_eq!(f.queued().await[0].info.ttl, Ttl::from_secs(42));
    }
}
