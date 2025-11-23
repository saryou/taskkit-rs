use chrono::{DateTime, Local, TimeZone, Utc};
use serde::{Deserialize, Serialize};
use std::ops::{Add, Sub};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

macro_rules! string_newtype {
    ($(#[$attr:meta])* $name:ident) => {
        $(#[$attr])*
        #[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
        pub struct $name(String);

        impl $name {
            pub fn new(id: impl Into<String>) -> Self {
                Self(id.into())
            }
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl From<String> for $name {
            fn from(s: String) -> Self {
                Self(s)
            }
        }

        impl From<&str> for $name {
            fn from(s: &str) -> Self {
                Self(s.to_string())
            }
        }

        impl AsRef<str> for $name {
            fn as_ref(&self) -> &str {
                &self.0
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(&self.0)
            }
        }

        impl From<$name> for String {
            fn from(id: $name) -> Self {
                id.0
            }
        }
    };
}

pub(crate) use string_newtype;

/// Time-to-live for task results (how long to keep results after completion)
///
/// Serialized as f64 seconds for Python/JSON compatibility
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Ttl(#[serde(with = "duration_secs_serde")] pub(crate) Duration);

mod duration_secs_serde {
    use serde::{Deserialize, Deserializer, Serializer};
    use std::time::Duration;

    pub fn serialize<S>(duration: &Duration, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_f64(duration.as_secs_f64())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Duration, D::Error>
    where
        D: Deserializer<'de>,
    {
        let secs: f64 = f64::deserialize(deserializer)?;
        Ok(Duration::from_secs_f64(secs))
    }
}

impl Ttl {
    pub fn from_secs(secs: u64) -> Self {
        Self(Duration::from_secs(secs))
    }

    pub fn from_secs_f64(secs: f64) -> Self {
        Self(Duration::from_secs_f64(secs))
    }

    pub fn from_duration(duration: Duration) -> Self {
        Self(duration)
    }

    pub fn as_duration(&self) -> Duration {
        self.0
    }

    pub fn as_secs_f64(&self) -> f64 {
        self.0.as_secs_f64()
    }
}

impl From<Duration> for Ttl {
    fn from(d: Duration) -> Self {
        Ttl(d)
    }
}

impl From<Ttl> for Duration {
    fn from(ttl: Ttl) -> Self {
        ttl.0
    }
}

impl From<f64> for Ttl {
    fn from(secs: f64) -> Self {
        Ttl::from_secs_f64(secs)
    }
}

/// Unix timestamp in seconds since epoch (wrapped f64 for type safety)
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Timestamp(f64);

impl Timestamp {
    pub fn from_secs_f64(secs: f64) -> Self {
        Self(secs)
    }

    pub fn as_secs_f64(&self) -> f64 {
        self.0
    }

    pub fn now() -> Self {
        Self(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("Time went backwards")
                .as_secs_f64(),
        )
    }

    pub fn to_datetime_utc(&self) -> DateTime<Utc> {
        let secs = self.0 as i64;
        let nanos = ((self.0 - secs as f64) * 1_000_000_000.0) as u32;
        Utc.timestamp_opt(secs, nanos).unwrap()
    }

    pub fn to_datetime(&self, tzinfo: chrono::FixedOffset) -> DateTime<chrono::FixedOffset> {
        let secs = self.0 as i64;
        let nanos = ((self.0 - secs as f64) * 1_000_000_000.0) as u32;
        tzinfo.timestamp_opt(secs, nanos).unwrap()
    }

    pub fn from_datetime(dt: DateTime<Utc>) -> Self {
        Self(dt.timestamp() as f64)
    }

    pub fn max(self, other: Self) -> Self {
        if self.0 >= other.0 { self } else { other }
    }
}

impl From<DateTime<Utc>> for Timestamp {
    fn from(dt: DateTime<Utc>) -> Self {
        Timestamp::from_datetime(dt)
    }
}

impl From<Timestamp> for DateTime<Utc> {
    fn from(ts: Timestamp) -> Self {
        ts.to_datetime_utc()
    }
}

impl From<f64> for Timestamp {
    fn from(secs: f64) -> Self {
        Timestamp::from_secs_f64(secs)
    }
}

impl From<Timestamp> for f64 {
    fn from(ts: Timestamp) -> Self {
        ts.as_secs_f64()
    }
}

impl Add<Duration> for Timestamp {
    type Output = Timestamp;

    fn add(self, rhs: Duration) -> Self::Output {
        Timestamp::from_secs_f64(self.0 + rhs.as_secs_f64())
    }
}

impl Sub<Duration> for Timestamp {
    type Output = Timestamp;

    fn sub(self, rhs: Duration) -> Self::Output {
        Timestamp::from_secs_f64(self.0 - rhs.as_secs_f64())
    }
}

impl Sub<Timestamp> for Timestamp {
    type Output = Duration;

    fn sub(self, rhs: Timestamp) -> Self::Output {
        Duration::from_secs_f64(self.0 - rhs.0)
    }
}

impl Add<f64> for Timestamp {
    type Output = Timestamp;

    fn add(self, rhs: f64) -> Self::Output {
        Timestamp::from_secs_f64(self.0 + rhs)
    }
}

impl Add<Timestamp> for f64 {
    type Output = Timestamp;

    fn add(self, rhs: Timestamp) -> Self::Output {
        Timestamp::from_secs_f64(self + rhs.0)
    }
}

impl Sub<f64> for Timestamp {
    type Output = Timestamp;

    fn sub(self, rhs: f64) -> Self::Output {
        Timestamp::from_secs_f64(self.0 - rhs)
    }
}

pub fn local_tz() -> chrono::FixedOffset {
    *Local::now().offset()
}
