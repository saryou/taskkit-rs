// Common SQL abstraction layer
#[cfg(any(feature = "mysql", feature = "postgres"))]
pub mod sqlx;

#[cfg(feature = "memory")]
pub mod memory;

#[cfg(feature = "redis")]
pub mod redis;

#[cfg(feature = "mysql")]
pub mod mysql;

#[cfg(feature = "postgres")]
pub mod postgres;

#[cfg(feature = "memory")]
pub use memory::MemoryBackend;

#[cfg(feature = "redis")]
pub use redis::RedisBackend;

#[cfg(feature = "mysql")]
pub use mysql::MysqlBackend;

#[cfg(feature = "postgres")]
pub use postgres::PostgresBackend;
