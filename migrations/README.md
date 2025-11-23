# Taskkit Database Migrations

This directory contains sqlx migrations for taskkit, organized by database type.

## Directory Structure

```
migrations/
├── mysql/          # MySQL/MariaDB migrations
│   ├── 20251214000001_initial_schema.up.sql
│   └── 20251214000001_initial_schema.down.sql
└── postgres/       # PostgreSQL migrations
```

## Prerequisites

Install sqlx-cli if not already installed:

```bash
# For MySQL
cargo install sqlx-cli --no-default-features --features mysql

# For PostgreSQL
cargo install sqlx-cli --no-default-features --features postgres

# For both
cargo install sqlx-cli --no-default-features --features mysql,postgres
```

## Setup MySQL for Testing

1. Start MySQL server:
```bash
# Using Docker
docker run -d --name taskkit-mysql \
  -e MYSQL_ROOT_PASSWORD=password \
  -e MYSQL_DATABASE=taskkit_test \
  -p 3306:3306 \
  mysql:8

# Or use your local MySQL installation
```

2. Create test database (if not using Docker):
```bash
mysql -u root -p -e "CREATE DATABASE IF NOT EXISTS taskkit_test;"
```

3. Set environment variable:
```bash
export MYSQL_URL="mysql://root:password@127.0.0.1:3306/taskkit_test"
```

## Setup PostgreSQL for Testing

```bash
# Using Docker
docker run -d --name taskkit-postgres \
  -e POSTGRES_PASSWORD=password \
  -e POSTGRES_DB=taskkit_test \
  -p 5432:5432 \
  postgres:16

export POSTGRES_URL="postgres://postgres:password@127.0.0.1:5432/taskkit_test"
```

## Running Migrations

### MySQL

Run migrations manually:
```bash
sqlx migrate run --source migrations/mysql --database-url "$MYSQL_URL"
```

Revert last migration:
```bash
sqlx migrate revert --source migrations/mysql --database-url "$MYSQL_URL"
```

### PostgreSQL

```bash
sqlx migrate run --source migrations/postgres --database-url "$POSTGRES_URL"
```

## Running Tests

Tests will automatically run migrations when the backend is created:

```bash
# Run all MySQL backend tests
cargo test --features mysql

# Run all PostgreSQL backend tests
cargo test --features postgres

# Run specific test
cargo test --features mysql mysql_test_workers

# Run with output
cargo test --features mysql -- --nocapture
```

## Database Schema

The migrations create the following tables:

- `taskkit_control_event` - Control events for worker management
- `taskkit_scheduler_state` - Scheduler state persistence
- `taskkit_worker` - Worker registration and heartbeats
- `taskkit_task` - Main task table with metadata and results

All tables use the `taskkit_` prefix to avoid conflicts with application tables.

## Adding New Migrations

```bash
# Create new MySQL migration
sqlx migrate add --source migrations/mysql -r <migration_name>

# Create new PostgreSQL migration
sqlx migrate add --source migrations/postgres -r <migration_name>
```

This will create both `.up.sql` and `.down.sql` files with timestamp prefixes.
