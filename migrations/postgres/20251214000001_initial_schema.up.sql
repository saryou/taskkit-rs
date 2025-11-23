-- Initial schema for taskkit (PostgreSQL)

CREATE TABLE IF NOT EXISTS taskkit_control_event (
    id BIGSERIAL PRIMARY KEY,
    sent DOUBLE PRECISION NOT NULL,
    data BYTEA NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_control_event_sent ON taskkit_control_event(sent);

CREATE TABLE IF NOT EXISTS taskkit_scheduler_state (
    id VARCHAR(255) PRIMARY KEY,
    data BYTEA NOT NULL
);

CREATE TABLE IF NOT EXISTS taskkit_worker (
    id VARCHAR(255) PRIMARY KEY,
    expires DOUBLE PRECISION NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_worker_expires ON taskkit_worker(expires);

CREATE TABLE IF NOT EXISTS taskkit_task (
    id VARCHAR(255) PRIMARY KEY,
    "group" VARCHAR(40) NOT NULL,
    name VARCHAR(255) NOT NULL,
    data BYTEA NOT NULL,
    due DOUBLE PRECISION NOT NULL,
    created DOUBLE PRECISION NOT NULL,
    scheduled DOUBLE PRECISION,
    retry_count INTEGER NOT NULL CHECK (retry_count >= 0),
    ttl DOUBLE PRECISION NOT NULL,
    assignee_worker_id VARCHAR(255),
    began DOUBLE PRECISION,
    result BYTEA,
    error_message TEXT,
    done DOUBLE PRECISION,
    disposable DOUBLE PRECISION
);

CREATE INDEX IF NOT EXISTS idx_task_began_group_due ON taskkit_task(began, "group", due);
CREATE INDEX IF NOT EXISTS idx_task_done_began ON taskkit_task(done, began);
CREATE INDEX IF NOT EXISTS idx_task_name_created ON taskkit_task(name, created);
CREATE INDEX IF NOT EXISTS idx_task_disposable ON taskkit_task(disposable);
