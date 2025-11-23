-- Rollback initial schema

DROP TABLE IF EXISTS taskkit_task;
DROP TABLE IF EXISTS taskkit_worker;
DROP TABLE IF EXISTS taskkit_scheduler_state;
DROP TABLE IF EXISTS taskkit_control_event;
