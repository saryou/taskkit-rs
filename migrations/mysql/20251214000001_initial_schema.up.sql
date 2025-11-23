-- Initial schema for taskkit (MySQL)

CREATE TABLE IF NOT EXISTS taskkit_control_event (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    sent DOUBLE NOT NULL,
    data LONGBLOB NOT NULL,
    INDEX idx_sent (sent)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS taskkit_scheduler_state (
    id VARCHAR(255) PRIMARY KEY,
    data LONGBLOB NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS taskkit_worker (
    id VARCHAR(255) PRIMARY KEY,
    expires DOUBLE NOT NULL,
    INDEX idx_expires (expires)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS taskkit_task (
    id VARCHAR(255) PRIMARY KEY,
    `group` VARCHAR(40) NOT NULL,
    name VARCHAR(255) NOT NULL,
    data LONGBLOB NOT NULL,
    due DOUBLE NOT NULL,
    created DOUBLE NOT NULL,
    scheduled DOUBLE,
    retry_count INT UNSIGNED NOT NULL,
    ttl DOUBLE NOT NULL,
    assignee_worker_id VARCHAR(255),
    began DOUBLE,
    result LONGBLOB,
    error_message TEXT,
    done DOUBLE,
    disposable DOUBLE,
    INDEX idx_began_group_due (began, `group`, due),
    INDEX idx_done_began (done, began),
    INDEX idx_name_created (name, created),
    INDEX idx_disposable (disposable)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

