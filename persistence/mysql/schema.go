package mysql

// Schema 是 MySQL 默认表结构，覆盖实例、任务、历史、幂等和 Outbox。
const Schema = `
CREATE TABLE IF NOT EXISTS workflow_instance (
    id VARCHAR(128) NOT NULL PRIMARY KEY,
    workflow VARCHAR(128) NOT NULL,
    version VARCHAR(64) NOT NULL,
    state VARCHAR(128) NOT NULL,
    status VARCHAR(32) NOT NULL,
    revision BIGINT NOT NULL,
    data JSON NULL,
    started_at DATETIME(3) NOT NULL,
    updated_at DATETIME(3) NOT NULL,
    finished_at DATETIME(3) NULL,
    KEY idx_workflow_instance_workflow_state (workflow, state),
    KEY idx_workflow_instance_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS workflow_task (
    id VARCHAR(128) NOT NULL PRIMARY KEY,
    instance_id VARCHAR(128) NOT NULL,
    task VARCHAR(128) NOT NULL,
    handler VARCHAR(128) NOT NULL,
    status VARCHAR(32) NOT NULL,
    attempt INT NOT NULL,
    max_attempts INT NOT NULL,
    next_run_at DATETIME(3) NOT NULL,
    timeout_at DATETIME(3) NULL,
    last_error TEXT NULL,
    input JSON NULL,
    output JSON NULL,
    created_at DATETIME(3) NOT NULL,
    updated_at DATETIME(3) NOT NULL,
    KEY idx_workflow_task_due (status, next_run_at),
    KEY idx_workflow_task_instance (instance_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS workflow_history (
    id VARCHAR(128) NOT NULL PRIMARY KEY,
    instance_id VARCHAR(128) NOT NULL,
    type VARCHAR(64) NOT NULL,
    message VARCHAR(512) NOT NULL,
    state VARCHAR(128) NOT NULL,
    task_id VARCHAR(128) NULL,
    task VARCHAR(128) NULL,
    event VARCHAR(128) NULL,
    payload JSON NULL,
    created_at DATETIME(3) NOT NULL,
    KEY idx_workflow_history_instance (instance_id, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS workflow_idempotency (
    scope VARCHAR(128) NOT NULL,
    idem_key VARCHAR(256) NOT NULL,
    result_id VARCHAR(128) NOT NULL,
    created_at DATETIME(3) NOT NULL,
    PRIMARY KEY (scope, idem_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS workflow_outbox (
    id VARCHAR(128) NOT NULL PRIMARY KEY,
    topic VARCHAR(128) NOT NULL,
    msg_key VARCHAR(256) NOT NULL,
    payload JSON NOT NULL,
    status VARCHAR(32) NOT NULL,
    attempt INT NOT NULL,
    next_run_at DATETIME(3) NOT NULL,
    created_at DATETIME(3) NOT NULL,
    updated_at DATETIME(3) NOT NULL,
    KEY idx_workflow_outbox_due (status, next_run_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
`
