package postgres

// Schema 是 PostgreSQL 默认表结构，覆盖实例、任务、历史、幂等和 Outbox。
const Schema = `
CREATE TABLE IF NOT EXISTS workflow_instance (
    id TEXT PRIMARY KEY,
    workflow TEXT NOT NULL,
    version TEXT NOT NULL,
    state TEXT NOT NULL,
    status TEXT NOT NULL,
    revision BIGINT NOT NULL,
    data JSONB,
    started_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_workflow_instance_workflow_state ON workflow_instance (workflow, state);
CREATE INDEX IF NOT EXISTS idx_workflow_instance_status ON workflow_instance (status);

CREATE TABLE IF NOT EXISTS workflow_task (
    id TEXT PRIMARY KEY,
    instance_id TEXT NOT NULL,
    task TEXT NOT NULL,
    handler TEXT NOT NULL,
    status TEXT NOT NULL,
    attempt INT NOT NULL,
    max_attempts INT NOT NULL,
    next_run_at TIMESTAMPTZ NOT NULL,
    timeout_at TIMESTAMPTZ,
    last_error TEXT,
    input JSONB,
    output JSONB,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_workflow_task_due ON workflow_task (status, next_run_at);
CREATE INDEX IF NOT EXISTS idx_workflow_task_instance ON workflow_task (instance_id);

CREATE TABLE IF NOT EXISTS workflow_history (
    id TEXT PRIMARY KEY,
    instance_id TEXT NOT NULL,
    type TEXT NOT NULL,
    message TEXT NOT NULL,
    state TEXT NOT NULL,
    task_id TEXT,
    task TEXT,
    event TEXT,
    payload JSONB,
    created_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_workflow_history_instance ON workflow_history (instance_id, created_at);

CREATE TABLE IF NOT EXISTS workflow_idempotency (
    scope TEXT NOT NULL,
    idem_key TEXT NOT NULL,
    result_id TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (scope, idem_key)
);

CREATE TABLE IF NOT EXISTS workflow_outbox (
    id TEXT PRIMARY KEY,
    topic TEXT NOT NULL,
    msg_key TEXT NOT NULL,
    payload JSONB NOT NULL,
    status TEXT NOT NULL,
    attempt INT NOT NULL,
    next_run_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_workflow_outbox_due ON workflow_outbox (status, next_run_at);
`
