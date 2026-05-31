package sqlstore

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/flandersrin/workflow-go/workflow"
)

// Dialect 描述 SQL 占位符风格。MySQL 使用 ?，PostgreSQL 使用 $1。
type Dialect string

const (
	MySQL    Dialect = "mysql"
	Postgres Dialect = "postgres"
)

// Store 是基于 database/sql 的默认持久化实现，供 MySQL 和 PostgreSQL 包复用。
type Store struct {
	db      *sql.DB
	dialect Dialect
}

// New 创建 SQL Store。
func New(db *sql.DB, dialect Dialect) *Store {
	return &Store{db: db, dialect: dialect}
}

// InitSchema 执行传入的建表 SQL。生产环境也可以把 SQL 交给迁移工具管理。
func (s *Store) InitSchema(ctx context.Context, schema string) error {
	for _, statement := range strings.Split(schema, ";") {
		statement = strings.TrimSpace(statement)
		if statement == "" {
			continue
		}
		if _, err := s.db.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("init schema: %w", err)
		}
	}
	return nil
}

func (s *Store) CreateInstance(ctx context.Context, req workflow.CreateInstanceRequest) error {
	return s.withTx(ctx, func(tx *sql.Tx) error {
		data, err := marshal(req.Instance.Data)
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, s.q(`INSERT INTO workflow_instance
(id, workflow, version, state, status, revision, data, started_at, updated_at, finished_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`),
			req.Instance.ID, req.Instance.Workflow, req.Instance.Version, req.Instance.State, req.Instance.Status,
			req.Instance.Revision, data, req.Instance.StartedAt, req.Instance.UpdatedAt, nullTime(req.Instance.FinishedAt))
		if err != nil {
			return fmt.Errorf("insert workflow instance: %w", err)
		}
		return s.appendAll(ctx, tx, req.History, req.Tasks, req.Outbox)
	})
}

func (s *Store) GetInstance(ctx context.Context, id string) (*workflow.WorkflowInstance, error) {
	row := s.db.QueryRowContext(ctx, s.q(`SELECT id, workflow, version, state, status, revision, data, started_at, updated_at, finished_at
FROM workflow_instance WHERE id = ?`), id)
	return scanInstance(row)
}

func (s *Store) UpdateInstance(ctx context.Context, req workflow.UpdateInstanceRequest) error {
	return s.withTx(ctx, func(tx *sql.Tx) error {
		data, err := marshal(req.Instance.Data)
		if err != nil {
			return err
		}
		res, err := tx.ExecContext(ctx, s.q(`UPDATE workflow_instance
SET state = ?, status = ?, revision = ?, data = ?, updated_at = ?, finished_at = ?
WHERE id = ? AND revision = ?`),
			req.Instance.State, req.Instance.Status, req.Instance.Revision, data, req.Instance.UpdatedAt,
			nullTime(req.Instance.FinishedAt), req.Instance.ID, req.ExpectedRevision)
		if err != nil {
			return fmt.Errorf("update workflow instance: %w", err)
		}
		affected, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if affected != 1 {
			return workflow.ErrConcurrentUpdate{InstanceID: req.Instance.ID}
		}
		return s.appendAll(ctx, tx, req.History, req.Tasks, req.Outbox)
	})
}

func (s *Store) AppendHistory(ctx context.Context, items []workflow.ExecutionHistory) error {
	return s.withTx(ctx, func(tx *sql.Tx) error {
		return s.insertHistory(ctx, tx, items)
	})
}

func (s *Store) ListHistory(ctx context.Context, instanceID string) ([]workflow.ExecutionHistory, error) {
	rows, err := s.db.QueryContext(ctx, s.q(`SELECT id, instance_id, type, message, state, task_id, task, event, payload, created_at
FROM workflow_history WHERE instance_id = ? ORDER BY created_at, id`), instanceID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []workflow.ExecutionHistory
	for rows.Next() {
		item, err := scanHistory(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

func (s *Store) ListDueTasks(ctx context.Context, limit int, now time.Time) ([]workflow.TaskExecution, error) {
	rows, err := s.db.QueryContext(ctx, s.q(`SELECT id, instance_id, task, handler, status, attempt, max_attempts,
next_run_at, timeout_at, last_error, input, output, created_at, updated_at
FROM workflow_task
WHERE status IN ('pending','retrying') AND next_run_at <= ?
ORDER BY next_run_at, id
LIMIT ?`), now, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []workflow.TaskExecution
	for rows.Next() {
		task, err := scanTask(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *task)
	}
	return out, rows.Err()
}

func (s *Store) MarkTaskRunning(ctx context.Context, id string, attempt int) (*workflow.TaskExecution, error) {
	res, err := s.db.ExecContext(ctx, s.q(`UPDATE workflow_task SET status = 'running', updated_at = ?
WHERE id = ? AND attempt = ? AND status IN ('pending','retrying')`), time.Now().UTC(), id, attempt)
	if err != nil {
		return nil, err
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return nil, err
	}
	if affected != 1 {
		return nil, workflow.ErrConcurrentUpdate{InstanceID: id}
	}
	row := s.db.QueryRowContext(ctx, s.q(`SELECT id, instance_id, task, handler, status, attempt, max_attempts,
next_run_at, timeout_at, last_error, input, output, created_at, updated_at
FROM workflow_task WHERE id = ?`), id)
	return scanTask(row)
}

func (s *Store) CompleteTask(ctx context.Context, req workflow.CompleteTaskRequest) error {
	return s.withTx(ctx, func(tx *sql.Tx) error {
		input, err := marshal(req.Task.Input)
		if err != nil {
			return err
		}
		output, err := marshal(req.Task.Output)
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, s.q(`UPDATE workflow_task
SET status = ?, attempt = ?, next_run_at = ?, timeout_at = ?, last_error = ?, input = ?, output = ?, updated_at = ?
WHERE id = ?`),
			req.Task.Status, req.Task.Attempt, req.Task.NextRunAt, nullTime(req.Task.TimeoutAt), nullString(req.Task.LastError),
			input, output, req.Task.UpdatedAt, req.Task.ID)
		if err != nil {
			return fmt.Errorf("complete task: %w", err)
		}
		return s.appendAll(ctx, tx, req.History, req.Tasks, req.Outbox)
	})
}

func (s *Store) RecordIdempotency(ctx context.Context, scope string, key string, resultID string) (bool, error) {
	_, err := s.db.ExecContext(ctx, s.q(`INSERT INTO workflow_idempotency (scope, idem_key, result_id, created_at)
VALUES (?, ?, ?, ?)`), scope, key, resultID, time.Now().UTC())
	if err == nil {
		return true, nil
	}
	existing, ok, getErr := s.GetIdempotency(ctx, scope, key)
	if getErr != nil {
		return false, err
	}
	return ok && existing == resultID, nil
}

func (s *Store) GetIdempotency(ctx context.Context, scope string, key string) (string, bool, error) {
	var resultID string
	err := s.db.QueryRowContext(ctx, s.q(`SELECT result_id FROM workflow_idempotency WHERE scope = ? AND idem_key = ?`), scope, key).Scan(&resultID)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return resultID, true, nil
}

func (s *Store) AppendOutbox(ctx context.Context, outbox []workflow.OutboxMessage) error {
	return s.withTx(ctx, func(tx *sql.Tx) error {
		return s.insertOutbox(ctx, tx, outbox)
	})
}

func (s *Store) ListOutbox(ctx context.Context, limit int) ([]workflow.OutboxMessage, error) {
	rows, err := s.db.QueryContext(ctx, s.q(`SELECT id, topic, msg_key, payload, status, attempt, next_run_at, created_at, updated_at
FROM workflow_outbox
WHERE status = 'pending' AND next_run_at <= ?
ORDER BY next_run_at, id
LIMIT ?`), time.Now().UTC(), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []workflow.OutboxMessage
	for rows.Next() {
		msg, err := scanOutbox(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, msg)
	}
	return out, rows.Err()
}

func (s *Store) MarkOutboxPublished(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, s.q(`UPDATE workflow_outbox SET status = 'published', updated_at = ? WHERE id = ?`), time.Now().UTC(), id)
	return err
}

func (s *Store) appendAll(ctx context.Context, tx *sql.Tx, history []workflow.ExecutionHistory, tasks []workflow.TaskExecution, outbox []workflow.OutboxMessage) error {
	if err := s.insertHistory(ctx, tx, history); err != nil {
		return err
	}
	if err := s.insertTasks(ctx, tx, tasks); err != nil {
		return err
	}
	return s.insertOutbox(ctx, tx, outbox)
}

func (s *Store) insertHistory(ctx context.Context, tx *sql.Tx, items []workflow.ExecutionHistory) error {
	for _, item := range items {
		payload, err := marshal(item.Payload)
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, s.q(`INSERT INTO workflow_history
(id, instance_id, type, message, state, task_id, task, event, payload, created_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`),
			item.ID, item.InstanceID, item.Type, item.Message, item.State, nullString(item.TaskID),
			nullString(item.Task), nullString(item.Event), payload, item.CreatedAt)
		if err != nil {
			return fmt.Errorf("insert history: %w", err)
		}
	}
	return nil
}

func (s *Store) insertTasks(ctx context.Context, tx *sql.Tx, tasks []workflow.TaskExecution) error {
	for _, task := range tasks {
		input, err := marshal(task.Input)
		if err != nil {
			return err
		}
		output, err := marshal(task.Output)
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, s.q(`INSERT INTO workflow_task
(id, instance_id, task, handler, status, attempt, max_attempts, next_run_at, timeout_at, last_error, input, output, created_at, updated_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`),
			task.ID, task.InstanceID, task.Task, task.Handler, task.Status, task.Attempt, task.MaxAttempts,
			task.NextRunAt, nullTime(task.TimeoutAt), nullString(task.LastError), input, output, task.CreatedAt, task.UpdatedAt)
		if err != nil {
			return fmt.Errorf("insert task: %w", err)
		}
	}
	return nil
}

func (s *Store) insertOutbox(ctx context.Context, tx *sql.Tx, outbox []workflow.OutboxMessage) error {
	for _, msg := range outbox {
		if msg.ID == "" {
			msg.ID = workflow.NewID("msg")
		}
		if msg.Status == "" {
			msg.Status = "pending"
		}
		now := time.Now().UTC()
		if msg.CreatedAt.IsZero() {
			msg.CreatedAt = now
		}
		if msg.UpdatedAt.IsZero() {
			msg.UpdatedAt = msg.CreatedAt
		}
		if msg.NextRunAt.IsZero() {
			msg.NextRunAt = msg.CreatedAt
		}
		payload, err := marshal(msg.Payload)
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, s.q(`INSERT INTO workflow_outbox
(id, topic, msg_key, payload, status, attempt, next_run_at, created_at, updated_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`),
			msg.ID, msg.Topic, msg.Key, payload, msg.Status, msg.Attempt, msg.NextRunAt, msg.CreatedAt, msg.UpdatedAt)
		if err != nil {
			return fmt.Errorf("insert outbox: %w", err)
		}
	}
	return nil
}

func (s *Store) withTx(ctx context.Context, fn func(*sql.Tx) error) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

func (s *Store) q(query string) string {
	if s.dialect != Postgres {
		return query
	}
	var b strings.Builder
	n := 1
	for _, ch := range query {
		if ch == '?' {
			fmt.Fprintf(&b, "$%d", n)
			n++
			continue
		}
		b.WriteRune(ch)
	}
	return b.String()
}

type scanner interface {
	Scan(dest ...any) error
}

func scanInstance(row scanner) (*workflow.WorkflowInstance, error) {
	var item workflow.WorkflowInstance
	var data []byte
	var finished sql.NullTime
	err := row.Scan(&item.ID, &item.Workflow, &item.Version, &item.State, &item.Status, &item.Revision, &data, &item.StartedAt, &item.UpdatedAt, &finished)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, workflow.ErrNotFound{Resource: "workflow_instance", ID: item.ID}
	}
	if err != nil {
		return nil, err
	}
	item.FinishedAt = finished.Time
	return &item, unmarshal(data, &item.Data)
}

func scanTask(row scanner) (*workflow.TaskExecution, error) {
	var item workflow.TaskExecution
	var input, output []byte
	var timeout sql.NullTime
	var lastError sql.NullString
	err := row.Scan(&item.ID, &item.InstanceID, &item.Task, &item.Handler, &item.Status, &item.Attempt, &item.MaxAttempts,
		&item.NextRunAt, &timeout, &lastError, &input, &output, &item.CreatedAt, &item.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, workflow.ErrNotFound{Resource: "task_execution", ID: item.ID}
	}
	if err != nil {
		return nil, err
	}
	item.TimeoutAt = timeout.Time
	item.LastError = lastError.String
	if err := unmarshal(input, &item.Input); err != nil {
		return nil, err
	}
	return &item, unmarshal(output, &item.Output)
}

func scanHistory(row scanner) (workflow.ExecutionHistory, error) {
	var item workflow.ExecutionHistory
	var payload []byte
	var taskID, task, event sql.NullString
	err := row.Scan(&item.ID, &item.InstanceID, &item.Type, &item.Message, &item.State, &taskID, &task, &event, &payload, &item.CreatedAt)
	item.TaskID = taskID.String
	item.Task = task.String
	item.Event = event.String
	if err != nil {
		return workflow.ExecutionHistory{}, err
	}
	return item, unmarshal(payload, &item.Payload)
}

func scanOutbox(row scanner) (workflow.OutboxMessage, error) {
	var item workflow.OutboxMessage
	var payload []byte
	err := row.Scan(&item.ID, &item.Topic, &item.Key, &payload, &item.Status, &item.Attempt, &item.NextRunAt, &item.CreatedAt, &item.UpdatedAt)
	if err != nil {
		return workflow.OutboxMessage{}, err
	}
	return item, unmarshal(payload, &item.Payload)
}

func marshal(value map[string]any) ([]byte, error) {
	if value == nil {
		value = map[string]any{}
	}
	return json.Marshal(value)
}

func unmarshal(raw []byte, target *map[string]any) error {
	if len(raw) == 0 {
		*target = map[string]any{}
		return nil
	}
	return json.Unmarshal(raw, target)
}

func nullString(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func nullTime(value time.Time) any {
	if value.IsZero() {
		return nil
	}
	return value
}
