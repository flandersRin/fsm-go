package mysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/flandersrin/fsm-go/fsm"
)

type Repository struct {
	db *sql.DB
}

func NewRepository(db *sql.DB) *Repository {
	return &Repository{db: db}
}

func (r *Repository) InitSchema(ctx context.Context) error {
	statements := strings.Split(Schema, ";")
	for _, statement := range statements {
		statement = strings.TrimSpace(statement)
		if statement == "" {
			continue
		}
		if _, err := r.db.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("init schema: %w", err)
		}
	}
	return nil
}

func (r *Repository) WithTx(ctx context.Context, fn func(context.Context, fsm.TxRepository) error) error {
	tx, err := r.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return err
	}

	txRepo := &TxRepository{tx: tx}
	if err := fn(ctx, txRepo); err != nil {
		_ = tx.Rollback()
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	return nil
}

type TxRepository struct {
	tx *sql.Tx
}

func (r *TxRepository) CreateEntity(ctx context.Context, entity fsm.StateEntity) error {
	data, err := json.Marshal(entity.Data)
	if err != nil {
		return err
	}
	_, err = r.tx.ExecContext(ctx, `
INSERT INTO fsm_entity (machine, machine_version, entity_id, state, revision, data)
VALUES (?, ?, ?, ?, ?, ?)
ON DUPLICATE KEY UPDATE
    machine_version = VALUES(machine_version),
    state = VALUES(state),
    revision = VALUES(revision),
    data = VALUES(data),
    deleted = 0
`, entity.Machine, entity.MachineVersion, entity.EntityID, entity.State, entity.Revision, string(data))
	if err != nil {
		return fmt.Errorf("create entity: %w", err)
	}
	return nil
}

func (r *TxRepository) GetEntity(ctx context.Context, machine string, entityID string) (*fsm.StateEntity, error) {
	var entity fsm.StateEntity
	var raw []byte
	err := r.tx.QueryRowContext(ctx, `
SELECT machine, machine_version, entity_id, state, revision, COALESCE(data, JSON_OBJECT()), create_at, update_at
FROM fsm_entity
WHERE machine = ? AND entity_id = ? AND deleted = 0
`, machine, entityID).Scan(
		&entity.Machine,
		&entity.MachineVersion,
		&entity.EntityID,
		&entity.State,
		&entity.Revision,
		&raw,
		&entity.CreatedAt,
		&entity.UpdatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("get entity: %w", err)
	}
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &entity.Data); err != nil {
			return nil, fmt.Errorf("decode entity data: %w", err)
		}
	}
	return &entity, nil
}

func (r *TxRepository) UpdateStateCAS(ctx context.Context, machine string, entityID string, fromState string, toState string, revision int64) (bool, error) {
	res, err := r.tx.ExecContext(ctx, `
UPDATE fsm_entity
SET state = ?, revision = revision + 1
WHERE machine = ? AND entity_id = ? AND state = ? AND revision = ? AND deleted = 0
`, toState, machine, entityID, fromState, revision)
	if err != nil {
		return false, fmt.Errorf("update state cas: %w", err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	return affected == 1, nil
}

func (r *TxRepository) InsertStateLog(ctx context.Context, log fsm.StateLog) error {
	payload, err := json.Marshal(log.Payload)
	if err != nil {
		return err
	}
	_, err = r.tx.ExecContext(ctx, `
INSERT INTO fsm_state_log (
    machine, machine_version, entity_id, event, from_state, to_state,
    transition_name, actor_id, request_id, idempotency_key, payload
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
`, log.Machine, log.MachineVersion, log.EntityID, log.Event, log.FromState, log.ToState, log.TransitionName, nullIfEmpty(log.ActorID), nullIfEmpty(log.RequestID), nullIfEmpty(log.IdempotencyKey), string(payload))
	if err != nil {
		return fmt.Errorf("insert state log: %w", err)
	}
	return nil
}

func (r *TxRepository) TryGetIdempotency(ctx context.Context, machine string, idempotencyKey string) (*fsm.IdempotencyResult, error) {
	var raw []byte
	err := r.tx.QueryRowContext(ctx, `
SELECT result
FROM fsm_idempotency
WHERE machine = ? AND idempotency_key = ? AND status = 'DONE' AND deleted = 0
	`, machine, idempotencyKey).Scan(&raw)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get idempotency: %w", err)
	}

	var result fsm.TransitionResult
	if err := json.Unmarshal(raw, &result); err != nil {
		return nil, fmt.Errorf("decode idempotency result: %w", err)
	}
	return &fsm.IdempotencyResult{Hit: true, Result: &result}, nil
}

func (r *TxRepository) SaveIdempotencyResult(ctx context.Context, machine string, idempotencyKey string, entityID string, event string, result fsm.TransitionResult) error {
	raw, err := json.Marshal(result)
	if err != nil {
		return err
	}
	_, err = r.tx.ExecContext(ctx, `
INSERT INTO fsm_idempotency (machine, idempotency_key, entity_id, event, result, status)
VALUES (?, ?, ?, ?, ?, 'DONE')
ON DUPLICATE KEY UPDATE result = VALUES(result), status = 'DONE'
`, machine, idempotencyKey, entityID, event, string(raw))
	if err != nil {
		return fmt.Errorf("save idempotency: %w", err)
	}
	return nil
}

func (r *TxRepository) InsertOutbox(ctx context.Context, msg fsm.OutboxMessage) error {
	payload, err := json.Marshal(msg.Payload)
	if err != nil {
		return err
	}
	_, err = r.tx.ExecContext(ctx, `
INSERT INTO fsm_outbox (topic, msg_key, payload, status, next_retry_at)
VALUES (?, ?, ?, 'PENDING', ?)
`, msg.Topic, msg.Key, string(payload), time.Now().UTC())
	if err != nil {
		return fmt.Errorf("insert outbox: %w", err)
	}
	return nil
}

func nullIfEmpty(value string) any {
	if value == "" {
		return nil
	}
	return value
}
