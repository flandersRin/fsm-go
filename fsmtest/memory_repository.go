package fsmtest

import (
	"context"
	"errors"
	"sync"

	"github.com/flandersrin/fsm-go/fsm"
)

type MemoryRepository struct {
	mu           sync.Mutex
	entities     map[entityKey]*fsm.StateEntity
	logs         []fsm.StateLog
	outbox       []fsm.OutboxMessage
	idempotency  map[idempotencyKey]fsm.TransitionResult
	nextOutboxID int64
}

func NewMemoryRepository() *MemoryRepository {
	return NewMemoryRepositoryWithCapacity(0, 0, 0, 0)
}

func NewMemoryRepositoryWithCapacity(entityCapacity int, logCapacity int, outboxCapacity int, idempotencyCapacity int) *MemoryRepository {
	return &MemoryRepository{
		entities:    make(map[entityKey]*fsm.StateEntity, entityCapacity),
		logs:        make([]fsm.StateLog, 0, logCapacity),
		outbox:      make([]fsm.OutboxMessage, 0, outboxCapacity),
		idempotency: make(map[idempotencyKey]fsm.TransitionResult, idempotencyCapacity),
	}
}

func (r *MemoryRepository) Logs() []fsm.StateLog {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]fsm.StateLog(nil), r.logs...)
}

func (r *MemoryRepository) Outbox() []fsm.OutboxMessage {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]fsm.OutboxMessage(nil), r.outbox...)
}

func (r *MemoryRepository) WithTx(ctx context.Context, fn func(context.Context, fsm.TxRepository) error) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return fn(ctx, &memoryTx{repo: r})
}

type memoryTx struct {
	repo *MemoryRepository
}

type entityKey struct {
	machine  string
	entityID string
}

type idempotencyKey struct {
	machine string
	key     string
}

func (tx *memoryTx) CreateEntity(_ context.Context, entity fsm.StateEntity) error {
	tx.repo.entities[entityKey{machine: entity.Machine, entityID: entity.EntityID}] = new(entity)
	return nil
}

func (tx *memoryTx) GetEntity(_ context.Context, machine string, entityID string) (*fsm.StateEntity, error) {
	entity, ok := tx.repo.entities[entityKey{machine: machine, entityID: entityID}]
	if !ok {
		return nil, errors.New("entity not found")
	}
	return new(*entity), nil
}

func (tx *memoryTx) UpdateStateCAS(_ context.Context, machine string, entityID string, fromState string, toState string, revision int64) (bool, error) {
	entity := tx.repo.entities[entityKey{machine: machine, entityID: entityID}]
	if entity == nil || entity.State != fromState || entity.Revision != revision {
		return false, nil
	}
	entity.State = toState
	entity.Revision++
	return true, nil
}

func (tx *memoryTx) InsertStateLog(_ context.Context, log fsm.StateLog) error {
	tx.repo.logs = append(tx.repo.logs, log)
	return nil
}

func (tx *memoryTx) TryGetIdempotency(_ context.Context, machine string, idempotencyKey string) (*fsm.IdempotencyResult, error) {
	result, ok := tx.repo.idempotency[idempotencyKeyFor(machine, idempotencyKey)]
	if !ok {
		return nil, nil
	}
	return &fsm.IdempotencyResult{Hit: true, Result: &result}, nil
}

func (tx *memoryTx) SaveIdempotencyResult(_ context.Context, machine string, idempotencyKey string, _ string, _ string, result fsm.TransitionResult) error {
	tx.repo.idempotency[idempotencyKeyFor(machine, idempotencyKey)] = result
	return nil
}

func idempotencyKeyFor(machine string, key string) idempotencyKey {
	return idempotencyKey{machine: machine, key: key}
}

func (tx *memoryTx) InsertOutbox(_ context.Context, msg fsm.OutboxMessage) error {
	tx.repo.nextOutboxID++
	msg.ID = tx.repo.nextOutboxID
	tx.repo.outbox = append(tx.repo.outbox, msg)
	return nil
}
