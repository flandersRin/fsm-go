package fsmtest

import (
	"context"
	"errors"
	"sync"

	"github.com/flandersrin/fsm-go/fsm"
)

type MemoryRepository struct {
	mu           sync.Mutex
	entities     map[string]*fsm.StateEntity
	logs         []fsm.StateLog
	outbox       []fsm.OutboxMessage
	idempotency  map[string]fsm.TransitionResult
	nextOutboxID int64
}

func NewMemoryRepository() *MemoryRepository {
	return &MemoryRepository{
		entities:    map[string]*fsm.StateEntity{},
		idempotency: map[string]fsm.TransitionResult{},
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

func (tx *memoryTx) CreateEntity(_ context.Context, entity fsm.StateEntity) error {
	copied := entity
	tx.repo.entities[entity.Machine+":"+entity.EntityID] = &copied
	return nil
}

func (tx *memoryTx) GetEntity(_ context.Context, machine string, entityID string) (*fsm.StateEntity, error) {
	entity, ok := tx.repo.entities[machine+":"+entityID]
	if !ok {
		return nil, errors.New("entity not found")
	}
	copied := *entity
	return &copied, nil
}

func (tx *memoryTx) UpdateStateCAS(_ context.Context, machine string, entityID string, fromState string, toState string, revision int64) (bool, error) {
	entity := tx.repo.entities[machine+":"+entityID]
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
	result, ok := tx.repo.idempotency[machine+":"+idempotencyKey]
	if !ok {
		return &fsm.IdempotencyResult{}, nil
	}
	return &fsm.IdempotencyResult{Hit: true, Result: &result}, nil
}

func (tx *memoryTx) SaveIdempotencyResult(_ context.Context, machine string, idempotencyKey string, _ string, _ string, result fsm.TransitionResult) error {
	tx.repo.idempotency[machine+":"+idempotencyKey] = result
	return nil
}

func (tx *memoryTx) InsertOutbox(_ context.Context, msg fsm.OutboxMessage) error {
	tx.repo.nextOutboxID++
	msg.ID = tx.repo.nextOutboxID
	tx.repo.outbox = append(tx.repo.outbox, msg)
	return nil
}
