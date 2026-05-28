package fsmtest

import (
	"context"
	"errors"
	"sync"

	"github.com/flandersrin/fsm-go/fsm"
)

// MemoryRepository 是面向测试、示例和 benchmark 的内存 Repository。
// 它用互斥锁模拟事务边界，适合本地验证状态流转行为；不要直接作为生产存储使用。
type MemoryRepository struct {
	mu           sync.Mutex
	entities     map[entityKey]*fsm.StateEntity
	logs         []fsm.StateLog
	outbox       []fsm.OutboxMessage
	idempotency  map[idempotencyKey]fsm.TransitionResult
	nextOutboxID int64
	tx           memoryTx
}

// NewMemoryRepository 创建不预设容量的内存 Repository。
func NewMemoryRepository() *MemoryRepository {
	return NewMemoryRepositoryWithCapacity(0, 0, 0, 0)
}

// NewMemoryRepositoryWithCapacity 创建可预设容量的内存 Repository。
// benchmark 可以通过预留实体、日志、Outbox 和幂等结果容量，减少 slice 或 map 扩容带来的噪声。
func NewMemoryRepositoryWithCapacity(entityCapacity int, logCapacity int, outboxCapacity int, idempotencyCapacity int) *MemoryRepository {
	repo := &MemoryRepository{
		entities:    make(map[entityKey]*fsm.StateEntity, entityCapacity),
		logs:        make([]fsm.StateLog, 0, logCapacity),
		outbox:      make([]fsm.OutboxMessage, 0, outboxCapacity),
		idempotency: make(map[idempotencyKey]fsm.TransitionResult, idempotencyCapacity),
	}
	repo.tx.repo = repo
	return repo
}

// Logs 返回状态日志副本。
func (r *MemoryRepository) Logs() []fsm.StateLog {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]fsm.StateLog(nil), r.logs...)
}

// Outbox 返回 Outbox 消息副本。
func (r *MemoryRepository) Outbox() []fsm.OutboxMessage {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]fsm.OutboxMessage(nil), r.outbox...)
}

// WithTx 在互斥锁保护下执行事务函数。
func (r *MemoryRepository) WithTx(ctx context.Context, fn func(context.Context, fsm.TxRepository) error) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return fn(ctx, &r.tx)
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
