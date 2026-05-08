package fsm

import (
	"context"
	"errors"
	"sync"
	"testing"
)

func TestValidateRejectsTerminalOutgoingTransition(t *testing.T) {
	spec := testSpec()
	spec.States[1].Terminal = true

	err := Validate(spec)
	if err == nil {
		t.Fatal("expected validation error")
	}
}

func TestRuntimeFireWritesLogOutboxAndIdempotency(t *testing.T) {
	ctx := context.Background()
	repo := newMemoryRepository()
	registry := NewActionRegistry()
	registry.Register("outbox.order_paid", func(ctx context.Context, ac ActionContext) error {
		return ac.Tx.InsertOutbox(ctx, OutboxMessage{Topic: "order.paid", Key: ac.Command.EntityID, Payload: map[string]any{"event": ac.Command.Event}})
	})

	machine, err := Compile(testSpec())
	if err != nil {
		t.Fatal(err)
	}
	runtime := NewRuntime(repo, registry)
	runtime.RegisterMachine(machine)

	if err := runtime.CreateEntity(ctx, StateEntity{
		Machine:        "order",
		MachineVersion: "v1",
		EntityID:       "order-1",
		State:          "PENDING",
		Data:           map[string]any{},
	}); err != nil {
		t.Fatal(err)
	}

	cmd := FireCommand{
		Machine:        "order",
		MachineVersion: "v1",
		EntityID:       "order-1",
		Event:          "PAY_SUCCESS",
		Actor:          Actor{ID: "u1", Role: "customer"},
		IdempotencyKey: "pay-1",
		Payload:        map[string]any{"paymentStatus": "SUCCESS", "amount": 100},
	}
	result, err := runtime.Fire(ctx, cmd)
	if err != nil {
		t.Fatal(err)
	}
	if result.FromState != "PENDING" || result.ToState != "PAID" {
		t.Fatalf("unexpected result: %+v", result)
	}
	if len(repo.logs) != 1 {
		t.Fatalf("expected 1 log, got %d", len(repo.logs))
	}
	if len(repo.outbox) != 1 {
		t.Fatalf("expected 1 outbox message, got %d", len(repo.outbox))
	}

	again, err := runtime.Fire(ctx, cmd)
	if err != nil {
		t.Fatal(err)
	}
	if !again.IdempotentHit {
		t.Fatal("expected idempotent hit")
	}
	if len(repo.outbox) != 1 {
		t.Fatalf("idempotent hit must not write outbox again, got %d", len(repo.outbox))
	}
}

func TestRuntimeRejectsGuardAndTerminalTransitions(t *testing.T) {
	ctx := context.Background()
	repo := newMemoryRepository()
	registry := NewActionRegistry()
	registry.Register("outbox.order_paid", func(context.Context, ActionContext) error { return nil })

	machine, err := Compile(testSpec())
	if err != nil {
		t.Fatal(err)
	}
	runtime := NewRuntime(repo, registry)
	runtime.RegisterMachine(machine)

	if err := runtime.CreateEntity(ctx, StateEntity{Machine: "order", MachineVersion: "v1", EntityID: "order-2", State: "PENDING", Data: map[string]any{}}); err != nil {
		t.Fatal(err)
	}
	_, err = runtime.Fire(ctx, FireCommand{
		Machine:        "order",
		MachineVersion: "v1",
		EntityID:       "order-2",
		Event:          "PAY_SUCCESS",
		Payload:        map[string]any{"paymentStatus": "FAILED", "amount": 100},
	})
	if !errors.As(err, &ErrGuardRejected{}) {
		t.Fatalf("expected guard rejected, got %v", err)
	}

	repo.entities["order:order-2"].State = "COMPLETED"
	_, err = runtime.Fire(ctx, FireCommand{Machine: "order", MachineVersion: "v1", EntityID: "order-2", Event: "PAY_SUCCESS"})
	if !errors.As(err, &ErrTerminalState{}) {
		t.Fatalf("expected terminal state error, got %v", err)
	}
}

func TestCompileSortsTransitionsByPriority(t *testing.T) {
	spec := &MachineSpec{
		Machine: "m",
		Version: "v1",
		Initial: "A",
		States:  []StateSpec{{Name: "A"}, {Name: "B"}, {Name: "C"}},
		Events:  []EventSpec{{Name: "GO"}},
		Transitions: []TransitionSpec{
			{Name: "low", From: "A", Event: "GO", To: "B", Priority: 1},
			{Name: "high", From: "A", Event: "GO", To: "C", Priority: 10},
		},
	}
	machine, err := Compile(spec)
	if err != nil {
		t.Fatal(err)
	}
	transitions, err := machine.FindTransitions("A", "GO")
	if err != nil {
		t.Fatal(err)
	}
	if transitions[0].Name != "high" {
		t.Fatalf("expected high priority first, got %s", transitions[0].Name)
	}
}

func testSpec() *MachineSpec {
	return &MachineSpec{
		Machine: "order",
		Version: "v1",
		Initial: "PENDING",
		States: []StateSpec{
			{Name: "PENDING"},
			{Name: "PAID"},
			{Name: "COMPLETED", Terminal: true},
		},
		Events: []EventSpec{
			{Name: "PAY_SUCCESS"},
			{Name: "FINISH"},
		},
		Transitions: []TransitionSpec{
			{
				Name:       "pay_success",
				From:       "PENDING",
				Event:      "PAY_SUCCESS",
				To:         "PAID",
				Priority:   10,
				Guard:      "payload.paymentStatus == 'SUCCESS' && payload.amount > 0",
				Idempotent: true,
				Actions:    ActionSpec{InTx: []string{"outbox.order_paid"}},
			},
			{
				Name:  "finish",
				From:  "PAID",
				Event: "FINISH",
				To:    "COMPLETED",
			},
		},
	}
}

type memoryRepository struct {
	mu           sync.Mutex
	entities     map[string]*StateEntity
	logs         []StateLog
	outbox       []OutboxMessage
	idempotency  map[string]TransitionResult
	nextOutboxID int64
}

func newMemoryRepository() *memoryRepository {
	return &memoryRepository{
		entities:    map[string]*StateEntity{},
		idempotency: map[string]TransitionResult{},
	}
}

func (r *memoryRepository) WithTx(ctx context.Context, fn func(context.Context, TxRepository) error) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return fn(ctx, &memoryTx{repo: r})
}

type memoryTx struct {
	repo *memoryRepository
}

func (tx *memoryTx) CreateEntity(_ context.Context, entity StateEntity) error {
	tx.repo.entities[entity.Machine+":"+entity.EntityID] = new(entity)
	return nil
}

func (tx *memoryTx) GetEntity(_ context.Context, machine string, entityID string) (*StateEntity, error) {
	entity, ok := tx.repo.entities[machine+":"+entityID]
	if !ok {
		return nil, errors.New("entity not found")
	}
	return new(*entity), nil
}

func (tx *memoryTx) UpdateStateCAS(_ context.Context, machine string, entityID string, fromState string, toState string, revision int64) (bool, error) {
	entity := tx.repo.entities[machine+":"+entityID]
	if entity.State != fromState || entity.Revision != revision {
		return false, nil
	}
	entity.State = toState
	entity.Revision++
	return true, nil
}

func (tx *memoryTx) InsertStateLog(_ context.Context, log StateLog) error {
	tx.repo.logs = append(tx.repo.logs, log)
	return nil
}

func (tx *memoryTx) TryGetIdempotency(_ context.Context, machine string, idempotencyKey string) (*IdempotencyResult, error) {
	result, ok := tx.repo.idempotency[machine+":"+idempotencyKey]
	if !ok {
		return nil, nil
	}
	return &IdempotencyResult{Hit: true, Result: &result}, nil
}

func (tx *memoryTx) SaveIdempotencyResult(_ context.Context, machine string, idempotencyKey string, _ string, _ string, result TransitionResult) error {
	tx.repo.idempotency[machine+":"+idempotencyKey] = result
	return nil
}

func (tx *memoryTx) InsertOutbox(_ context.Context, msg OutboxMessage) error {
	tx.repo.nextOutboxID++
	msg.ID = tx.repo.nextOutboxID
	tx.repo.outbox = append(tx.repo.outbox, msg)
	return nil
}
