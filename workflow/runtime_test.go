package workflow_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/flandersrin/workflow-go/workflow"
	"github.com/flandersrin/workflow-go/workflowtest"
)

type fixedClock struct {
	now time.Time
}

func (c fixedClock) Now() time.Time { return c.now }

func TestRuntimeCompletesTaskDrivenWorkflow(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 5, 31, 10, 0, 0, 0, time.UTC)
	runtime, store := newRuntime(t, now)
	runtime.RegisterTask("payment.charge", workflow.TaskHandlerFunc(func(context.Context, workflow.TaskContext) (workflow.TaskResult, error) {
		return workflow.TaskResult{Event: "PAYMENT_OK", Output: map[string]any{"payment_id": "pay-1"}}, nil
	}))

	instance, err := runtime.StartWorkflow(ctx, workflow.StartOptions{
		Workflow: "order", Version: "v1", InstanceID: "order-1", IdempotencyKey: "start-order-1",
		Data: map[string]any{"amount": 100},
	})
	if err != nil {
		t.Fatal(err)
	}
	if instance.State != "PENDING" {
		t.Fatalf("expected PENDING, got %s", instance.State)
	}

	report, err := runtime.RunDueTasks(ctx, workflow.RunOptions{Limit: 10, Now: now})
	if err != nil {
		t.Fatal(err)
	}
	if report.Succeeded != 1 {
		t.Fatalf("expected one succeeded task, got %+v", report)
	}
	got, err := runtime.GetWorkflow(ctx, "order-1")
	if err != nil {
		t.Fatal(err)
	}
	if got.State != "PAID" || got.Status != workflow.InstanceCompleted {
		t.Fatalf("unexpected instance: %+v", got)
	}
	history, err := store.ListHistory(ctx, "order-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(history) < 5 {
		t.Fatalf("expected rich history, got %d", len(history))
	}
}

func TestRuntimeRetriesAndCompensatesFailedTask(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 5, 31, 10, 0, 0, 0, time.UTC)
	runtime, store := newRuntime(t, now)
	failCount := 0
	runtime.RegisterTask("payment.charge", workflow.TaskHandlerFunc(func(context.Context, workflow.TaskContext) (workflow.TaskResult, error) {
		failCount++
		return workflow.TaskResult{}, errors.New("payment gateway down")
	}))
	runtime.RegisterTask("payment.refund", workflow.TaskHandlerFunc(func(context.Context, workflow.TaskContext) (workflow.TaskResult, error) {
		return workflow.TaskResult{}, nil
	}))

	if _, err := runtime.StartWorkflow(ctx, workflow.StartOptions{Workflow: "order", Version: "v1", InstanceID: "order-2"}); err != nil {
		t.Fatal(err)
	}
	report, err := runtime.RunDueTasks(ctx, workflow.RunOptions{Limit: 10, Now: now})
	if err != nil {
		t.Fatal(err)
	}
	if report.Retried != 1 {
		t.Fatalf("expected retry report, got %+v", report)
	}
	report, err = runtime.RunDueTasks(ctx, workflow.RunOptions{Limit: 10, Now: now.Add(time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	if report.Failed != 1 {
		t.Fatalf("expected failed report, got %+v", report)
	}
	report, err = runtime.RunDueTasks(ctx, workflow.RunOptions{Limit: 10, Now: now.Add(time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	if report.Succeeded != 1 {
		t.Fatalf("expected compensation success, got %+v", report)
	}
	if failCount != 2 {
		t.Fatalf("expected two attempts, got %d", failCount)
	}
	history, err := store.ListHistory(ctx, "order-2")
	if err != nil {
		t.Fatal(err)
	}
	var compensated bool
	for _, item := range history {
		if item.Type == workflow.HistoryCompensation {
			compensated = true
		}
	}
	if !compensated {
		t.Fatal("expected compensation history")
	}
}

func TestCompileRejectsBrokenDefinition(t *testing.T) {
	_, err := workflow.Compile(&workflow.WorkflowDefinition{
		Name: "broken", Version: "v1", Initial: "A",
		States: []workflow.StateDefinition{{Name: "A", OnEnter: []string{"missing"}}},
	})
	if !errors.As(err, &workflow.ErrInvalidDefinition{}) {
		t.Fatalf("expected invalid definition, got %v", err)
	}
}

func newRuntime(t *testing.T, now time.Time) (*workflow.Runtime, *workflowtest.MemoryStore) {
	t.Helper()
	store := workflowtest.NewMemoryStore()
	runtime := workflow.NewRuntime(store, workflow.WithClock(fixedClock{now: now}))
	machine, err := workflow.Compile(orderDefinition())
	if err != nil {
		t.Fatal(err)
	}
	runtime.RegisterMachine(machine)
	return runtime, store
}

func orderDefinition() *workflow.WorkflowDefinition {
	return &workflow.WorkflowDefinition{
		Name: "order", Version: "v1", Initial: "PENDING",
		States: []workflow.StateDefinition{
			{Name: "PENDING", OnEnter: []string{"charge_payment"}},
			{Name: "PAID", Terminal: true},
			{Name: "FAILED", Terminal: true},
		},
		Events: []workflow.EventDefinition{{Name: "PAYMENT_OK"}, {Name: "PAYMENT_FAILED"}},
		Tasks: []workflow.TaskDefinition{
			{
				Name: "charge_payment", Handler: "payment.charge",
				Retry:        workflow.RetryPolicy{MaxAttempts: 2, Backoff: time.Second},
				Compensation: "refund_payment", OnFailure: "PAYMENT_FAILED",
			},
			{Name: "refund_payment", Handler: "payment.refund", Retry: workflow.RetryPolicy{MaxAttempts: 1, Backoff: time.Second}},
		},
		Transitions: []workflow.Transition{
			{Name: "payment_ok", From: "PENDING", Event: "PAYMENT_OK", To: "PAID"},
			{Name: "payment_failed", From: "PENDING", Event: "PAYMENT_FAILED", To: "FAILED"},
		},
	}
}
