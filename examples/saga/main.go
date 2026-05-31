package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/flandersrin/workflow-go/workflow"
	"github.com/flandersrin/workflow-go/workflowtest"
)

func main() {
	ctx := context.Background()
	store := workflowtest.NewMemoryStore()
	runtime := workflow.NewRuntime(store)
	machine, err := workflow.Compile(definition())
	if err != nil {
		log.Fatal(err)
	}
	runtime.RegisterMachine(machine)
	runtime.RegisterTask("inventory.reserve", workflow.TaskHandlerFunc(func(context.Context, workflow.TaskContext) (workflow.TaskResult, error) {
		return workflow.TaskResult{}, errors.New("inventory locked")
	}))
	runtime.RegisterTask("inventory.release", workflow.TaskHandlerFunc(func(context.Context, workflow.TaskContext) (workflow.TaskResult, error) {
		return workflow.TaskResult{}, nil
	}))
	if _, err := runtime.StartWorkflow(ctx, workflow.StartOptions{Workflow: "saga_order", Version: "v1", InstanceID: "saga-1"}); err != nil {
		log.Fatal(err)
	}
	now := time.Now().UTC()
	report, _ := runtime.RunDueTasks(ctx, workflow.RunOptions{Limit: 10, Now: now})
	compensation, _ := runtime.RunDueTasks(ctx, workflow.RunOptions{Limit: 10, Now: now})
	history, _ := runtime.ListHistory(ctx, "saga-1")
	fmt.Printf("report=%+v compensation=%+v history=%d\n", report, compensation, len(history))
}

func definition() *workflow.WorkflowDefinition {
	return &workflow.WorkflowDefinition{
		Name: "saga_order", Version: "v1", Initial: "RESERVING",
		States: []workflow.StateDefinition{{Name: "RESERVING", OnEnter: []string{"reserve"}}, {Name: "FAILED", Terminal: true}},
		Events: []workflow.EventDefinition{{Name: "FAILED"}},
		Tasks: []workflow.TaskDefinition{
			{Name: "reserve", Handler: "inventory.reserve", Retry: workflow.RetryPolicy{MaxAttempts: 1, Backoff: time.Second}, Compensation: "release", OnFailure: "FAILED"},
			{Name: "release", Handler: "inventory.release", Retry: workflow.RetryPolicy{MaxAttempts: 1, Backoff: time.Second}},
		},
		Transitions: []workflow.Transition{{Name: "failed", From: "RESERVING", Event: "FAILED", To: "FAILED"}},
	}
}
