package benchmark_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/flandersrin/workflow-go/workflow"
	"github.com/flandersrin/workflow-go/workflowtest"
)

func BenchmarkMemoryRuntimeStartAndRun(b *testing.B) {
	ctx := context.Background()
	store := workflowtest.NewMemoryStore()
	runtime := workflow.NewRuntime(store)
	machine, err := workflow.Compile(&workflow.WorkflowDefinition{
		Name: "bench", Version: "v1", Initial: "PENDING",
		States:      []workflow.StateDefinition{{Name: "PENDING", OnEnter: []string{"task"}}, {Name: "DONE", Terminal: true}},
		Events:      []workflow.EventDefinition{{Name: "DONE"}},
		Tasks:       []workflow.TaskDefinition{{Name: "task", Handler: "noop", Retry: workflow.RetryPolicy{MaxAttempts: 1, Backoff: time.Millisecond}}},
		Transitions: []workflow.Transition{{Name: "done", From: "PENDING", Event: "DONE", To: "DONE"}},
	})
	if err != nil {
		b.Fatal(err)
	}
	runtime.RegisterMachine(machine)
	runtime.RegisterTask("noop", workflow.TaskHandlerFunc(func(context.Context, workflow.TaskContext) (workflow.TaskResult, error) {
		return workflow.TaskResult{Event: "DONE"}, nil
	}))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := runtime.StartWorkflow(ctx, workflow.StartOptions{Workflow: "bench", Version: "v1", InstanceID: fmt.Sprintf("bench-%d", i)}); err != nil {
			b.Fatal(err)
		}
	}
	if _, err := runtime.RunDueTasks(ctx, workflow.RunOptions{Limit: b.N}); err != nil {
		b.Fatal(err)
	}
}
