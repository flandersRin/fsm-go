package main

import (
	"context"
	"flag"
	"fmt"
	"time"

	"github.com/flandersrin/workflow-go/workflow"
	"github.com/flandersrin/workflow-go/workflowtest"
)

func main() {
	n := flag.Int("n", 20000, "流程实例数量")
	flag.Parse()

	ctx := context.Background()
	store := workflowtest.NewMemoryStore()
	runtime := workflow.NewRuntime(store)
	machine, err := workflow.Compile(definition())
	if err != nil {
		panic(err)
	}
	runtime.RegisterMachine(machine)
	runtime.RegisterTask("order.noop", workflow.TaskHandlerFunc(func(context.Context, workflow.TaskContext) (workflow.TaskResult, error) {
		return workflow.TaskResult{Event: "DONE"}, nil
	}))

	start := time.Now()
	for i := 0; i < *n; i++ {
		_, err := runtime.StartWorkflow(ctx, workflow.StartOptions{
			Workflow: "perf_order", Version: "v1", InstanceID: fmt.Sprintf("perf-%08d", i),
			Data: map[string]any{"i": i},
		})
		if err != nil {
			panic(err)
		}
	}
	startDuration := time.Since(start)

	runStart := time.Now()
	total := 0
	for total < *n {
		report, err := runtime.RunDueTasks(ctx, workflow.RunOptions{Limit: 1000})
		if err != nil {
			panic(err)
		}
		total += report.Succeeded
		if report.Scanned == 0 {
			break
		}
	}
	runDuration := time.Since(runStart)
	fmt.Printf("instances=%d start_duration=%s start_tps=%.0f task_duration=%s task_tps=%.0f completed=%d\n",
		*n, startDuration, float64(*n)/startDuration.Seconds(), runDuration, float64(total)/runDuration.Seconds(), total)
}

func definition() *workflow.WorkflowDefinition {
	return &workflow.WorkflowDefinition{
		Name: "perf_order", Version: "v1", Initial: "PENDING",
		States:      []workflow.StateDefinition{{Name: "PENDING", OnEnter: []string{"noop"}}, {Name: "DONE", Terminal: true}},
		Events:      []workflow.EventDefinition{{Name: "DONE"}},
		Tasks:       []workflow.TaskDefinition{{Name: "noop", Handler: "order.noop", Retry: workflow.RetryPolicy{MaxAttempts: 1, Backoff: time.Millisecond}}},
		Transitions: []workflow.Transition{{Name: "done", From: "PENDING", Event: "DONE", To: "DONE"}},
	}
}
