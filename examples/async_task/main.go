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
	attempts := 0
	runtime.RegisterTask("report.build", workflow.TaskHandlerFunc(func(context.Context, workflow.TaskContext) (workflow.TaskResult, error) {
		attempts++
		if attempts == 1 {
			return workflow.TaskResult{}, errors.New("temporary error")
		}
		return workflow.TaskResult{Event: "DONE"}, nil
	}))
	if _, err := runtime.StartWorkflow(ctx, workflow.StartOptions{Workflow: "async_report", Version: "v1", InstanceID: "report-1"}); err != nil {
		log.Fatal(err)
	}
	now := time.Now().UTC()
	first, _ := runtime.RunDueTasks(ctx, workflow.RunOptions{Limit: 10, Now: now})
	second, _ := runtime.RunDueTasks(ctx, workflow.RunOptions{Limit: 10, Now: now.Add(time.Second)})
	current, _ := runtime.GetWorkflow(ctx, "report-1")
	fmt.Printf("first=%+v second=%+v state=%s attempts=%d\n", first, second, current.State, attempts)
}

func definition() *workflow.WorkflowDefinition {
	return &workflow.WorkflowDefinition{
		Name: "async_report", Version: "v1", Initial: "BUILDING",
		States:      []workflow.StateDefinition{{Name: "BUILDING", OnEnter: []string{"build"}}, {Name: "DONE", Terminal: true}},
		Events:      []workflow.EventDefinition{{Name: "DONE"}},
		Tasks:       []workflow.TaskDefinition{{Name: "build", Handler: "report.build", Retry: workflow.RetryPolicy{MaxAttempts: 2, Backoff: time.Second}}},
		Transitions: []workflow.Transition{{Name: "done", From: "BUILDING", Event: "DONE", To: "DONE"}},
	}
}
