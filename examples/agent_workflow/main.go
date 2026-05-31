package main

import (
	"context"
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
	runtime.RegisterTask("agent.plan", workflow.TaskHandlerFunc(func(context.Context, workflow.TaskContext) (workflow.TaskResult, error) {
		return workflow.TaskResult{Event: "PLAN_READY", Output: map[string]any{"tool": "search"}}, nil
	}))
	if _, err := runtime.StartWorkflow(ctx, workflow.StartOptions{Workflow: "agent_run", Version: "v1", InstanceID: "agent-1"}); err != nil {
		log.Fatal(err)
	}
	report, _ := runtime.RunDueTasks(ctx, workflow.RunOptions{Limit: 10})
	current, _ := runtime.GetWorkflow(ctx, "agent-1")
	fmt.Printf("state=%s report=%+v\n", current.State, report)
}

func definition() *workflow.WorkflowDefinition {
	return &workflow.WorkflowDefinition{
		Name: "agent_run", Version: "v1", Initial: "THINKING",
		States: []workflow.StateDefinition{{Name: "THINKING", OnEnter: []string{"plan"}}, {Name: "WAITING_TOOL"}, {Name: "COMPLETED", Terminal: true}},
		Events: []workflow.EventDefinition{{Name: "PLAN_READY"}, {Name: "TOOL_DONE"}},
		Tasks:  []workflow.TaskDefinition{{Name: "plan", Handler: "agent.plan", Retry: workflow.RetryPolicy{MaxAttempts: 2, Backoff: time.Second}}},
		Transitions: []workflow.Transition{
			{Name: "plan_ready", From: "THINKING", Event: "PLAN_READY", To: "WAITING_TOOL"},
			{Name: "tool_done", From: "WAITING_TOOL", Event: "TOOL_DONE", To: "COMPLETED"},
		},
	}
}
