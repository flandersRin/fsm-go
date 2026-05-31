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
	runtime.RegisterTask("payment.charge", workflow.TaskHandlerFunc(func(context.Context, workflow.TaskContext) (workflow.TaskResult, error) {
		return workflow.TaskResult{Event: "PAYMENT_OK", Output: map[string]any{"paid_at": time.Now().UTC().Format(time.RFC3339)}}, nil
	}))
	instance, err := runtime.StartWorkflow(ctx, workflow.StartOptions{Workflow: "order", Version: "v1", InstanceID: "order-demo-1", Data: map[string]any{"amount": 100}})
	if err != nil {
		log.Fatal(err)
	}
	report, err := runtime.RunDueTasks(ctx, workflow.RunOptions{Limit: 10})
	if err != nil {
		log.Fatal(err)
	}
	current, err := runtime.GetWorkflow(ctx, instance.ID)
	if err != nil {
		log.Fatal(err)
	}
	history, err := runtime.ListHistory(ctx, instance.ID)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("state=%s status=%s tasks=%+v history=%d\n", current.State, current.Status, report, len(history))
}

func definition() *workflow.WorkflowDefinition {
	return &workflow.WorkflowDefinition{
		Name: "order", Version: "v1", Initial: "PENDING",
		States: []workflow.StateDefinition{{Name: "PENDING", OnEnter: []string{"charge_payment"}}, {Name: "PAID", Terminal: true}},
		Events: []workflow.EventDefinition{{Name: "PAYMENT_OK"}},
		Tasks: []workflow.TaskDefinition{{
			Name: "charge_payment", Handler: "payment.charge",
			Retry: workflow.RetryPolicy{MaxAttempts: 3, Backoff: time.Second},
		}},
		Transitions: []workflow.Transition{{Name: "payment_ok", From: "PENDING", Event: "PAYMENT_OK", To: "PAID"}},
	}
}
