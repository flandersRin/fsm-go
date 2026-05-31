package main

import (
	"context"
	"fmt"
	"log"

	"github.com/flandersrin/workflow-go/workflow"
	"github.com/flandersrin/workflow-go/workflowtest"
)

func main() {
	ctx := context.Background()
	store := workflowtest.NewMemoryStore()
	runtime := workflow.NewRuntime(store)

	Register(runtime)
	RegisterTask(runtime, HandlerPaymentCharge, workflow.TaskHandlerFunc(chargePayment))

	instance, err := Start(ctx, runtime, "order-demo-1", map[string]any{"amount": 100})
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
	fmt.Printf("state=%s status=%s report=%+v history=%d\n", current.State, current.Status, report, len(history))
}

func chargePayment(context.Context, workflow.TaskContext) (workflow.TaskResult, error) {
	return workflow.TaskResult{}, nil
}
