package main

import (
	"context"
	"fmt"
	"log"

	"github.com/flandersrin/fsm-go/actions"
	"github.com/flandersrin/fsm-go/fsm"
	"github.com/flandersrin/fsm-go/fsmtest"
)

func main() {
	ctx := context.Background()

	spec, err := fsm.LoadYAML("configs/order.v1.yaml")
	if err != nil {
		log.Fatal(err)
	}
	machine, err := fsm.Compile(spec)
	if err != nil {
		log.Fatal(err)
	}

	repo := fsmtest.NewMemoryRepository()
	registry := fsm.NewActionRegistry()
	actions.RegisterOutbox(registry, map[string]string{
		"outbox.order_paid": "order.paid",
	})

	runtime := fsm.NewRuntime(repo, registry)
	runtime.RegisterMachine(machine)

	err = runtime.CreateEntity(ctx, fsm.StateEntity{
		Machine:        "order",
		MachineVersion: "v1",
		EntityID:       "order-example-1",
		State:          "PENDING",
		Data:           map[string]any{},
	})
	if err != nil {
		log.Fatal(err)
	}

	result, err := runtime.Fire(ctx, fsm.FireCommand{
		Machine:        "order",
		MachineVersion: "v1",
		EntityID:       "order-example-1",
		Event:          "PAY_SUCCESS",
		Actor:          fsm.Actor{ID: "user-1", Role: "customer"},
		RequestID:      "request-example-1",
		IdempotencyKey: "payment-example-1",
		Payload: map[string]any{
			"paymentStatus": "SUCCESS",
			"amount":        100,
		},
	})
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("%s -> %s\n", result.FromState, result.ToState)
	fmt.Printf("logs=%d outbox=%d\n", len(repo.Logs()), len(repo.Outbox()))
}
