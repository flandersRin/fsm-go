package workflow_test

import (
	"errors"
	"testing"
	"time"

	"github.com/flandersrin/workflow-go/workflow"
)

const (
	statePending workflow.StateName   = "PENDING"
	statePaid    workflow.StateName   = "PAID"
	eventPaid    workflow.EventName   = "PAID"
	taskCharge   workflow.TaskName    = "charge"
	handlerPay   workflow.HandlerName = "payment.charge"
)

func TestDefinitionBuilderCreatesCompilableWorkflow(t *testing.T) {
	def := workflow.NewDefinition("typed_order", "v1").
		Initial(statePending).
		State(statePending, workflow.OnEnter(taskCharge)).
		State(statePaid, workflow.Terminal()).
		Event(eventPaid).
		Task(taskCharge, handlerPay, workflow.Retry(3, time.Second), workflow.OnSuccess(eventPaid)).
		Transition("paid", statePending, eventPaid, statePaid).
		Build()

	machine, err := workflow.Compile(def)
	if err != nil {
		t.Fatal(err)
	}
	if machine.Initial != string(statePending) {
		t.Fatalf("unexpected initial state: %s", machine.Initial)
	}
}

func TestCompileRejectsTaskUnknownEvent(t *testing.T) {
	def := workflow.NewDefinition("broken", "v1").
		Initial(statePending).
		State(statePending, workflow.OnEnter(taskCharge)).
		Task(taskCharge, handlerPay, workflow.OnSuccess(eventPaid)).
		Build()

	_, err := workflow.Compile(def)
	if !errors.As(err, &workflow.ErrInvalidDefinition{}) {
		t.Fatalf("expected invalid definition, got %v", err)
	}
}
