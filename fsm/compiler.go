package fsm

import (
	"fmt"
	"sort"

	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/vm"
)

type Machine struct {
	Name        string
	Version     string
	Initial     string
	States      map[string]State
	Events      map[string]Event
	Transitions map[string][]CompiledTransition
}

type State struct {
	Name     string
	Terminal bool
}

type Event struct {
	Name string
}

type CompiledTransition struct {
	Name       string
	From       string
	Event      string
	To         string
	Priority   int
	Guard      string
	GuardCode  *vm.Program
	Idempotent bool
	Actions    ActionSpec
}

func Compile(spec *MachineSpec) (*Machine, error) {
	if err := Validate(spec); err != nil {
		return nil, err
	}

	machine := &Machine{
		Name:        spec.Machine,
		Version:     spec.Version,
		Initial:     spec.Initial,
		States:      make(map[string]State, len(spec.States)),
		Events:      make(map[string]Event, len(spec.Events)),
		Transitions: make(map[string][]CompiledTransition, len(spec.Transitions)),
	}

	for _, state := range spec.States {
		machine.States[state.Name] = State(state)
	}
	for _, event := range spec.Events {
		machine.Events[event.Name] = Event(event)
	}
	for _, transition := range spec.Transitions {
		compiled := CompiledTransition{
			Name:       transition.Name,
			From:       transition.From,
			Event:      transition.Event,
			To:         transition.To,
			Priority:   transition.Priority,
			Guard:      transition.Guard,
			Idempotent: transition.Idempotent,
			Actions:    transition.Actions,
		}
		if transition.Guard != "" {
			program, err := expr.Compile(transition.Guard, expr.Env(guardEnv{}), expr.AsBool())
			if err != nil {
				return nil, fmt.Errorf("compile guard %s: %w", transition.Name, err)
			}
			compiled.GuardCode = program
		}
		key := transitionKey(transition.From, transition.Event)
		machine.Transitions[key] = append(machine.Transitions[key], compiled)
	}
	for key := range machine.Transitions {
		sort.SliceStable(machine.Transitions[key], func(i, j int) bool {
			return machine.Transitions[key][i].Priority > machine.Transitions[key][j].Priority
		})
	}

	return machine, nil
}

func (m *Machine) FindTransitions(state string, event string) ([]CompiledTransition, error) {
	if current, ok := m.States[state]; ok && current.Terminal {
		return nil, ErrTerminalState{State: state}
	}
	transitions := m.Transitions[transitionKey(state, event)]
	if len(transitions) == 0 {
		return nil, ErrInvalidTransition{State: state, Event: event}
	}
	return transitions, nil
}

func transitionKey(state string, event string) string {
	return state + ":" + event
}
