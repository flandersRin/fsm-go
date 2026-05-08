package fsm

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/expr-lang/expr"
)

type Actor struct {
	ID   string
	Role string
}

type FireCommand struct {
	Machine        string
	MachineVersion string
	EntityID       string
	Event          string
	Actor          Actor
	RequestID      string
	IdempotencyKey string
	Payload        map[string]any
	Meta           map[string]any
}

type TransitionResult struct {
	Machine        string    `json:"machine"`
	MachineVersion string    `json:"machine_version"`
	EntityID       string    `json:"entity_id"`
	Event          string    `json:"event"`
	FromState      string    `json:"from_state"`
	ToState        string    `json:"to_state"`
	TransitionName string    `json:"transition_name"`
	Revision       int64     `json:"revision"`
	Changed        bool      `json:"changed"`
	IdempotentHit  bool      `json:"idempotent_hit"`
	CreatedAt      time.Time `json:"created_at"`
}

type Runtime struct {
	repo      Repository
	actions   *ActionRegistry
	observers []Observer

	mu       sync.RWMutex
	machines map[string]*Machine
}

func NewRuntime(repo Repository, actions *ActionRegistry, opts ...RuntimeOption) *Runtime {
	if actions == nil {
		actions = NewActionRegistry()
	}
	runtime := &Runtime{
		repo:     repo,
		actions:  actions,
		machines: map[string]*Machine{},
	}
	for _, opt := range opts {
		opt(runtime)
	}
	return runtime
}

func (r *Runtime) RegisterMachine(machine *Machine) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.machines[machineKey(machine.Name, machine.Version)] = machine
}

func (r *Runtime) GetMachine(name string, version string) (*Machine, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	machine, ok := r.machines[machineKey(name, version)]
	if !ok {
		return nil, fmt.Errorf("machine not found: %s:%s", name, version)
	}
	return machine, nil
}

func (r *Runtime) CreateEntity(ctx context.Context, entity StateEntity) error {
	return r.repo.WithTx(ctx, func(ctx context.Context, tx TxRepository) error {
		return tx.CreateEntity(ctx, entity)
	})
}

func (r *Runtime) Fire(ctx context.Context, cmd FireCommand) (result *TransitionResult, err error) {
	startedAt := time.Now()
	r.observeTransitionStarted(ctx, cmd)
	defer func() {
		r.observeTransitionCompleted(ctx, cmd, result, err, time.Since(startedAt))
	}()

	machine, err := r.GetMachine(cmd.Machine, cmd.MachineVersion)
	if err != nil {
		return nil, err
	}

	err = r.repo.WithTx(ctx, func(ctx context.Context, tx TxRepository) error {
		if cmd.IdempotencyKey != "" {
			idem, err := tx.TryGetIdempotency(ctx, cmd.Machine, cmd.IdempotencyKey)
			if err != nil {
				return err
			}
			if idem != nil && idem.Hit && idem.Result != nil {
				copied := *idem.Result
				copied.IdempotentHit = true
				result = &copied
				return nil
			}
		}

		entity, err := tx.GetEntity(ctx, cmd.Machine, cmd.EntityID)
		if err != nil {
			return err
		}

		transition, err := r.pickTransition(ctx, machine, entity, cmd)
		if err != nil {
			return err
		}

		next := &TransitionResult{
			Machine:        cmd.Machine,
			MachineVersion: cmd.MachineVersion,
			EntityID:       cmd.EntityID,
			Event:          cmd.Event,
			FromState:      entity.State,
			ToState:        transition.To,
			TransitionName: transition.Name,
			Revision:       entity.Revision + 1,
			Changed:        true,
			CreatedAt:      time.Now().UTC(),
		}

		ac := ActionContext{Command: cmd, Entity: entity, Transition: transition, Tx: tx, Result: next}
		for _, action := range transition.Actions.InTx {
			if err := r.actions.Run(ctx, action, ac); err != nil {
				return err
			}
		}

		updated, err := tx.UpdateStateCAS(ctx, cmd.Machine, cmd.EntityID, entity.State, transition.To, entity.Revision)
		if err != nil {
			return err
		}
		if !updated {
			return ErrConcurrentTransition{EntityID: cmd.EntityID}
		}

		if err := tx.InsertStateLog(ctx, StateLog{
			Machine:        cmd.Machine,
			MachineVersion: cmd.MachineVersion,
			EntityID:       cmd.EntityID,
			Event:          cmd.Event,
			FromState:      entity.State,
			ToState:        transition.To,
			TransitionName: transition.Name,
			ActorID:        cmd.Actor.ID,
			RequestID:      cmd.RequestID,
			IdempotencyKey: cmd.IdempotencyKey,
			Payload:        cmd.Payload,
		}); err != nil {
			return err
		}

		if cmd.IdempotencyKey != "" && transition.Idempotent {
			if err := tx.SaveIdempotencyResult(ctx, cmd.Machine, cmd.IdempotencyKey, cmd.EntityID, cmd.Event, *next); err != nil {
				return err
			}
		}

		result = next
		return nil
	})
	if err != nil {
		return nil, err
	}

	return result, nil
}

func (r *Runtime) pickTransition(ctx context.Context, machine *Machine, entity *StateEntity, cmd FireCommand) (CompiledTransition, error) {
	transitions, err := machine.FindTransitions(entity.State, cmd.Event)
	if err != nil {
		return CompiledTransition{}, err
	}

	for _, transition := range transitions {
		ok, err := evaluateGuard(ctx, transition, entity, cmd)
		if err != nil {
			return CompiledTransition{}, err
		}
		if ok {
			return transition, nil
		}
	}

	return CompiledTransition{}, ErrGuardRejected{State: entity.State, Event: cmd.Event}
}

func evaluateGuard(ctx context.Context, transition CompiledTransition, entity *StateEntity, cmd FireCommand) (bool, error) {
	if transition.Guard == "" {
		return true, nil
	}
	env := guardEnvPool.Get().(*guardEnv)
	env.Actor.ID = cmd.Actor.ID
	env.Actor.Role = cmd.Actor.Role
	env.Entity = entity.Data
	env.Payload = cmd.Payload
	env.Meta = cmd.Meta
	env.Event = cmd.Event
	env.State = entity.State
	defer func() {
		*env = guardEnv{}
		guardEnvPool.Put(env)
	}()
	program := transition.GuardCode
	if program == nil {
		var err error
		program, err = expr.Compile(transition.Guard, expr.Env(guardEnv{}), expr.AsBool())
		if err != nil {
			return false, fmt.Errorf("compile guard: %w", err)
		}
	}
	out, err := expr.Run(program, env)
	if err != nil {
		return false, fmt.Errorf("evaluate guard: %w", err)
	}
	ok, valid := out.(bool)
	if !valid {
		return false, fmt.Errorf("guard must return bool")
	}
	_ = ctx
	return ok, nil
}

type guardEnv struct {
	Actor   guardActor     `expr:"actor"`
	Entity  map[string]any `expr:"entity"`
	Payload map[string]any `expr:"payload"`
	Meta    map[string]any `expr:"meta"`
	Event   string         `expr:"event"`
	State   string         `expr:"state"`
}

type guardActor struct {
	ID   string `expr:"id"`
	Role string `expr:"role"`
}

var guardEnvPool = sync.Pool{
	New: func() any {
		return new(guardEnv)
	},
}

func machineKey(name string, version string) string {
	return name + ":" + version
}
