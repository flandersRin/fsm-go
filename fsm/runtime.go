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

// FireCommand 是一次状态流转请求。
// Runtime 会根据 Machine、MachineVersion、EntityID 读取当前实体状态，
// 用 Event 找到候选迁移规则，再用 Payload、Meta、Actor 等字段执行 guard 判断。
type FireCommand struct {
	Machine        string         // 状态机名称。
	MachineVersion string         // 状态机规则版本。
	EntityID       string         // 业务实体 ID。
	Event          string         // 本次触发的事件名称。
	Actor          Actor          // 触发流转的操作者信息，可在 guard 和日志中使用。
	RequestID      string         // 请求 ID，只用于记录和排查，不参与幂等判断。
	IdempotencyKey string         // 幂等键。非空且迁移规则开启幂等时，成功结果会被保存。
	Payload        map[string]any // 业务载荷，可在 guard、Action 和状态日志中使用。
	Meta           map[string]any // 调用侧扩展信息，可在 guard 和 Action 中使用。
}

// TransitionResult 是一次流转的返回结果。
// 普通成功会返回新的状态和版本号；命中幂等时会返回已保存的成功结果，并把 IdempotentHit 置为 true。
type TransitionResult struct {
	Machine        string    `json:"machine"`         // 状态机名称。
	MachineVersion string    `json:"machine_version"` // 状态机规则版本。
	EntityID       string    `json:"entity_id"`       // 业务实体 ID。
	Event          string    `json:"event"`           // 触发本次流转的事件。
	FromState      string    `json:"from_state"`      // 流转前状态。
	ToState        string    `json:"to_state"`        // 流转后状态。
	TransitionName string    `json:"transition_name"` // 命中的迁移规则名称。
	Revision       int64     `json:"revision"`        // 流转后的实体版本号。
	Changed        bool      `json:"changed"`         // 是否发生状态变化。当前成功流转恒为 true。
	IdempotentHit  bool      `json:"idempotent_hit"`  // 是否直接返回了已保存的幂等结果。
	CreatedAt      time.Time `json:"created_at"`      // 流转结果创建时间。
}

// Runtime 是状态流转的统一入口。
// 一次 Fire 会在 Repository 事务中完成幂等检查、实体读取、迁移匹配、guard 判断、
// 事务内 Action、CAS 状态更新、状态日志写入和幂等结果保存。
type Runtime struct {
	repo      Repository
	actions   *ActionRegistry
	observers []Observer

	mu       sync.RWMutex
	machines map[string]*Machine
}

// NewRuntime 创建 Runtime。actions 为空时会自动使用空 ActionRegistry。
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

// RegisterMachine 注册编译后的状态机规则。
// 同名不同版本会作为不同规则保存。
func (r *Runtime) RegisterMachine(machine *Machine) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.machines[machineKey(machine.Name, machine.Version)] = machine
}

// GetMachine 按名称和版本读取已注册的状态机规则。
func (r *Runtime) GetMachine(name string, version string) (*Machine, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	machine, ok := r.machines[machineKey(name, version)]
	if !ok {
		return nil, fmt.Errorf("machine not found: %s:%s", name, version)
	}
	return machine, nil
}

// CreateEntity 创建一条状态实体记录。
// 调用方需要传入 Machine、MachineVersion、EntityID 和初始 State。
func (r *Runtime) CreateEntity(ctx context.Context, entity StateEntity) error {
	return r.repo.WithTx(ctx, func(ctx context.Context, tx TxRepository) error {
		return tx.CreateEntity(ctx, entity)
	})
}

// Fire 触发一次状态流转。
// 若配置了 Observer，会在流转开始和结束时分别发送事件；没有 Observer 时会跳过观测事件构造。
func (r *Runtime) Fire(ctx context.Context, cmd FireCommand) (*TransitionResult, error) {
	if len(r.observers) == 0 {
		return r.fire(ctx, cmd)
	}

	startedAt := time.Now()
	r.observeTransitionStarted(ctx, cmd)
	result, err := r.fire(ctx, cmd)
	r.observeTransitionCompleted(ctx, cmd, result, err, time.Since(startedAt))
	return result, err
}

func (r *Runtime) fire(ctx context.Context, cmd FireCommand) (result *TransitionResult, err error) {
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
