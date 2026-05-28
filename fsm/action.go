package fsm

import (
	"context"
	"fmt"
)

// ActionContext 是事务内 Action 的执行上下文。
// Action 可以读取命令、实体、命中的迁移规则和当前结果，也可以通过 Tx 写入 Outbox 或业务扩展数据。
type ActionContext struct {
	Command    FireCommand        // 本次流转命令。
	Entity     *StateEntity       // 流转前读取到的实体状态。
	Transition CompiledTransition // 本次命中的迁移规则。
	Tx         TxRepository       // 当前事务内的存储接口。
	Result     *TransitionResult  // 即将返回并保存的流转结果。
}

// ActionFunc 是迁移规则绑定的动作函数。
// 当前 Runtime 只执行事务内 Action；返回错误会中断本次流转并回滚事务。
type ActionFunc func(context.Context, ActionContext) error

// ActionRegistry 保存可由 DSL 引用的动作函数。
// TransitionSpec.Actions.InTx 中的名称必须先注册到这里。
type ActionRegistry struct {
	actions map[string]ActionFunc
}

// NewActionRegistry 创建空动作注册表。
func NewActionRegistry() *ActionRegistry {
	return &ActionRegistry{actions: map[string]ActionFunc{}}
}

// Register 注册一个动作名称。
// 重复注册同名动作会覆盖旧动作。
func (r *ActionRegistry) Register(name string, fn ActionFunc) {
	r.actions[name] = fn
}

// Run 执行动作。
// 动作不存在或执行失败都会返回错误，Runtime 会把错误作为本次流转失败处理。
func (r *ActionRegistry) Run(ctx context.Context, name string, ac ActionContext) error {
	fn, ok := r.actions[name]
	if !ok {
		return fmt.Errorf("action not found: %s", name)
	}
	if err := fn(ctx, ac); err != nil {
		return fmt.Errorf("run action %s: %w", name, err)
	}
	return nil
}
