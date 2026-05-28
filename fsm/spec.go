package fsm

// MachineSpec 是一份状态机 DSL 的根配置。
// 同一个 machine 可以通过不同 version 同时注册多份规则，用于灰度或历史版本兼容。
type MachineSpec struct {
	Machine     string           `yaml:"machine"`     // 状态机名称，例如 order。
	Version     string           `yaml:"version"`     // 状态机规则版本，例如 v1。
	Initial     string           `yaml:"initial"`     // 新实体的初始状态。
	States      []StateSpec      `yaml:"states"`      // 状态集合，Transition 的 from/to 必须引用这里的状态。
	Events      []EventSpec      `yaml:"events"`      // 事件集合，Transition 的 event 必须引用这里的事件。
	Transitions []TransitionSpec `yaml:"transitions"` // 状态迁移规则集合。
}

// StateSpec 描述 DSL 中的一个状态。
type StateSpec struct {
	Name     string `yaml:"name"`     // 状态名称。
	Terminal bool   `yaml:"terminal"` // 终态。进入终态后不再允许继续流转。
}

// EventSpec 描述 DSL 中的一个可触发事件。
type EventSpec struct {
	Name string `yaml:"name"`
}

// TransitionSpec 描述一次状态迁移规则。
// Runtime 会按 from + event 找到候选规则，再按 priority 从高到低判断 guard。
type TransitionSpec struct {
	Name       string     `yaml:"name"`       // 迁移名称，会写入流转结果和状态日志。
	From       string     `yaml:"from"`       // 允许触发该规则的源状态。
	Event      string     `yaml:"event"`      // 触发该规则的事件。
	To         string     `yaml:"to"`         // 成功流转后的目标状态。
	Priority   int        `yaml:"priority"`   // 同一 from + event 下的匹配优先级，数值越大越先判断。
	Guard      string     `yaml:"guard"`      // expr 表达式，返回 true 才允许流转；为空表示直接通过。
	Idempotent bool       `yaml:"idempotent"` // 为 true 且命令带 IdempotencyKey 时，成功结果会被保存并可复用。
	Actions    ActionSpec `yaml:"actions"`    // 流转过程中要执行的动作。
}

// ActionSpec 描述迁移规则绑定的动作。
type ActionSpec struct {
	InTx []string `yaml:"in_tx"` // 事务内动作。任一动作失败都会导致本次流转失败。

	// AfterCommit 预留给事务提交后动作。当前 Runtime 尚未执行该字段。
	AfterCommit []string `yaml:"after_commit"`
}
