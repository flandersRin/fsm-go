package workflow

import "time"

// DefinitionBuilder 用 Go 代码构建流程定义。
// 它适合生成代码和业务手写定义，比直接拼 WorkflowDefinition 更不容易漏字段或写错引用关系。
type DefinitionBuilder struct {
	def WorkflowDefinition
}

// NewDefinition 创建流程定义构建器。
func NewDefinition(name string, version string) *DefinitionBuilder {
	return &DefinitionBuilder{def: WorkflowDefinition{Name: name, Version: version}}
}

// Initial 设置初始状态。
func (b *DefinitionBuilder) Initial(state StateName) *DefinitionBuilder {
	b.def.Initial = string(state)
	return b
}

// State 添加普通状态。
func (b *DefinitionBuilder) State(state StateName, opts ...StateOption) *DefinitionBuilder {
	item := StateDefinition{Name: string(state)}
	for _, opt := range opts {
		opt(&item)
	}
	b.def.States = append(b.def.States, item)
	return b
}

// Event 添加流程事件。
func (b *DefinitionBuilder) Event(event EventName) *DefinitionBuilder {
	b.def.Events = append(b.def.Events, EventDefinition{Name: string(event)})
	return b
}

// Task 添加任务定义。
func (b *DefinitionBuilder) Task(task TaskName, handler HandlerName, opts ...TaskOption) *DefinitionBuilder {
	item := TaskDefinition{Name: string(task), Handler: string(handler)}
	for _, opt := range opts {
		opt(&item)
	}
	b.def.Tasks = append(b.def.Tasks, item)
	return b
}

// Transition 添加事件驱动的状态转移。
func (b *DefinitionBuilder) Transition(name string, from StateName, event EventName, to StateName) *DefinitionBuilder {
	b.def.Transitions = append(b.def.Transitions, Transition{
		Name: name, From: string(from), Event: string(event), To: string(to),
	})
	return b
}

// Build 返回可编译的流程定义。调用方仍应执行 Compile，获得完整引用校验。
func (b *DefinitionBuilder) Build() *WorkflowDefinition {
	out := b.def
	return &out
}

// MustCompile 编译流程定义，失败时 panic。它适合在生成代码的 init 或测试里快速暴露定义错误。
func (b *DefinitionBuilder) MustCompile() *Machine {
	machine, err := Compile(b.Build())
	if err != nil {
		panic(err)
	}
	return machine
}

// StateOption 修改状态定义。
type StateOption func(*StateDefinition)

// Terminal 标记终态。
func Terminal() StateOption {
	return func(state *StateDefinition) {
		state.Terminal = true
	}
}

// OnEnter 声明进入状态后要调度的任务。
func OnEnter(tasks ...TaskName) StateOption {
	return func(state *StateDefinition) {
		for _, task := range tasks {
			state.OnEnter = append(state.OnEnter, string(task))
		}
	}
}

// TaskOption 修改任务定义。
type TaskOption func(*TaskDefinition)

// Retry 设置任务重试策略。
func Retry(maxAttempts int, backoff time.Duration) TaskOption {
	return func(task *TaskDefinition) {
		task.Retry = RetryPolicy{MaxAttempts: maxAttempts, Backoff: backoff}
	}
}

// Timeout 设置任务执行超时。
func Timeout(timeout time.Duration) TaskOption {
	return func(task *TaskDefinition) {
		task.Timeout = timeout
	}
}

// Compensation 设置任务最终失败后的补偿任务。
func Compensation(taskName TaskName) TaskOption {
	return func(task *TaskDefinition) {
		task.Compensation = string(taskName)
	}
}

// OnSuccess 设置任务成功后默认发出的事件。
func OnSuccess(event EventName) TaskOption {
	return func(task *TaskDefinition) {
		task.OnSuccess = string(event)
	}
}

// OnFailure 设置任务最终失败后发出的事件。
func OnFailure(event EventName) TaskOption {
	return func(task *TaskDefinition) {
		task.OnFailure = string(event)
	}
}
