package workflow

import "time"

// InstanceStatus 表示流程实例当前是否还需要运行时继续推进。
// 调度器只会拉取 running 状态下的到期任务。
type InstanceStatus string

const (
	InstanceRunning   InstanceStatus = "running"
	InstanceCompleted InstanceStatus = "completed"
	InstanceFailed    InstanceStatus = "failed"
)

// TaskStatus 表示一次任务执行记录的生命周期。
// failed 表示任务已经失败且没有后续重试；retrying 表示运行时会在 NextRunAt 后再次调度。
type TaskStatus string

const (
	TaskPending   TaskStatus = "pending"
	TaskRunning   TaskStatus = "running"
	TaskSucceeded TaskStatus = "succeeded"
	TaskRetrying  TaskStatus = "retrying"
	TaskFailed    TaskStatus = "failed"
	TaskTimedOut  TaskStatus = "timed_out"
)

// HistoryType 用于区分执行时间线中的事件类别。
// 这些事件是恢复、排查和开源可观测体验的基础，不应被当成普通日志替代。
type HistoryType string

const (
	HistoryWorkflowStarted   HistoryType = "workflow_started"
	HistoryWorkflowCompleted HistoryType = "workflow_completed"
	HistoryWorkflowFailed    HistoryType = "workflow_failed"
	HistorySignalReceived    HistoryType = "signal_received"
	HistoryStateChanged      HistoryType = "state_changed"
	HistoryTaskScheduled     HistoryType = "task_scheduled"
	HistoryTaskStarted       HistoryType = "task_started"
	HistoryTaskSucceeded     HistoryType = "task_succeeded"
	HistoryTaskRetrying      HistoryType = "task_retrying"
	HistoryTaskFailed        HistoryType = "task_failed"
	HistoryTaskTimedOut      HistoryType = "task_timed_out"
	HistoryCompensation      HistoryType = "compensation_scheduled"
	HistoryOutboxAppended    HistoryType = "outbox_appended"
)

// WorkflowDefinition 是一类流程的定义。状态只表达流程所处阶段，任务负责真正执行工作。
type WorkflowDefinition struct {
	Name        string            `yaml:"name"`
	Version     string            `yaml:"version"`
	Initial     string            `yaml:"initial"`
	States      []StateDefinition `yaml:"states"`
	Events      []EventDefinition `yaml:"events"`
	Tasks       []TaskDefinition  `yaml:"tasks"`
	Transitions []Transition      `yaml:"transitions"`
}

// StateName 是类型化状态名。业务侧可以通过生成代码得到常量，避免到处手写字符串。
type StateName string

// EventName 是类型化事件名。它让状态转移、Signal 调用和任务返回使用同一组常量。
type EventName string

// TaskName 是类型化任务名。状态入口和补偿任务都应引用这个类型。
type TaskName string

// HandlerName 是类型化处理器名。运行时注册和任务定义使用同一组常量。
type HandlerName string

// StateDefinition 描述流程中的稳定状态。OnEnter 只负责调度任务，不直接执行业务代码。
type StateDefinition struct {
	Name     string   `yaml:"name"`
	Terminal bool     `yaml:"terminal"`
	OnEnter  []string `yaml:"on_enter"`
}

// EventDefinition 描述流程可以接收的事件。
type EventDefinition struct {
	Name string `yaml:"name"`
}

// TaskDefinition 描述可执行任务及其失败策略。
// Handler 指向运行时注册的任务处理器，Retry、Timeout、Compensation 由运行时统一解释。
type TaskDefinition struct {
	Name         string        `yaml:"name"`
	Handler      string        `yaml:"handler"`
	Retry        RetryPolicy   `yaml:"retry"`
	Timeout      time.Duration `yaml:"timeout"`
	Compensation string        `yaml:"compensation"`
	OnSuccess    string        `yaml:"on_success"`
	OnFailure    string        `yaml:"on_failure"`
}

// RetryPolicy 描述任务失败后的重试策略。MaxAttempts 包含第一次执行。
type RetryPolicy struct {
	MaxAttempts int           `yaml:"max_attempts"`
	Backoff     time.Duration `yaml:"backoff"`
}

// Transition 描述事件驱动的状态变化。
type Transition struct {
	Name  string `yaml:"name"`
	From  string `yaml:"from"`
	Event string `yaml:"event"`
	To    string `yaml:"to"`
}

// WorkflowInstance 是一次真实流程运行的当前快照。
type WorkflowInstance struct {
	ID         string
	Workflow   string
	Version    string
	State      string
	Status     InstanceStatus
	Revision   int64
	Data       map[string]any
	StartedAt  time.Time
	UpdatedAt  time.Time
	FinishedAt time.Time
}

// TaskExecution 记录某个任务的一次可恢复执行状态。
type TaskExecution struct {
	ID          string
	InstanceID  string
	Task        string
	Handler     string
	Status      TaskStatus
	Attempt     int
	MaxAttempts int
	NextRunAt   time.Time
	TimeoutAt   time.Time
	LastError   string
	Input       map[string]any
	Output      map[string]any
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

// WorkflowEvent 是进入流程运行时的事件。
type WorkflowEvent struct {
	ID             string
	InstanceID     string
	Name           string
	IdempotencyKey string
	Payload        map[string]any
	CreatedAt      time.Time
}

// ExecutionHistory 是可展示、可恢复排查的执行时间线。
type ExecutionHistory struct {
	ID         string
	InstanceID string
	Type       HistoryType
	Message    string
	State      string
	TaskID     string
	Task       string
	Event      string
	Payload    map[string]any
	CreatedAt  time.Time
}

// OutboxMessage 是运行时在本地事务中追加的可靠消息。
type OutboxMessage struct {
	ID        string
	Topic     string
	Key       string
	Payload   map[string]any
	Status    string
	Attempt   int
	NextRunAt time.Time
	CreatedAt time.Time
	UpdatedAt time.Time
}

// CreateInstanceRequest 把创建实例时需要原子写入的内容聚合成一个对象。
// 这样后续增加审计、Outbox 或索引字段时，不会破坏 Store 接口的参数顺序。
type CreateInstanceRequest struct {
	Instance WorkflowInstance
	History  []ExecutionHistory
	Tasks    []TaskExecution
	Outbox   []OutboxMessage
}

// UpdateInstanceRequest 表示一次实例推进需要原子提交的全部变更。
type UpdateInstanceRequest struct {
	Instance         WorkflowInstance
	ExpectedRevision int64
	History          []ExecutionHistory
	Tasks            []TaskExecution
	Outbox           []OutboxMessage
}

// CompleteTaskRequest 表示一次任务结束需要原子提交的全部变更。
type CompleteTaskRequest struct {
	Task    TaskExecution
	History []ExecutionHistory
	Tasks   []TaskExecution
	Outbox  []OutboxMessage
}

// HistoryInput 是构造时间线记录的参数对象。
type HistoryInput struct {
	InstanceID string
	Type       HistoryType
	Message    string
	State      string
	TaskID     string
	Task       string
	Event      string
	Payload    map[string]any
	CreatedAt  time.Time
}

// StartOptions 是启动流程时的输入。
type StartOptions struct {
	InstanceID     string
	Workflow       string
	Version        string
	IdempotencyKey string
	Data           map[string]any
}

// SignalOptions 是向流程发送事件时的输入。
type SignalOptions struct {
	InstanceID     string
	Event          string
	IdempotencyKey string
	Payload        map[string]any
}

// RunOptions 控制一次调度扫描的规模。
type RunOptions struct {
	Limit int
	Now   time.Time
}

// RunReport 汇总一次任务调度结果，便于脚本和服务端监控。
type RunReport struct {
	Scanned   int
	Succeeded int
	Retried   int
	Failed    int
	TimedOut  int
}
