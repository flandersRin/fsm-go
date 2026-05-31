package workflow

import (
	"context"
	"time"
)

// Store 是 workflow-go 的持久化边界。
// 实现必须保证同一实例的状态推进、任务记录、历史追加和 Outbox 追加具备一致性。
type Store interface {
	CreateInstance(context.Context, CreateInstanceRequest) error
	GetInstance(context.Context, string) (*WorkflowInstance, error)
	UpdateInstance(context.Context, UpdateInstanceRequest) error
	AppendHistory(context.Context, []ExecutionHistory) error
	ListHistory(context.Context, string) ([]ExecutionHistory, error)
	ListDueTasks(context.Context, int, time.Time) ([]TaskExecution, error)
	MarkTaskRunning(context.Context, string, int) (*TaskExecution, error)
	CompleteTask(context.Context, CompleteTaskRequest) error
	RecordIdempotency(context.Context, string, string, string) (bool, error)
	GetIdempotency(context.Context, string, string) (string, bool, error)
	AppendOutbox(context.Context, []OutboxMessage) error
	ListOutbox(context.Context, int) ([]OutboxMessage, error)
	MarkOutboxPublished(context.Context, string) error
}

// MessagePublisher 是对外消息系统的最小接口。Kafka 是默认实现，其他消息队列可按此适配。
type MessagePublisher interface {
	Publish(context.Context, Message) error
}

// MessageConsumer 是消息消费的最小接口，消费方必须用 Message.ID 做幂等。
type MessageConsumer interface {
	Consume(context.Context, func(context.Context, Message) error) error
}

// Message 是 workflow-go 与消息系统之间的通用消息格式。
type Message struct {
	ID      string
	Topic   string
	Key     string
	Payload []byte
	Headers map[string]string
}

// TaskHandler 是业务任务执行入口。实现方应保证同一 TaskExecution 重试时不会重复产生不可控副作用。
type TaskHandler interface {
	HandleTask(context.Context, TaskContext) (TaskResult, error)
}

// TaskHandlerFunc 让普通函数可以直接注册为任务处理器。
type TaskHandlerFunc func(context.Context, TaskContext) (TaskResult, error)

func (fn TaskHandlerFunc) HandleTask(ctx context.Context, task TaskContext) (TaskResult, error) {
	return fn(ctx, task)
}

// TaskContext 是任务执行时可见的稳定上下文。
type TaskContext struct {
	Instance WorkflowInstance
	Task     TaskExecution
	Data     map[string]any
}

// TaskResult 表达任务的业务结果。Event 非空时，运行时会用它继续推进流程。
type TaskResult struct {
	Event  string
	Output map[string]any
	Outbox []OutboxMessage
}
