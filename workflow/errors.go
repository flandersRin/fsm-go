package workflow

import "fmt"

// ErrNotFound 表示请求的流程、实例或任务不存在。
type ErrNotFound struct {
	Resource string
	ID       string
}

func (e ErrNotFound) Error() string {
	return fmt.Sprintf("%s not found: %s", e.Resource, e.ID)
}

// ErrInvalidDefinition 表示流程定义无法编译成可运行模型。
type ErrInvalidDefinition struct {
	Message string
}

func (e ErrInvalidDefinition) Error() string {
	return "invalid workflow definition: " + e.Message
}

// ErrInvalidTransition 表示当前状态不接受该事件。
type ErrInvalidTransition struct {
	State string
	Event string
}

func (e ErrInvalidTransition) Error() string {
	return fmt.Sprintf("invalid transition from %s by %s", e.State, e.Event)
}

// ErrConcurrentUpdate 表示同一实例被其他调度器抢先推进。
type ErrConcurrentUpdate struct {
	InstanceID string
}

func (e ErrConcurrentUpdate) Error() string {
	return "concurrent workflow update: " + e.InstanceID
}

// ErrIdempotentConflict 表示同一个幂等键被用于不同语义的请求。
type ErrIdempotentConflict struct {
	Key string
}

func (e ErrIdempotentConflict) Error() string {
	return "idempotent conflict: " + e.Key
}
