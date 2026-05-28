package fsm

import (
	"context"
	"reflect"
	"strings"
	"time"
)

// Observer 接收状态流转观测事件。
// TransitionStarted 会在流转开始前触发；TransitionCompleted 会在流转结束后触发，
// 无论成功、失败还是命中幂等都会触发完成事件。
type Observer interface {
	TransitionStarted(context.Context, TransitionStarted)
	TransitionCompleted(context.Context, TransitionCompleted)
}

// TransitionStarted 表示一次流转已经开始。
type TransitionStarted struct {
	Command FireCommand // 本次流转命令。
}

// TransitionCompleted 表示一次流转已经结束。
type TransitionCompleted struct {
	Command   FireCommand       // 本次流转命令。
	Result    *TransitionResult // 成功或幂等命中时的结果；失败时通常为空。
	Err       error             // 流转错误；成功时为空。
	Duration  time.Duration     // 从 Runtime 接收命令到流转结束的耗时。
	ErrorType string            // 错误类型短名；成功时为空。
	Status    string            // success、error 或 idempotent_hit。
}

// RuntimeOption 用于配置 Runtime。
type RuntimeOption func(*Runtime)

// WithObserver 为 Runtime 添加一个观测器。
// 可以多次调用注册多个 Observer，Runtime 会按注册顺序通知。
func WithObserver(observer Observer) RuntimeOption {
	return func(runtime *Runtime) {
		if observer != nil {
			runtime.observers = append(runtime.observers, observer)
		}
	}
}

func (r *Runtime) observeTransitionStarted(ctx context.Context, cmd FireCommand) {
	event := TransitionStarted{Command: cmd}
	for _, observer := range r.observers {
		observer.TransitionStarted(ctx, event)
	}
}

func (r *Runtime) observeTransitionCompleted(ctx context.Context, cmd FireCommand, result *TransitionResult, err error, duration time.Duration) {
	event := TransitionCompleted{
		Command:   cmd,
		Result:    result,
		Err:       err,
		Duration:  duration,
		ErrorType: errorType(err),
		Status:    transitionStatus(result, err),
	}
	for _, observer := range r.observers {
		observer.TransitionCompleted(ctx, event)
	}
}

func transitionStatus(result *TransitionResult, err error) string {
	if err != nil {
		return "error"
	}
	if result != nil && result.IdempotentHit {
		return "idempotent_hit"
	}
	return "success"
}

func errorType(err error) string {
	if err == nil {
		return ""
	}
	t := reflect.TypeOf(err)
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	name := t.String()
	if idx := strings.LastIndex(name, "."); idx >= 0 {
		return name[idx+1:]
	}
	return name
}
