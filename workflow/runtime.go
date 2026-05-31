package workflow

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// Runtime 是 workflow-go 的统一入口，负责状态推进、任务调度、历史记录和恢复执行。
type Runtime struct {
	store    Store
	clock    Clock
	mu       sync.RWMutex
	machines map[string]*Machine
	handlers map[string]TaskHandler
}

// Clock 让测试可以稳定控制时间，生产默认使用系统时间。
type Clock interface {
	Now() time.Time
}

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now().UTC() }

// NewRuntime 创建运行时。store 是唯一必需依赖，数据库和消息队列都通过接口进入。
func NewRuntime(store Store, opts ...RuntimeOption) *Runtime {
	r := &Runtime{
		store:    store,
		clock:    systemClock{},
		machines: map[string]*Machine{},
		handlers: map[string]TaskHandler{},
	}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

// RuntimeOption 修改运行时配置。
type RuntimeOption func(*Runtime)

// WithClock 替换运行时时钟，主要用于测试重试和超时。
func WithClock(clock Clock) RuntimeOption {
	return func(r *Runtime) {
		if clock != nil {
			r.clock = clock
		}
	}
}

// RegisterMachine 注册编译后的流程定义。
func (r *Runtime) RegisterMachine(machine *Machine) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.machines[machineKey(machine.Name, machine.Version)] = machine
}

// RegisterTask 注册任务处理器。任务定义中的 handler 必须能在这里找到。
func (r *Runtime) RegisterTask(name string, handler TaskHandler) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.handlers[name] = handler
}

// StartWorkflow 创建流程实例并调度初始状态的入口任务。
func (r *Runtime) StartWorkflow(ctx context.Context, opts StartOptions) (*WorkflowInstance, error) {
	machine, err := r.machine(opts.Workflow, opts.Version)
	if err != nil {
		return nil, err
	}
	if opts.InstanceID == "" {
		opts.InstanceID = NewID("wf")
	}
	if opts.IdempotencyKey != "" {
		if id, ok, err := r.store.GetIdempotency(ctx, opts.Workflow, opts.IdempotencyKey); err != nil {
			return nil, err
		} else if ok {
			return r.store.GetInstance(ctx, id)
		}
	}
	now := r.clock.Now()
	instance := WorkflowInstance{
		ID: opts.InstanceID, Workflow: opts.Workflow, Version: opts.Version,
		State: machine.Initial, Status: InstanceRunning, Data: cloneMap(opts.Data),
		StartedAt: now, UpdatedAt: now,
	}
	history := []ExecutionHistory{history(HistoryInput{InstanceID: instance.ID, Type: HistoryWorkflowStarted, Message: "流程已启动", State: machine.Initial, Payload: cloneMap(opts.Data), CreatedAt: now})}
	tasks, h := r.scheduleStateTasks(machine, instance, now)
	history = append(history, h...)
	if opts.IdempotencyKey != "" {
		ok, err := r.store.RecordIdempotency(ctx, opts.Workflow, opts.IdempotencyKey, instance.ID)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, ErrIdempotentConflict{Key: opts.IdempotencyKey}
		}
	}
	if err := r.store.CreateInstance(ctx, CreateInstanceRequest{Instance: instance, History: history, Tasks: tasks}); err != nil {
		return nil, err
	}
	return &instance, nil
}

// SignalWorkflow 向流程发送事件并尝试推进状态。
func (r *Runtime) SignalWorkflow(ctx context.Context, opts SignalOptions) (*WorkflowInstance, error) {
	instance, err := r.store.GetInstance(ctx, opts.InstanceID)
	if err != nil {
		return nil, err
	}
	if opts.IdempotencyKey != "" {
		if id, ok, err := r.store.GetIdempotency(ctx, instance.Workflow, opts.IdempotencyKey); err != nil {
			return nil, err
		} else if ok {
			if id != opts.InstanceID {
				return nil, ErrIdempotentConflict{Key: opts.IdempotencyKey}
			}
			return instance, nil
		}
	}
	machine, err := r.machine(instance.Workflow, instance.Version)
	if err != nil {
		return nil, err
	}
	next, histories, tasks, err := r.applyEvent(machine, *instance, opts.Event, opts.Payload)
	if err != nil {
		return nil, err
	}
	if opts.IdempotencyKey != "" {
		ok, err := r.store.RecordIdempotency(ctx, instance.Workflow, opts.IdempotencyKey, instance.ID)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, ErrIdempotentConflict{Key: opts.IdempotencyKey}
		}
	}
	if err := r.store.UpdateInstance(ctx, UpdateInstanceRequest{Instance: next, ExpectedRevision: instance.Revision, History: histories, Tasks: tasks}); err != nil {
		return nil, err
	}
	return &next, nil
}

// RunDueTasks 执行到期任务。它可以被 HTTP、cron、worker 或测试脚本反复调用。
func (r *Runtime) RunDueTasks(ctx context.Context, opts RunOptions) (RunReport, error) {
	if opts.Limit <= 0 {
		opts.Limit = 100
	}
	now := opts.Now
	if now.IsZero() {
		now = r.clock.Now()
	}
	due, err := r.store.ListDueTasks(ctx, opts.Limit, now)
	if err != nil {
		return RunReport{}, err
	}
	report := RunReport{Scanned: len(due)}
	for _, task := range due {
		locked, err := r.store.MarkTaskRunning(ctx, task.ID, task.Attempt)
		if err != nil {
			if errors.As(err, &ErrConcurrentUpdate{}) {
				continue
			}
			return report, err
		}
		kind, err := r.runTask(ctx, *locked, now)
		if err != nil {
			return report, err
		}
		switch kind {
		case TaskSucceeded:
			report.Succeeded++
		case TaskRetrying:
			report.Retried++
		case TaskFailed:
			report.Failed++
		case TaskTimedOut:
			report.TimedOut++
		}
	}
	return report, nil
}

// GetWorkflow 读取实例当前快照。
func (r *Runtime) GetWorkflow(ctx context.Context, instanceID string) (*WorkflowInstance, error) {
	return r.store.GetInstance(ctx, instanceID)
}

// ListHistory 读取实例完整时间线。
func (r *Runtime) ListHistory(ctx context.Context, instanceID string) ([]ExecutionHistory, error) {
	return r.store.ListHistory(ctx, instanceID)
}

func (r *Runtime) runTask(ctx context.Context, task TaskExecution, now time.Time) (TaskStatus, error) {
	instance, err := r.store.GetInstance(ctx, task.InstanceID)
	if err != nil {
		return "", err
	}
	machine, err := r.machine(instance.Workflow, instance.Version)
	if err != nil {
		return "", err
	}
	def, ok := machine.Tasks[task.Task]
	if !ok {
		return "", ErrNotFound{Resource: "task", ID: task.Task}
	}
	handler, err := r.handler(def.Handler)
	if err != nil {
		return "", err
	}
	start := history(HistoryInput{InstanceID: instance.ID, Type: HistoryTaskStarted, Message: "任务开始执行", State: instance.State, TaskID: task.ID, Task: task.Task, CreatedAt: now})
	runCtx := ctx
	cancel := func() {}
	if !task.TimeoutAt.IsZero() {
		timeout := time.Until(task.TimeoutAt)
		if timeout <= 0 {
			return r.finishTask(ctx, task, *instance, def, TaskTimedOut, nil, fmt.Errorf("task timed out"), []ExecutionHistory{start}, now)
		}
		runCtx, cancel = context.WithTimeout(ctx, timeout)
	}
	defer cancel()
	result, handleErr := handler.HandleTask(runCtx, TaskContext{Instance: *instance, Task: task, Data: cloneMap(instance.Data)})
	status := TaskSucceeded
	if handleErr != nil {
		status = TaskFailed
	}
	return r.finishTask(ctx, task, *instance, def, status, &result, handleErr, []ExecutionHistory{start}, now)
}

func (r *Runtime) finishTask(ctx context.Context, task TaskExecution, instance WorkflowInstance, def TaskDefinition, status TaskStatus, result *TaskResult, taskErr error, histories []ExecutionHistory, now time.Time) (TaskStatus, error) {
	task.UpdatedAt = now
	task.Output = nil
	outbox := []OutboxMessage{}
	nextTasks := []TaskExecution{}
	if result != nil {
		task.Output = cloneMap(result.Output)
		outbox = append(outbox, result.Outbox...)
	}
	if status == TaskSucceeded {
		task.Status = TaskSucceeded
		histories = append(histories, history(HistoryInput{InstanceID: instance.ID, Type: HistoryTaskSucceeded, Message: "任务执行成功", State: instance.State, TaskID: task.ID, Task: task.Task, Payload: task.Output, CreatedAt: now}))
		successEvent := def.OnSuccess
		if result != nil && result.Event != "" {
			successEvent = result.Event
		}
		if successEvent != "" {
			machine, err := r.machine(instance.Workflow, instance.Version)
			if err != nil {
				return "", err
			}
			payload := map[string]any{}
			if result != nil {
				payload = result.Output
			}
			next, h, scheduled, err := r.applyEvent(machine, instance, successEvent, payload)
			if err != nil {
				return "", err
			}
			histories = append(histories, h...)
			nextTasks = append(nextTasks, scheduled...)
			if err := r.store.UpdateInstance(ctx, UpdateInstanceRequest{Instance: next, ExpectedRevision: instance.Revision, History: histories, Tasks: nextTasks, Outbox: outbox}); err != nil {
				return "", err
			}
			return TaskSucceeded, r.store.CompleteTask(ctx, CompleteTaskRequest{Task: task})
		}
		return TaskSucceeded, r.store.CompleteTask(ctx, CompleteTaskRequest{Task: task, History: histories, Tasks: nextTasks, Outbox: outbox})
	}
	if status == TaskTimedOut {
		task.Status = TaskTimedOut
		task.LastError = "task timed out"
		histories = append(histories, history(HistoryInput{InstanceID: instance.ID, Type: HistoryTaskTimedOut, Message: "任务执行超时", State: instance.State, TaskID: task.ID, Task: task.Task, CreatedAt: now}))
		return TaskTimedOut, r.store.CompleteTask(ctx, CompleteTaskRequest{Task: task, History: histories})
	}
	if taskErr != nil {
		task.LastError = taskErr.Error()
	}
	if task.Attempt < task.MaxAttempts {
		task.Status = TaskRetrying
		task.Attempt++
		task.NextRunAt = now.Add(def.Retry.Backoff)
		histories = append(histories, history(HistoryInput{InstanceID: instance.ID, Type: HistoryTaskRetrying, Message: "任务失败，等待重试", State: instance.State, TaskID: task.ID, Task: task.Task, Payload: map[string]any{"error": task.LastError}, CreatedAt: now}))
		return TaskRetrying, r.store.CompleteTask(ctx, CompleteTaskRequest{Task: task, History: histories})
	}
	task.Status = TaskFailed
	histories = append(histories, history(HistoryInput{InstanceID: instance.ID, Type: HistoryTaskFailed, Message: "任务最终失败", State: instance.State, TaskID: task.ID, Task: task.Task, Payload: map[string]any{"error": task.LastError}, CreatedAt: now}))
	if def.Compensation != "" {
		comp, scheduledHistory := r.scheduleTask(instance, def.Compensation, now)
		nextTasks = append(nextTasks, comp)
		histories = append(histories, history(HistoryInput{InstanceID: instance.ID, Type: HistoryCompensation, Message: "已调度补偿任务", State: instance.State, TaskID: comp.ID, Task: comp.Task, CreatedAt: now}))
		histories = append(histories, scheduledHistory)
	}
	if def.OnFailure != "" {
		machine, err := r.machine(instance.Workflow, instance.Version)
		if err != nil {
			return "", err
		}
		next, h, scheduled, err := r.applyEvent(machine, instance, def.OnFailure, map[string]any{"error": task.LastError})
		if err != nil {
			return "", err
		}
		histories = append(histories, h...)
		nextTasks = append(nextTasks, scheduled...)
		if err := r.store.UpdateInstance(ctx, UpdateInstanceRequest{Instance: next, ExpectedRevision: instance.Revision, History: histories, Tasks: nextTasks, Outbox: outbox}); err != nil {
			return "", err
		}
		return TaskFailed, r.store.CompleteTask(ctx, CompleteTaskRequest{Task: task})
	}
	return TaskFailed, r.store.CompleteTask(ctx, CompleteTaskRequest{Task: task, History: histories, Tasks: nextTasks, Outbox: outbox})
}

func (r *Runtime) applyEvent(machine *Machine, instance WorkflowInstance, event string, payload map[string]any) (WorkflowInstance, []ExecutionHistory, []TaskExecution, error) {
	now := r.clock.Now()
	histories := []ExecutionHistory{
		history(HistoryInput{InstanceID: instance.ID, Type: HistorySignalReceived, Message: "收到流程事件", State: instance.State, Event: event, Payload: cloneMap(payload), CreatedAt: now}),
	}
	transition, ok := machine.Transitions[transitionKey(instance.State, event)]
	if !ok {
		return WorkflowInstance{}, nil, nil, ErrInvalidTransition{State: instance.State, Event: event}
	}
	from := instance.State
	instance.State = transition.To
	instance.Revision++
	instance.UpdatedAt = now
	if machine.States[transition.To].Terminal {
		instance.Status = InstanceCompleted
		instance.FinishedAt = now
	}
	histories = append(histories, history(HistoryInput{InstanceID: instance.ID, Type: HistoryStateChanged, Message: "流程状态已推进", State: instance.State, Event: event, Payload: map[string]any{"from": from, "to": transition.To}, CreatedAt: now}))
	if instance.Status == InstanceCompleted {
		histories = append(histories, history(HistoryInput{InstanceID: instance.ID, Type: HistoryWorkflowCompleted, Message: "流程已完成", State: instance.State, CreatedAt: now}))
		return instance, histories, nil, nil
	}
	tasks, h := r.scheduleStateTasks(machine, instance, now)
	histories = append(histories, h...)
	return instance, histories, tasks, nil
}

func (r *Runtime) scheduleStateTasks(machine *Machine, instance WorkflowInstance, now time.Time) ([]TaskExecution, []ExecutionHistory) {
	state := machine.States[instance.State]
	var tasks []TaskExecution
	var histories []ExecutionHistory
	for _, taskName := range state.OnEnter {
		task, h := r.scheduleTask(instance, taskName, now)
		tasks = append(tasks, task)
		histories = append(histories, h)
	}
	return tasks, histories
}

func (r *Runtime) scheduleTask(instance WorkflowInstance, taskName string, now time.Time) (TaskExecution, ExecutionHistory) {
	machine, _ := r.machine(instance.Workflow, instance.Version)
	def := machine.Tasks[taskName]
	timeoutAt := time.Time{}
	if def.Timeout > 0 {
		timeoutAt = now.Add(def.Timeout)
	}
	task := TaskExecution{
		ID: NewID("task"), InstanceID: instance.ID, Task: def.Name, Handler: def.Handler,
		Status: TaskPending, Attempt: 1, MaxAttempts: def.Retry.MaxAttempts,
		NextRunAt: now, TimeoutAt: timeoutAt, CreatedAt: now, UpdatedAt: now,
	}
	return task, history(HistoryInput{InstanceID: instance.ID, Type: HistoryTaskScheduled, Message: "任务已调度", State: instance.State, TaskID: task.ID, Task: task.Task, CreatedAt: now})
}

func (r *Runtime) machine(name string, version string) (*Machine, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	machine, ok := r.machines[machineKey(name, version)]
	if !ok {
		return nil, ErrNotFound{Resource: "workflow", ID: machineKey(name, version)}
	}
	return machine, nil
}

func (r *Runtime) handler(name string) (TaskHandler, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	handler, ok := r.handlers[name]
	if !ok {
		return nil, ErrNotFound{Resource: "handler", ID: name}
	}
	return handler, nil
}

func machineKey(name string, version string) string {
	return name + ":" + version
}
