package workflowtest

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/flandersrin/workflow-go/workflow"
)

// MemoryStore 是测试和示例使用的内存持久化实现。
// 它用互斥锁模拟事务边界，便于验证并发推进、重试和历史时间线。
type MemoryStore struct {
	mu          sync.Mutex
	instances   map[string]workflow.WorkflowInstance
	tasks       map[string]workflow.TaskExecution
	history     []workflow.ExecutionHistory
	idempotency map[string]string
	outbox      map[string]workflow.OutboxMessage
}

// NewMemoryStore 创建空内存存储。
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		instances:   map[string]workflow.WorkflowInstance{},
		tasks:       map[string]workflow.TaskExecution{},
		idempotency: map[string]string{},
		outbox:      map[string]workflow.OutboxMessage{},
	}
}

func (s *MemoryStore) CreateInstance(_ context.Context, req workflow.CreateInstanceRequest) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.instances[req.Instance.ID] = cloneInstance(req.Instance)
	s.appendHistoryLocked(req.History)
	for _, task := range req.Tasks {
		s.tasks[task.ID] = cloneTask(task)
	}
	for _, msg := range req.Outbox {
		s.outbox[msg.ID] = cloneOutbox(msg)
	}
	return nil
}

func (s *MemoryStore) GetInstance(_ context.Context, id string) (*workflow.WorkflowInstance, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	instance, ok := s.instances[id]
	if !ok {
		return nil, workflow.ErrNotFound{Resource: "workflow_instance", ID: id}
	}
	copied := cloneInstance(instance)
	return &copied, nil
}

func (s *MemoryStore) UpdateInstance(_ context.Context, req workflow.UpdateInstanceRequest) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	current, ok := s.instances[req.Instance.ID]
	if !ok {
		return workflow.ErrNotFound{Resource: "workflow_instance", ID: req.Instance.ID}
	}
	if current.Revision != req.ExpectedRevision {
		return workflow.ErrConcurrentUpdate{InstanceID: req.Instance.ID}
	}
	s.instances[req.Instance.ID] = cloneInstance(req.Instance)
	s.appendHistoryLocked(req.History)
	for _, task := range req.Tasks {
		s.tasks[task.ID] = cloneTask(task)
	}
	for _, msg := range req.Outbox {
		s.outbox[msg.ID] = cloneOutbox(msg)
	}
	return nil
}

func (s *MemoryStore) AppendHistory(_ context.Context, history []workflow.ExecutionHistory) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.appendHistoryLocked(history)
	return nil
}

func (s *MemoryStore) ListHistory(_ context.Context, instanceID string) ([]workflow.ExecutionHistory, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []workflow.ExecutionHistory
	for _, item := range s.history {
		if item.InstanceID == instanceID {
			out = append(out, cloneHistory(item))
		}
	}
	sort.SliceStable(out, func(i, j int) bool {
		return out[i].CreatedAt.Before(out[j].CreatedAt)
	})
	return out, nil
}

func (s *MemoryStore) ListDueTasks(_ context.Context, limit int, now time.Time) ([]workflow.TaskExecution, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []workflow.TaskExecution
	for _, task := range s.tasks {
		if len(out) >= limit {
			break
		}
		if (task.Status == workflow.TaskPending || task.Status == workflow.TaskRetrying) && !task.NextRunAt.After(now) {
			out = append(out, cloneTask(task))
		}
	}
	sort.SliceStable(out, func(i, j int) bool {
		return out[i].NextRunAt.Before(out[j].NextRunAt)
	})
	return out, nil
}

func (s *MemoryStore) MarkTaskRunning(_ context.Context, id string, attempt int) (*workflow.TaskExecution, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	task, ok := s.tasks[id]
	if !ok {
		return nil, workflow.ErrNotFound{Resource: "task_execution", ID: id}
	}
	if task.Attempt != attempt || (task.Status != workflow.TaskPending && task.Status != workflow.TaskRetrying) {
		return nil, workflow.ErrConcurrentUpdate{InstanceID: task.InstanceID}
	}
	task.Status = workflow.TaskRunning
	task.UpdatedAt = time.Now().UTC()
	s.tasks[id] = cloneTask(task)
	copied := cloneTask(task)
	return &copied, nil
}

func (s *MemoryStore) CompleteTask(_ context.Context, req workflow.CompleteTaskRequest) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tasks[req.Task.ID] = cloneTask(req.Task)
	s.appendHistoryLocked(req.History)
	for _, next := range req.Tasks {
		s.tasks[next.ID] = cloneTask(next)
	}
	for _, msg := range req.Outbox {
		s.outbox[msg.ID] = cloneOutbox(msg)
	}
	return nil
}

func (s *MemoryStore) RecordIdempotency(_ context.Context, scope string, key string, resultID string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	full := scope + ":" + key
	existing, ok := s.idempotency[full]
	if ok {
		return existing == resultID, nil
	}
	s.idempotency[full] = resultID
	return true, nil
}

func (s *MemoryStore) GetIdempotency(_ context.Context, scope string, key string) (string, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	id, ok := s.idempotency[scope+":"+key]
	return id, ok, nil
}

func (s *MemoryStore) AppendOutbox(_ context.Context, outbox []workflow.OutboxMessage) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, msg := range outbox {
		s.outbox[msg.ID] = cloneOutbox(msg)
	}
	return nil
}

func (s *MemoryStore) ListOutbox(_ context.Context, limit int) ([]workflow.OutboxMessage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []workflow.OutboxMessage
	for _, msg := range s.outbox {
		if limit > 0 && len(out) >= limit {
			break
		}
		if msg.Status == "" || msg.Status == "pending" {
			out = append(out, cloneOutbox(msg))
		}
	}
	return out, nil
}

func (s *MemoryStore) MarkOutboxPublished(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	msg, ok := s.outbox[id]
	if !ok {
		return workflow.ErrNotFound{Resource: "outbox", ID: id}
	}
	msg.Status = "published"
	msg.UpdatedAt = time.Now().UTC()
	s.outbox[id] = msg
	return nil
}

func (s *MemoryStore) appendHistoryLocked(items []workflow.ExecutionHistory) {
	for _, item := range items {
		s.history = append(s.history, cloneHistory(item))
	}
}

func cloneInstance(in workflow.WorkflowInstance) workflow.WorkflowInstance {
	in.Data = cloneMap(in.Data)
	return in
}

func cloneTask(in workflow.TaskExecution) workflow.TaskExecution {
	in.Input = cloneMap(in.Input)
	in.Output = cloneMap(in.Output)
	return in
}

func cloneHistory(in workflow.ExecutionHistory) workflow.ExecutionHistory {
	in.Payload = cloneMap(in.Payload)
	return in
}

func cloneOutbox(in workflow.OutboxMessage) workflow.OutboxMessage {
	in.Payload = cloneMap(in.Payload)
	return in
}

func cloneMap(in map[string]any) map[string]any {
	if in == nil {
		return map[string]any{}
	}
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
