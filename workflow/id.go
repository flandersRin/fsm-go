package workflow

import (
	"crypto/rand"
	"encoding/hex"
	"time"
)

// NewID 生成带前缀的本地唯一 ID，避免核心包依赖具体数据库自增策略。
func NewID(prefix string) string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return prefix + "-" + time.Now().UTC().Format("20060102150405.000000000")
	}
	return prefix + "-" + hex.EncodeToString(b[:])
}

func history(input HistoryInput) ExecutionHistory {
	return ExecutionHistory{
		ID: NewID("hist"), InstanceID: input.InstanceID, Type: input.Type, Message: input.Message,
		State: input.State, TaskID: input.TaskID, Task: input.Task, Event: input.Event,
		Payload: cloneMap(input.Payload), CreatedAt: input.CreatedAt,
	}
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
