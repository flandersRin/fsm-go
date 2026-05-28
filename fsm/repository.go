package fsm

import (
	"context"
	"time"
)

// StateEntity 是业务实体在状态机中的当前状态记录。
type StateEntity struct {
	Machine        string         // 状态机名称。
	MachineVersion string         // 状态机规则版本。
	EntityID       string         // 业务实体 ID。
	State          string         // 当前状态。
	Revision       int64          // 当前版本号，用于 CAS 并发控制。
	Data           map[string]any // 实体扩展数据，可供 guard 读取。
	CreatedAt      time.Time      // 创建时间，由具体 Repository 决定是否维护。
	UpdatedAt      time.Time      // 更新时间，由具体 Repository 决定是否维护。
}

// StateLog 是一次状态流转日志。
// Runtime 会在状态更新成功后写入日志，用于审计和排查。
type StateLog struct {
	Machine        string         // 状态机名称。
	MachineVersion string         // 状态机规则版本。
	EntityID       string         // 业务实体 ID。
	Event          string         // 触发事件。
	FromState      string         // 流转前状态。
	ToState        string         // 流转后状态。
	TransitionName string         // 命中的迁移规则名称。
	ActorID        string         // 操作者 ID。
	RequestID      string         // 请求 ID。
	IdempotencyKey string         // 幂等键。
	Payload        map[string]any // 本次流转载荷。
}

// OutboxMessage 是事务内写出的待投递消息。
// 业务可以通过 Action 写入 Outbox，并由独立投递器异步发送。
type OutboxMessage struct {
	ID      int64          // 消息 ID，由 Repository 生成。
	Topic   string         // 消息主题。
	Key     string         // 消息 key，通常使用 EntityID。
	Payload map[string]any // 消息载荷。
}

// IdempotencyResult 是幂等查询结果。
type IdempotencyResult struct {
	Hit    bool              // 是否命中已保存结果。
	Result *TransitionResult // 已保存的成功流转结果。
}

// Repository 定义 Runtime 需要的事务入口。
// WithTx 必须保证 fn 中所有 TxRepository 操作在同一事务边界内执行；
// fn 返回错误时应回滚，返回 nil 时应提交。内存实现可以用互斥锁模拟事务边界。
type Repository interface {
	WithTx(ctx context.Context, fn func(context.Context, TxRepository) error) error
}

// TxRepository 定义 Runtime 在事务内需要的存储操作。
// 实现方需要保证 UpdateStateCAS 的并发语义：只有当前状态和 revision 都匹配时才更新成功。
// 幂等结果、状态日志和 Outbox 应与状态更新处在同一事务里，避免状态变了但审计或消息丢失。
type TxRepository interface {
	// CreateEntity 创建状态实体。
	CreateEntity(ctx context.Context, entity StateEntity) error

	// GetEntity 读取状态实体。返回值应避免让调用方直接修改存储内部对象。
	GetEntity(ctx context.Context, machine string, entityID string) (*StateEntity, error)

	// UpdateStateCAS 按当前状态和 revision 做并发安全更新。
	// 返回 false 表示有并发流转已经抢先更新，Runtime 会返回 ErrConcurrentTransition。
	UpdateStateCAS(ctx context.Context, machine string, entityID string, fromState string, toState string, revision int64) (bool, error)

	// InsertStateLog 写入状态流转日志。
	InsertStateLog(ctx context.Context, log StateLog) error

	// TryGetIdempotency 查询已保存的幂等结果。
	TryGetIdempotency(ctx context.Context, machine string, idempotencyKey string) (*IdempotencyResult, error)

	// SaveIdempotencyResult 保存成功流转结果。
	SaveIdempotencyResult(ctx context.Context, machine string, idempotencyKey string, entityID string, event string, result TransitionResult) error

	// InsertOutbox 写入事务内 Outbox 消息。
	InsertOutbox(ctx context.Context, msg OutboxMessage) error
}
