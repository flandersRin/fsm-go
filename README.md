# workflow-go

workflow-go 是一个面向复杂业务流程编排的 Go 工作流运行时。

它不是传统 FSM 库。传统 FSM 主要解决“某个状态能不能切到另一个状态”，workflow-go 解决的是“一个长期运行的业务流程如何执行、失败、重试、补偿、恢复和被观察”。

## GSM 定位

### Goal

用状态机作为核心抽象，解决复杂业务流程编排问题。

### Strategy

- 状态只表达流程位置。
- 任务负责真正执行业务动作。
- 执行历史成为一等数据。
- 持久化和消息都通过接口接入。
- 重试、超时、补偿、恢复由运行时统一处理。

### Mechanism

- `WorkflowDefinition` 描述流程。
- `WorkflowInstance` 保存当前快照。
- `TaskExecution` 记录可恢复任务。
- `ExecutionHistory` 提供完整时间线。
- `Store` 抽象持久化。
- `MessagePublisher` / `MessageConsumer` 抽象消息系统。
- 默认提供 MySQL、PostgreSQL、Kafka 实现。

## 适合场景

- 订单流程。
- 审批流程。
- Saga 编排。
- 异步任务恢复。
- Kafka 消费治理。
- AI Agent Workflow。

## 快速运行

```bash
go test ./...
go run ./examples/order
go run ./examples/async_task
go run ./examples/saga
go run ./examples/agent_workflow
```

生成业务数据：

```bash
go run ./scripts/generate-business-data --kind order --count 10000 --out order.jsonl
```

运行内存性能测试：

```bash
go run ./scripts/perf --n 20000
```

## 核心入口

- `StartWorkflow`：启动流程。
- `SignalWorkflow`：发送事件。
- `RunDueTasks`：执行到期、重试、超时任务。
- `GetWorkflow`：读取当前状态。
- `ListHistory`：读取执行时间线。

## 文档

- [GSM](docs/gsm.md)
- [架构说明](docs/architecture.md)
- [执行模型](docs/execution-model.md)
- [持久化](docs/persistence.md)
- [消息](docs/messaging.md)
- [恢复机制](docs/recovery.md)
- [可观测性](docs/observability.md)
- [性能测试](docs/performance.md)
