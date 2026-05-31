# GSM

## Goal

workflow-go 的目标不是实现一个有限状态机库，而是用状态机作为核心抽象，解决复杂业务流程编排问题。

真实业务流程会遇到机器重启、网络失败、任务超时、重复请求、消息投递失败、人工介入和补偿执行。只提供状态切换 API，无法覆盖这些问题。

## Strategy

workflow-go 选择把“状态”和“执行”分开。

状态只描述流程位置，例如 `PENDING`、`PAYING`、`PAID`。任务负责真正执行工作，例如扣款、发消息、调用工具、生成报表。

运行时负责统一处理：

- 执行历史。
- 任务调度。
- 重试。
- 超时。
- 补偿。
- 恢复。
- 幂等。
- 消息衔接。

## Mechanism

核心机制由几类对象组成：

- `WorkflowDefinition`：流程定义。
- `WorkflowInstance`：流程实例快照。
- `TaskDefinition`：任务定义。
- `TaskExecution`：任务执行记录。
- `ExecutionHistory`：完整时间线。
- `Store`：持久化接口。
- `MessagePublisher` / `MessageConsumer`：消息接口。

MySQL、PostgreSQL、Kafka 是默认实现，但不是核心绑定。
