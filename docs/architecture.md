# 架构说明

workflow-go 分为四层。

## 核心运行时

`workflow` 包只包含流程模型、编译、运行时、任务接口、存储接口和消息接口。

核心包不依赖 MySQL、PostgreSQL、Kafka，也不依赖任何业务系统。

## 持久化实现

默认提供：

- `persistence/mysql`
- `persistence/postgres`
- `workflowtest`

MySQL 和 PostgreSQL 用于生产接入。`workflowtest` 只用于测试和示例。

## 消息实现

默认提供：

- `messaging/kafka`

Kafka 通过通用消息接口接入。用户可以按同样接口接入 RabbitMQ、NATS、Redis Stream 或云消息队列。

## 示例和脚本

示例覆盖订单、异步任务、Saga 和 Agent Workflow。

脚本覆盖业务数据生成和性能测试。
