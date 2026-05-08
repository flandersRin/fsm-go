# 架构说明

## 目标

FSM Go 不是简单的状态枚举工具，而是一个状态治理库。

目标是把业务对象状态变化从分散的业务代码里收口出来，统一处理：

- 状态迁移规则。
- 非法流转拒绝。
- Guard 条件判断。
- 并发控制。
- 幂等。
- 状态日志。
- Outbox 一致性。

## 核心链路

```text
业务服务
  -> Runtime.Fire
  -> Machine Registry
  -> Compiled DSL
  -> Guard Engine
  -> Repository Transaction
      -> CAS 更新状态
      -> 写状态日志
      -> 写 Outbox
      -> 保存幂等结果
```

## 模块职责

| 模块 | 职责 |
|---|---|
| `fsm` | 核心状态机、DSL、Runtime、Repository 接口 |
| `actions` | 可复用 Action |
| `persistence/mysql` | MySQL Repository 实现 |
| `fsmtest` | 测试和示例使用的内存 Repository |
| `cmd/fsm-demo` | 可运行 demo 服务 |
| `test/integration` | Testcontainers 集成测试 |

## 存储边界

核心库只依赖 Repository 接口，不强制绑定 MySQL。

默认 MySQL 实现提供四张表：

- `fsm_entity`
- `fsm_state_log`
- `fsm_idempotency`
- `fsm_outbox`

默认表结构不包含 `tenant_id` 和 `sub_tenant_id`。如果业务需要租户隔离，应通过自定义 Repository 或插件实现。

## 协议取舍

第一版不引入 Protobuf 和 Buf。

当前定位是 Go library，没有强制要求 gRPC 服务、跨语言协议或强事件契约管理。未来如果要做独立服务，再单独设计协议层。
