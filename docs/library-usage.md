# 库接入示例

FSM Go 的核心定位是 Go library。业务系统直接在自己的服务里加载 DSL、注册状态机、接入 Repository，然后通过统一入口触发状态流转。

## 1. 定义 DSL

```yaml
machine: order
version: v1
initial: PENDING

states:
  - name: PENDING
  - name: PAID
  - name: COMPLETED
    terminal: true

events:
  - name: PAY_SUCCESS
  - name: FINISH

transitions:
  - name: pay_success
    from: PENDING
    event: PAY_SUCCESS
    to: PAID
    priority: 10
    guard: "payload.paymentStatus == 'SUCCESS' && payload.amount > 0"
    idempotent: true
    actions:
      in_tx:
        - outbox.order_paid
```

## 2. 加载并注册状态机

```go
spec, err := fsm.LoadYAML("configs/order.v1.yaml")
if err != nil {
    return err
}

machine, err := fsm.Compile(spec)
if err != nil {
    return err
}

runtime := fsm.NewRuntime(repository, actionRegistry)
runtime.RegisterMachine(machine)
```

## 3. 初始化业务对象状态

```go
err := runtime.CreateEntity(ctx, fsm.StateEntity{
    Machine:        "order",
    MachineVersion: "v1",
    EntityID:       "order-10001",
    State:          "PENDING",
    Data:           map[string]any{},
})
```

## 4. 触发状态流转

```go
result, err := runtime.Fire(ctx, fsm.FireCommand{
    Machine:        "order",
    MachineVersion: "v1",
    EntityID:       "order-10001",
    Event:          "PAY_SUCCESS",
    Actor:          fsm.Actor{ID: "user-1", Role: "customer"},
    RequestID:      "req-1",
    IdempotencyKey: "pay-10001",
    Payload: map[string]any{
        "paymentStatus": "SUCCESS",
        "amount":        100,
    },
})
```

如果成功，状态会从 `PENDING` 变成 `PAID`，同时写入状态日志、幂等结果和 Outbox 消息。

## 5. 接入 MySQL

```go
db, err := sql.Open("mysql", dsn)
if err != nil {
    return err
}

repository := mysqlrepo.NewRepository(db)
if err := repository.InitSchema(ctx); err != nil {
    return err
}
```

生产环境通常不建议应用启动时自动建表，可以把 `persistence/mysql.Schema` 里的 SQL 交给迁移工具执行。

## 6. 注册 Action

```go
registry := fsm.NewActionRegistry()

actions.RegisterOutbox(registry, map[string]string{
    "outbox.order_paid": "order.paid",
})
```

业务也可以注册自己的 Action，例如写审计、发通知、写业务扩展表。
