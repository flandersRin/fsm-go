# 业务接入说明

业务接入只走三步，不需要手写复杂的 `WorkflowDefinition`。

## 第一步：写一份简单流程文件

在业务目录放一个 `workflow.yaml`：

```yaml
package: order
workflow: order
version: v1
initial: PENDING

states:
  - name: PENDING
    on_enter:
      - charge_payment
  - name: PAID
    terminal: true

events:
  - name: PAYMENT_OK

tasks:
  - name: charge_payment
    handler: payment.charge
    max_attempts: 3
    backoff: 1s
    on_success: PAYMENT_OK

transitions:
  - name: payment_ok
    from: PENDING
    event: PAYMENT_OK
    to: PAID
```

这份文件只描述业务流程，不写 Go 代码。

## 第二步：生成强类型代码

在同目录加一行生成命令：

```go
package order

//go:generate go run github.com/flandersrin/workflow-go/cmd/workflowgen --in workflow.yaml --out workflow_gen.go
```

执行：

```bash
go generate ./...
```

生成结果会包含：

- 状态常量，例如 `StatePending`。
- 事件常量，例如 `EventPaymentOk`。
- 任务常量，例如 `TaskChargePayment`。
- 处理器常量，例如 `HandlerPaymentCharge`。
- `Register(runtime)`。
- `Start(ctx, runtime, instanceID, data)`。
- `Signal(ctx, runtime, instanceID, event, payload)`。

业务代码不需要自己拼流程名、版本、状态、事件这些字符串。

## 第三步：注册任务并启动流程

```go
store := workflowtest.NewMemoryStore()
runtime := workflow.NewRuntime(store)

Register(runtime)

RegisterTask(runtime, HandlerPaymentCharge, workflow.TaskHandlerFunc(func(ctx context.Context, task workflow.TaskContext) (workflow.TaskResult, error) {
    return workflow.TaskResult{}, nil
}))

instance, err := Start(ctx, runtime, "order-10001", map[string]any{
    "amount": 100,
})
```

如果任务要由业务自己决定下一步事件，可以返回：

```go
return workflow.TaskResult{Event: string(EventPaymentOk)}, nil
```

如果任务定义里已经写了 `on_success`，普通成功只需要返回空结果。

## 校验发生在哪里

生成代码仍然会调用 `workflow.Compile`。

它会检查：

- 初始状态是否存在。
- 状态是否重复。
- 事件是否重复。
- 任务是否重复。
- 状态入口任务是否存在。
- 补偿任务是否存在。
- 任务成功事件是否存在。
- 任务失败事件是否存在。
- 转移的来源状态、目标状态、事件是否存在。
- 同一个状态和事件是否定义了重复转移。

所以业务方即使改错 YAML，也会在启动或测试阶段直接失败。

## 推荐目录

```text
internal/orderflow/
  workflow.yaml
  generate.go
  workflow_gen.go
  handlers.go
```

`workflow.yaml` 负责描述流程。

`workflow_gen.go` 由工具生成，不手改。

`handlers.go` 写业务任务。

业务服务启动时只负责创建 runtime、注册流程、注册任务。
