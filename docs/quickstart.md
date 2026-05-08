# 快速开始

本文档说明如何在本地运行 FSM Go，并完成一次订单状态流转。

## 1. 运行测试

```bash
go test ./...
go test -race ./...
```

如果本机已经安装 Docker，可以运行集成测试：

```bash
go test -tags=integration ./test/integration/...
```

集成测试会通过 Testcontainers 自动启动真实 MySQL，测试结束后自动清理。

## 2. 运行 Go 示例

```bash
go run ./examples/order
```

预期输出类似：

```text
PENDING -> PAID
logs=1 outbox=1
```

这个示例使用内存 Repository，适合快速理解库的接入方式。

## 3. 启动 Docker Demo

```bash
docker compose up -d --build
```

健康检查：

```bash
curl http://127.0.0.1:8080/healthz
```

初始化订单：

```bash
curl -X POST http://127.0.0.1:8080/demo/order/init \
  -H 'Content-Type: application/json' \
  -d '{"entity_id":"order-10001","data":{}}'
```

触发支付成功：

```bash
curl -X POST http://127.0.0.1:8080/demo/order/fire \
  -H 'Content-Type: application/json' \
  -d '{
    "entity_id":"order-10001",
    "event":"PAY_SUCCESS",
    "actor_id":"user-1",
    "actor_role":"customer",
    "request_id":"req-1",
    "idempotency_key":"pay-10001",
    "payload":{"paymentStatus":"SUCCESS","amount":100}
  }'
```

查看结果：

```bash
curl http://127.0.0.1:8080/demo/order/order-10001
curl http://127.0.0.1:8080/demo/order/order-10001/logs
curl http://127.0.0.1:8080/demo/outbox
```

清理环境：

```bash
docker compose down -v
```
