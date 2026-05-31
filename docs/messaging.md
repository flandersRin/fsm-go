# 消息

核心包只定义消息接口，不绑定具体消息系统。

## 发布接口

`MessagePublisher` 负责发布消息。

## 消费接口

`MessageConsumer` 负责消费消息。消费方必须使用消息 ID 做幂等。

## Kafka

`messaging/kafka` 是默认 Kafka 实现。

## Outbox

Outbox 用于把本地流程推进和对外消息发送衔接起来。流程运行时先在本地事务里写 Outbox，后台发布器再把消息发到 Kafka。
