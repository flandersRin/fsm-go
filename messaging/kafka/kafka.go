package kafka

import (
	"context"
	"encoding/json"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/flandersrin/workflow-go/workflow"
)

// Publisher 是基于 segmentio/kafka-go 的默认消息发布器。
type Publisher struct {
	writer *kafka.Writer
}

// PublisherConfig 描述 Kafka 发布器配置。
type PublisherConfig struct {
	Brokers []string
	Topic   string
}

// NewPublisher 创建 Kafka 发布器。
func NewPublisher(config PublisherConfig) *Publisher {
	return &Publisher{writer: &kafka.Writer{Addr: kafka.TCP(config.Brokers...), Topic: config.Topic, RequiredAcks: kafka.RequireOne}}
}

// Publish 发布一条通用 workflow 消息。
func (p *Publisher) Publish(ctx context.Context, msg workflow.Message) error {
	headers := make([]kafka.Header, 0, len(msg.Headers))
	for key, value := range msg.Headers {
		headers = append(headers, kafka.Header{Key: key, Value: []byte(value)})
	}
	return p.writer.WriteMessages(ctx, kafka.Message{
		Key:     []byte(msg.Key),
		Value:   msg.Payload,
		Headers: headers,
		Time:    time.Now().UTC(),
	})
}

// Close 关闭底层 Kafka writer。
func (p *Publisher) Close() error {
	return p.writer.Close()
}

// Consumer 是基于 segmentio/kafka-go 的默认消息消费器。
type Consumer struct {
	reader *kafka.Reader
}

// ConsumerConfig 描述 Kafka 消费器配置。
type ConsumerConfig struct {
	Brokers []string
	Topic   string
	GroupID string
}

// NewConsumer 创建 Kafka 消费器。
func NewConsumer(config ConsumerConfig) *Consumer {
	return &Consumer{reader: kafka.NewReader(kafka.ReaderConfig{Brokers: config.Brokers, Topic: config.Topic, GroupID: config.GroupID})}
}

// Consume 持续消费消息。handler 返回错误时不会提交当前消息，便于 Kafka 后续重投。
func (c *Consumer) Consume(ctx context.Context, handler func(context.Context, workflow.Message) error) error {
	for {
		raw, err := c.reader.FetchMessage(ctx)
		if err != nil {
			return err
		}
		headers := map[string]string{}
		for _, header := range raw.Headers {
			headers[header.Key] = string(header.Value)
		}
		msg := workflow.Message{ID: string(raw.Key), Topic: raw.Topic, Key: string(raw.Key), Payload: raw.Value, Headers: headers}
		if err := handler(ctx, msg); err != nil {
			return err
		}
		if err := c.reader.CommitMessages(ctx, raw); err != nil {
			return err
		}
	}
}

// Close 关闭底层 Kafka reader。
func (c *Consumer) Close() error {
	return c.reader.Close()
}

// OutboxPublisher 把 workflow Outbox 消息发布到 Kafka。
type OutboxPublisher struct {
	publisher workflow.MessagePublisher
}

// NewOutboxPublisher 创建 Outbox 发布器。
func NewOutboxPublisher(publisher workflow.MessagePublisher) *OutboxPublisher {
	return &OutboxPublisher{publisher: publisher}
}

// PublishOutbox 发布一条 Outbox 消息。
func (p *OutboxPublisher) PublishOutbox(ctx context.Context, msg workflow.OutboxMessage) error {
	payload, err := json.Marshal(msg.Payload)
	if err != nil {
		return err
	}
	return p.publisher.Publish(ctx, workflow.Message{
		ID: msg.ID, Topic: msg.Topic, Key: msg.Key, Payload: payload,
		Headers: map[string]string{"workflow_outbox_id": msg.ID},
	})
}
