package kafka

import (
	"context"
	"fmt"

	"github.com/segmentio/kafka-go"
)

type Producer struct {
	writer *kafka.Writer
}

type Message struct {
	Key   []byte
	Value []byte
}

func NewProducer(brokers []string, topic string) *Producer {
	return &Producer{
		writer: &kafka.Writer{
			Addr:         kafka.TCP(brokers...),
			Topic:        topic,
			Balancer:     &kafka.Hash{},
			RequiredAcks: kafka.RequireAll,
			Async:        false,
		},
	}
}

func (p *Producer) Publish(ctx context.Context, messages ...Message) error {
	const op = "internal.infrastructure.kafka.producer.Publish"

	if len(messages) == 0 {
		return nil
	}

	kafkaMessages := make([]kafka.Message, 0, len(messages))

	for _, message := range messages {
		kafkaMessages = append(kafkaMessages, kafka.Message{
			Key:   message.Key,
			Value: message.Value,
		})
	}

	err := p.writer.WriteMessages(ctx, kafkaMessages...)
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	return nil
}

func (p *Producer) Close() error {
	const op = "internal.infrastructure.kafka.producer.Close"

	if err := p.writer.Close(); err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	return nil
}
