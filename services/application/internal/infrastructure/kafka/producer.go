package kafka

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/segmentio/kafka-go"
)

type Producer struct {
	writer *kafka.Writer
}

type Message struct {
	Key   []byte
	Value []byte
}

func NewProducer(brokers []string, topic string) (*Producer, error) {
	if len(brokers) == 0 {
		return nil, errors.New("brokers list must not be empty")
	}

	normalizedBrokers := make([]string, 0, len(brokers))

	for _, broker := range brokers {
		broker = strings.TrimSpace(broker)
		if broker == "" {
			return nil, errors.New("broker address must not be empty")
		}

		normalizedBrokers = append(normalizedBrokers, broker)
	}

	topic = strings.TrimSpace(topic)

	if topic == "" {
		return nil, errors.New("topic must not be empty")
	}

	return &Producer{
		writer: &kafka.Writer{
			Addr:         kafka.TCP(normalizedBrokers...),
			Topic:        topic,
			Balancer:     &kafka.Hash{},
			RequiredAcks: kafka.RequireAll,
			Async:        false,
		},
	}, nil
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
