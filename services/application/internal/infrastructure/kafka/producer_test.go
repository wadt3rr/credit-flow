package kafka

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"
	"uuid"

	kafkago "github.com/segmentio/kafka-go"
)

func TestNewProducer(t *testing.T) {
	tests := []struct {
		name    string
		brokers []string
		topic   string
		wantErr bool
	}{
		{
			name:    "empty brokers",
			brokers: []string{},
			topic:   "applications",
			wantErr: true,
		},
		{
			name:    "empty broker address",
			brokers: []string{""},
			topic:   "applications",
			wantErr: true,
		},
		{
			name:    "whitespace broker address",
			brokers: []string{"   "},
			topic:   "applications",
			wantErr: true,
		},
		{
			name:    "empty topic",
			brokers: []string{"test"},
			topic:   "",
			wantErr: true,
		},
		{
			name:    "whitespace topic",
			brokers: []string{"test"},
			topic:   "    ",
			wantErr: true,
		},
		{
			name:    "valid config",
			brokers: []string{"test"},
			topic:   "applications",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			producer, err := NewProducer(tt.brokers, tt.topic)

			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}

				if producer != nil {
					t.Fatal("expected nil producer on error")
				}

				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if producer == nil {
				t.Fatal("expected producer, got nil")
			}

			if producer.writer == nil {
				t.Fatal("expected writer, got nil")
			}

			if err := producer.Close(); err != nil {
				t.Fatalf("close producer: %v", err)
			}
		})
	}
}

func TestProducerIntegration(t *testing.T) {
	brokersEnv := os.Getenv("KAFKA_TEST_BROKERS")
	topic := os.Getenv("KAFKA_TEST_TOPIC")

	if brokersEnv == "" || topic == "" {
		t.Skip("set KAFKA_TEST_BROKERS and KAFKA_TEST_TOPIC to run integration test")
	}

	brokers := strings.Split(brokersEnv, ",")

	ctx, cancel := context.WithTimeout(
		context.Background(),
		15*time.Second,
	)
	defer cancel()

	producer, err := NewProducer(brokers, topic)
	if err != nil {
		t.Fatalf("create producer: %v", err)
	}
	defer producer.Close()

	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:     brokers,
		Topic:       topic,
		GroupID:     uuid.NewV7().String(),
		MinBytes:    1,
		MaxBytes:    10e6,
		StartOffset: kafkago.FirstOffset,
	})
	defer reader.Close()

	key := []byte(uuid.NewV7().String())
	value := []byte(`{"event":"integration-test"}`)

	err = producer.Publish(ctx, Message{
		Key:   key,
		Value: value,
	})
	if err != nil {
		t.Fatalf("publish message: %v", err)
	}

	for {
		message, err := reader.FetchMessage(ctx)
		if err != nil {
			t.Fatalf("fetch message: %v", err)
		}

		if string(message.Key) != string(key) {
			continue
		}

		if string(message.Value) != string(value) {
			t.Fatalf(
				"unexpected message value: got %s, want %s",
				message.Value,
				value,
			)
		}

		break
	}
}
