package application

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
	"uuid"

	"github.com/wadt3rr/credit-flow/services/application/internal/domain"
)

func TestProcessBatchSuccess(t *testing.T) {
	event := domain.OutboxEvent{
		ID:            uuid.NewV7(),
		AggregateID:   uuid.NewV7(),
		AggregateType: "application",
		EventType:     "ApplicationCreated",
		Payload:       json.RawMessage(`{"application_id":"test"}`),
	}

	repository := &MockOutboxRepository{
		Events: []domain.OutboxEvent{event},
	}

	producer := &MockEventProducer{}

	publisher, err := NewOutboxPublisher(repository, producer)

	if err != nil {
		t.Fatalf("create outbox publisher: %v", err)
	}

	ctx := context.Background()

	if err := publisher.ProcessBatch(ctx); err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if len(producer.Messages) != 1 {
		t.Fatalf("expected 1 message, got: %d", len(producer.Messages))
	}

	if !bytes.Equal(producer.Messages[0].Key, event.AggregateID[:]) {
		t.Fatalf("expected %q key, got: %q", event.AggregateID, producer.Messages[0].Key)
	}

	if !bytes.Equal(producer.Messages[0].Value, event.Payload) {
		t.Fatalf("expected %q value, got: %q", event.Payload, producer.Messages[0].Value)
	}

	if len(repository.MarkedEvents) != 1 {
		t.Fatalf("expected 1 marked event, got: %d", len(repository.MarkedEvents))
	}

	if repository.MarkedEvents[0] != event.ID {
		t.Fatalf("expected event ID %s, got: %s", event.ID, repository.MarkedEvents[0])
	}

	if len(repository.ReleasedEvents) != 0 {
		t.Fatalf("expected 0 events, got: %d", len(repository.ReleasedEvents))
	}

}

func TestProcessBatchPublishError(t *testing.T) {
	event := domain.OutboxEvent{
		ID:            uuid.NewV7(),
		AggregateID:   uuid.NewV7(),
		AggregateType: "application",
		EventType:     "ApplicationCreated",
		Payload:       json.RawMessage(`{"application_id":"test"}`),
	}

	repository := &MockOutboxRepository{
		Events: []domain.OutboxEvent{event},
	}

	producer := &MockEventProducer{
		Errors: []error{errors.New("kafka unavailable")},
	}

	publisher, err := NewOutboxPublisher(repository, producer)

	if err != nil {
		t.Fatalf("create outbox publisher: %v", err)
	}

	ctx := context.Background()
	err = publisher.ProcessBatch(ctx)

	if err == nil {
		t.Fatalf("expected error, got no error")
	}

	if len(producer.Messages) != 1 {
		t.Fatalf("producer didn't get message")
	}

	if len(repository.ReleasedEvents) != 1 {
		t.Fatalf("expected 1 released event, got: %d", len(repository.ReleasedEvents))
	}

	if repository.ReleasedEvents[0] != event.ID {
		t.Fatalf("expected event ID %s, got: %s", event.ID, repository.ReleasedEvents[0])
	}

	if len(repository.MarkedEvents) != 0 {
		t.Fatalf("expected 0 marked events, got: %d", len(repository.MarkedEvents))
	}
}

func TestProcessBatchMarkPublishedError(t *testing.T) {
	event := domain.OutboxEvent{
		ID:            uuid.NewV7(),
		AggregateID:   uuid.NewV7(),
		AggregateType: "application",
		EventType:     "ApplicationCreated",
		Payload:       json.RawMessage(`{"application_id":"test"}`),
	}

	repository := &MockOutboxRepository{
		Events:  []domain.OutboxEvent{event},
		MarkErr: errors.New("database unavailable"),
	}

	producer := &MockEventProducer{}

	publisher, err := NewOutboxPublisher(repository, producer)

	if err != nil {
		t.Fatalf("create outbox publisher: %v", err)
	}

	ctx := context.Background()
	err = publisher.ProcessBatch(ctx)
	if !errors.Is(err, repository.MarkErr) {
		t.Fatalf("expected %v, got: %v", repository.MarkErr, err)
	}

	if len(producer.Messages) != 1 {
		t.Fatalf("expected 1 message, got: %d", len(producer.Messages))
	}

	if len(repository.MarkedEvents) != 1 {
		t.Fatalf("expected 1 marked event, got: %d", len(repository.MarkedEvents))
	}

	if repository.MarkedEvents[0] != event.ID {
		t.Fatalf("expected event ID %s, got: %s", event.ID, repository.MarkedEvents[0])
	}

	if len(repository.ReleasedEvents) != 0 {
		t.Fatalf("expected 0 events, got: %d", len(repository.ReleasedEvents))
	}

}

func TestProcessBatchClaimError(t *testing.T) {
	claimErr := errors.New("database unavailable")

	repository := &MockOutboxRepository{
		ClaimErr: claimErr,
	}

	producer := &MockEventProducer{}

	publisher, err := NewOutboxPublisher(repository, producer)
	if err != nil {
		t.Fatalf("create outbox publisher: %v", err)
	}

	ctx := context.Background()
	err = publisher.ProcessBatch(ctx)

	if !errors.Is(err, claimErr) {
		t.Fatalf("expected %v, got: %v", claimErr, err)
	}

	if len(producer.Messages) != 0 {
		t.Fatalf("expected 0 messages, got: %d", len(producer.Messages))
	}

	if len(repository.MarkedEvents) != 0 {
		t.Fatalf("expected 0 marked events, got: %d", len(repository.MarkedEvents))
	}

	if len(repository.ReleasedEvents) != 0 {
		t.Fatalf("expected 0 released events, got: %d", len(repository.ReleasedEvents))
	}
}

func TestProcessBatchEmpty(t *testing.T) {
	repository := &MockOutboxRepository{}
	producer := &MockEventProducer{}

	publisher, err := NewOutboxPublisher(repository, producer)
	if err != nil {
		t.Fatalf("create outbox publisher: %v", err)
	}

	ctx := context.Background()
	err = publisher.ProcessBatch(ctx)

	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if len(producer.Messages) != 0 {
		t.Fatalf("expected 0 messages, got: %d", len(producer.Messages))
	}

	if len(repository.MarkedEvents) != 0 {
		t.Fatalf("expected 0 marked events, got: %d", len(repository.MarkedEvents))
	}

	if len(repository.ReleasedEvents) != 0 {
		t.Fatalf("expected 0 released events, got: %d", len(repository.ReleasedEvents))
	}
}

func TestProcessBatchMultipleEvents(t *testing.T) {
	events := []domain.OutboxEvent{
		{
			ID:          uuid.NewV7(),
			AggregateID: uuid.NewV7(),
			EventType:   "ApplicationCreated",
			Payload:     json.RawMessage(`{"id":1}`),
		},
		{
			ID:          uuid.NewV7(),
			AggregateID: uuid.NewV7(),
			EventType:   "ApplicationUpdated",
			Payload:     json.RawMessage(`{"id":2}`),
		},
		{
			ID:          uuid.NewV7(),
			AggregateID: uuid.NewV7(),
			EventType:   "ApplicationSubmitted",
			Payload:     json.RawMessage(`{"id":3}`),
		},
	}

	repository := &MockOutboxRepository{
		Events: events,
	}

	publishErr := errors.New("kafka unavailable")

	producer := &MockEventProducer{
		Errors: []error{
			nil,
			publishErr,
			nil,
		},
	}

	publisher, err := NewOutboxPublisher(repository, producer)
	if err != nil {
		t.Fatalf("create outbox publisher: %v", err)
	}

	err = publisher.ProcessBatch(context.Background())

	if !errors.Is(err, publishErr) {
		t.Fatalf("expected %v, got: %v", publishErr, err)
	}

	if producer.Calls != 3 {
		t.Fatalf("expected 3 publish calls, got: %d", producer.Calls)
	}

	if len(producer.Messages) != 3 {
		t.Fatalf("expected 3 messages, got: %d", len(producer.Messages))
	}

	if len(repository.MarkedEvents) != 2 {
		t.Fatalf("expected 2 marked events, got: %d", len(repository.MarkedEvents))
	}

	if repository.MarkedEvents[0] != events[0].ID ||
		repository.MarkedEvents[1] != events[2].ID {
		t.Fatal("unexpected marked events")
	}

	if len(repository.ReleasedEvents) != 1 {
		t.Fatalf("expected 1 released event, got: %d", len(repository.ReleasedEvents))
	}

	if repository.ReleasedEvents[0] != events[1].ID {
		t.Fatalf("expected released event %s, got: %s",
			events[1].ID, repository.ReleasedEvents[0])
	}
}

func TestRunProcessesImmediately(t *testing.T) {
	repository := &MockOutboxRepository{
		ClaimNotifications: make(chan struct{}, 1),
	}
	producer := &MockEventProducer{}

	publisher, err := NewOutboxPublisher(repository, producer)
	if err != nil {
		t.Fatalf("create outbox publisher: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)

	go func() {
		done <- publisher.Run(ctx, time.Hour)
	}()

	select {
	case <-repository.ClaimNotifications:
	case <-time.After(time.Second):
		t.Fatal("expected immediate ProcessBatch call")
	}

	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run did not stop after context cancellation")
	}
}

func TestRunProcessesOnInterval(t *testing.T) {
	repository := &MockOutboxRepository{
		ClaimNotifications: make(chan struct{}, 2),
	}
	producer := &MockEventProducer{}

	publisher, err := NewOutboxPublisher(repository, producer)
	if err != nil {
		t.Fatalf("create outbox publisher: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)

	go func() {
		done <- publisher.Run(ctx, 20*time.Millisecond)
	}()

	for i := 0; i < 2; i++ {
		select {
		case <-repository.ClaimNotifications:

		case <-time.After(time.Second):
			t.Fatalf("expected ProcessBatch call %d", i+1)
		}
	}

	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run did not stop after context cancellation")
	}
}

func TestRunInvalidInterval(t *testing.T) {
	tests := []struct {
		name     string
		interval time.Duration
	}{
		{
			name:     "zero interval",
			interval: 0,
		},
		{
			name:     "negative interval",
			interval: -time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repository := &MockOutboxRepository{}
			producer := &MockEventProducer{}

			publisher, err := NewOutboxPublisher(repository, producer)
			if err != nil {
				t.Fatalf("create outbox publisher: %v", err)
			}

			err = publisher.Run(context.Background(), tt.interval)

			if err == nil {
				t.Fatal("expected error for invalid interval")
			}
		})
	}
}

func TestRunContinuesAfterProcessBatchError(t *testing.T) {
	repository := &MockOutboxRepository{
		ClaimErr:           errors.New("database unavailable"),
		ClaimNotifications: make(chan struct{}, 2),
	}
	producer := &MockEventProducer{}

	publisher, err := NewOutboxPublisher(repository, producer)
	if err != nil {
		t.Fatalf("create outbox publisher: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)

	go func() {
		done <- publisher.Run(ctx, 20*time.Millisecond)
	}()

	for i := 0; i < 2; i++ {
		select {
		case <-repository.ClaimNotifications:
		case <-time.After(time.Second):
			t.Fatalf("expected ProcessBatch call %d", i+1)
		}
	}

	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("expected no error after cancellation, got: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run did not stop after context cancellation")
	}
}
