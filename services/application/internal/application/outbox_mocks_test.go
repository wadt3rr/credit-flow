package application

import (
	"context"
	"uuid"

	"github.com/wadt3rr/credit-flow/services/application/internal/domain"
	"github.com/wadt3rr/credit-flow/services/application/internal/infrastructure/kafka"
)

var _ OutboxRepository = (*MockOutboxRepository)(nil)
var _ EventProducer = (*MockEventProducer)(nil)

type MockEventProducer struct {
	Messages []kafka.Message
	Errors   []error
	Calls    int
}

type MockOutboxRepository struct {
	Events []domain.OutboxEvent

	ClaimErr   error
	MarkErr    error
	ReleaseErr error

	ClaimedPublisherID uuid.UUID
	MarkedEvents       []uuid.UUID
	ReleasedEvents     []uuid.UUID

	ClaimNotifications chan struct{}
}

func (p *MockEventProducer) Publish(ctx context.Context, messages ...kafka.Message) error {
	p.Messages = append(p.Messages, messages...)

	if p.Calls >= len(p.Errors) {
		p.Calls++
		return nil
	}

	err := p.Errors[p.Calls]
	p.Calls++

	return err
}

func (r *MockOutboxRepository) ClaimBatch(
	ctx context.Context,
	publisherID uuid.UUID,
) ([]domain.OutboxEvent, error) {
	r.ClaimedPublisherID = publisherID

	if r.ClaimNotifications != nil {
		select {
		case r.ClaimNotifications <- struct{}{}:
		default:
		}
	}

	return r.Events, r.ClaimErr
}

func (r *MockOutboxRepository) MarkPublished(ctx context.Context, eventID uuid.UUID, publisherID uuid.UUID) error {
	r.MarkedEvents = append(r.MarkedEvents, eventID)

	return r.MarkErr
}

func (r *MockOutboxRepository) ReleaseClaim(ctx context.Context, eventID uuid.UUID, publisherID uuid.UUID, publishErr error) error {
	r.ReleasedEvents = append(r.ReleasedEvents, eventID)

	return r.ReleaseErr
}
