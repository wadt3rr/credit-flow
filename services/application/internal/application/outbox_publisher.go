package application

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"
	"uuid"

	"github.com/wadt3rr/credit-flow/services/application/internal/domain"
	"github.com/wadt3rr/credit-flow/services/application/internal/infrastructure/kafka"
)

type OutboxRepository interface {
	ClaimBatch(ctx context.Context, publisherID uuid.UUID) ([]domain.OutboxEvent, error)
	MarkPublished(ctx context.Context, eventID uuid.UUID, publisherID uuid.UUID) error
	ReleaseClaim(ctx context.Context, eventID uuid.UUID, publisherID uuid.UUID, publishErr error) error
}

type EventProducer interface {
	Publish(ctx context.Context, messages ...kafka.Message) error
}

type OutboxPublisher struct {
	repository  OutboxRepository
	producer    EventProducer
	publisherID uuid.UUID
}

func NewOutboxPublisher(repository OutboxRepository, producer EventProducer) (*OutboxPublisher, error) {
	const op = "internal.application.outbox_publisher.NewOutboxPublisher"

	if repository == nil {
		return nil, fmt.Errorf("%s: repository must not be nil", op)
	}

	if producer == nil {
		return nil, fmt.Errorf("%s: producer must not be nil", op)
	}

	return &OutboxPublisher{
		repository:  repository,
		producer:    producer,
		publisherID: uuid.NewV7(),
	}, nil
}

func (p *OutboxPublisher) ProcessBatch(ctx context.Context) error {
	const op = "internal.application.outbox_publisher.ProcessBatch"

	batch, err := p.repository.ClaimBatch(ctx, p.publisherID)
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}
	if len(batch) == 0 {
		return nil
	}

	var batchErr error

	for _, event := range batch {
		if err := ctx.Err(); err != nil {
			batchErr = errors.Join(batchErr, err)
			break
		}
		message := kafka.Message{
			Key:   event.AggregateID[:],
			Value: event.Payload,
		}

		if err := p.producer.Publish(ctx, message); err != nil {
			releaseCtx, cancel := context.WithTimeout(
				context.WithoutCancel(ctx),
				5*time.Second,
			)

			releaseErr := p.repository.ReleaseClaim(
				releaseCtx,
				event.ID,
				p.publisherID,
				err,
			)
			cancel()

			if releaseErr != nil {
				batchErr = errors.Join(
					batchErr,
					fmt.Errorf("release event %s: %w", event.ID, releaseErr),
				)
			}

			batchErr = errors.Join(
				batchErr,
				fmt.Errorf("publish event %s: %w", event.ID, err),
			)
			continue
		}

		if err := p.repository.MarkPublished(ctx, event.ID, p.publisherID); err != nil {
			batchErr = errors.Join(batchErr, fmt.Errorf("mark published event %s: %w", event.ID, err))
		}
	}

	return batchErr

}

func (p *OutboxPublisher) Run(ctx context.Context, interval time.Duration) error {
	const (
		op           = "internal.application.outbox_publisher.Run"
		batchTimeout = 30 * time.Second
	)
	if interval <= 0 {
		return fmt.Errorf("%s: interval must be positive", op)
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		if err := ctx.Err(); err != nil {
			return nil
		}

		batchCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), batchTimeout)

		err := p.ProcessBatch(batchCtx)
		cancel()

		if err != nil {
			slog.Error(
				"process outbox batch",
				"publisher_id", p.publisherID,
				"error", err,
			)
		}

		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}
