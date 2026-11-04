package postgres

import (
	"context"
	"fmt"
	"uuid"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/wadt3rr/credit-flow/services/application/internal/domain"
)

const maxOutboxAttempts = 10

type OutboxRepository struct {
	pool *pgxpool.Pool
}

func NewOutboxRepository(pool *pgxpool.Pool) *OutboxRepository {
	return &OutboxRepository{
		pool: pool,
	}
}

func (r *OutboxRepository) ClaimBatch(ctx context.Context, publisherID uuid.UUID) ([]domain.OutboxEvent, error) {
	const op = "internal.repository.postgres.outbox.ClaimBatch"
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	defer tx.Rollback(ctx)

	query := `
		WITH exhausted_events AS (
			SELECT id
			FROM outbox_events
			WHERE
				attempts >= 10
				AND (
					status = 'pending'
					OR (
						status = 'claimed'
						AND locked_until <= NOW()
					)
				)
			FOR UPDATE SKIP LOCKED
		),
		marked_as_failed AS (
			UPDATE outbox_events AS o
			SET
				status = 'failed',
				locked_by = NULL,
				locked_until = NULL
			FROM exhausted_events AS e
			WHERE o.id = e.id
			RETURNING o.id
		),
		events_to_claim AS (
			SELECT id
			FROM outbox_events
			WHERE
				attempts < 10
				AND (
					(
						status = 'pending'
						AND next_attempt_at <= NOW()
					)
					OR (
						status = 'claimed'
						AND locked_until <= NOW()
					)
				)
			ORDER BY created_at ASC, id ASC
			LIMIT 100
			FOR UPDATE SKIP LOCKED
		)
		UPDATE outbox_events AS o
		SET
			status = 'claimed',
			locked_by = $1,
			locked_until = NOW() + INTERVAL '30 seconds',
			attempts = o.attempts + 1
		FROM events_to_claim AS e
		WHERE o.id = e.id
		RETURNING
			o.id,
			o.aggregate_type,
			o.aggregate_id,
			o.event_type,
			o.payload,
			o.attempts
		;
	
	`

	rows, err := tx.Query(ctx, query, publisherID)
	if err != nil {
		return nil, fmt.Errorf("%s: query events: %w", op, err)
	}
	defer rows.Close()

	events := make([]domain.OutboxEvent, 0, 100)

	for rows.Next() {
		var event domain.OutboxEvent

		err := rows.Scan(
			&event.ID,
			&event.AggregateType,
			&event.AggregateID,
			&event.EventType,
			&event.Payload,
			&event.Attempts,
		)
		if err != nil {
			return nil, fmt.Errorf("%s: scan rows: %w", op, err)
		}

		events = append(events, event)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("%s: iterate rows: %w", op, err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("%s: commit transaction: %w", op, err)
	}

	return events, nil
}

func (r *OutboxRepository) MarkPublished(ctx context.Context, eventID uuid.UUID, publisherID uuid.UUID) error {
	const op = "internal.repository.postgres.outbox.MarkPublished"

	query := `
		UPDATE outbox_events
		SET
			status = 'published',
			published_at = NOW(),
			locked_by = NULL,
			locked_until = NULL
		WHERE id = $1
		AND locked_by = $2
		AND locked_until > NOW()
		AND published_at IS NULL;
	`
	tag, err := r.pool.Exec(ctx, query, eventID, publisherID)
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	if tag.RowsAffected() != 1 {
		return fmt.Errorf("%s: %w", op, domain.ErrLeaseLost)
	}

	return nil

}

func (r *OutboxRepository) ReleaseClaim(ctx context.Context, eventID uuid.UUID, publisherID uuid.UUID, publishErr error) error {
	const op = "internal.repository.postgres.outbox.ReleaseClaim"

	query := `
		UPDATE outbox_events
		SET
			status = 'pending',
			last_error = $3,
			locked_by = NULL,
			locked_until = NULL
		WHERE id = $1
		AND locked_by = $2
		AND published_at IS NULL;
	`

	if publishErr == nil {
		return fmt.Errorf("%s: publish error is nil", op)
	}

	tag, err := r.pool.Exec(ctx, query, eventID, publisherID, publishErr.Error())
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	if tag.RowsAffected() != 1 {
		return fmt.Errorf("%s: %w", op, domain.ErrLeaseLost)
	}

	return nil
}
