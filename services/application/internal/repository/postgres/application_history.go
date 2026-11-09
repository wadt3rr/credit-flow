package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/wadt3rr/credit-flow/services/application/internal/domain"
)

type ApplicationHistoryRepository struct {
	pool *pgxpool.Pool
}

func NewApplicationHistoryRepository(pool *pgxpool.Pool) *ApplicationHistoryRepository {
	return &ApplicationHistoryRepository{
		pool: pool,
	}
}

func (r *ApplicationHistoryRepository) executor(ctx context.Context) DBTX {
	if tx, ok := txFromContext(ctx); ok {
		return tx
	}

	return r.pool
}

func (r *ApplicationHistoryRepository) Create(ctx context.Context, appHistory domain.ApplicationHistory) error {
	const op = "internal.repository.postgres.application_history.Create"

	query := `
		INSERT INTO application_history (id, application_id, prev_status, new_status, actor_type, actor_id, reason) VALUES ($1, $2, $3, $4, $5, $6, $7);
	`

	tag, err := r.executor(ctx).Exec(ctx, query, appHistory.ID, appHistory.ApplicationID, appHistory.PrevStatus, appHistory.NewStatus, appHistory.ActorType, appHistory.ActorID, appHistory.Reason)
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	if tag.RowsAffected() != 1 {
		return fmt.Errorf("%s: expected 1 affected row, got %d",
			op, tag.RowsAffected())
	}

	return nil
}
