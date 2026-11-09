package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/wadt3rr/credit-flow/services/application/internal/domain"
)

type ApplicationRepository struct {
	pool *pgxpool.Pool
}

func NewApplicationRepository(pool *pgxpool.Pool) *ApplicationRepository {
	return &ApplicationRepository{
		pool: pool,
	}
}

func (r *ApplicationRepository) executor(ctx context.Context) DBTX {
	if tx, ok := txFromContext(ctx); ok {
		return tx
	}

	return r.pool
}

func (r *ApplicationRepository) Create(ctx context.Context, app domain.Application) (domain.Application, error) {
	const op = "internal.repository.postgres.application.Create"

	query := `
		INSERT INTO applications (
			id, company_id, product_id, amount, term, purpose, status
		) VALUES ($1, $2, $3, $4, $5, $6, $7)
		RETURNING version, created_at, updated_at;
	`

	row := r.executor(ctx).QueryRow(
		ctx,
		query,
		app.ID,
		app.CompanyID,
		app.ProductID,
		app.Amount,
		app.Term,
		app.Purpose,
		app.Status,
	)
	if err := row.Scan(&app.Version, &app.CreatedAt, &app.UpdatedAt); err != nil {
		return domain.Application{}, fmt.Errorf("%s: %w", op, err)
	}

	return app, nil
}
