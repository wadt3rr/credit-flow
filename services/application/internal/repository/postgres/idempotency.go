package postgres

import (
	"context"
	"errors"
	"fmt"
	"uuid"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/wadt3rr/credit-flow/services/application/internal/domain"
)

type IdempotencyRecordsRepository struct {
	pool *pgxpool.Pool
}

func NewIdempotencyRecordsRepository(pool *pgxpool.Pool) *IdempotencyRecordsRepository {
	return &IdempotencyRecordsRepository{
		pool: pool,
	}
}

func (r *IdempotencyRecordsRepository) executor(ctx context.Context) DBTX {
	if tx, ok := txFromContext(ctx); ok {
		return tx
	}

	return r.pool
}

func (r *IdempotencyRecordsRepository) Get(ctx context.Context, subjectID uuid.UUID, operation string, idempotencyKey string) (*domain.IdempotencyRecord, error) {
	const op = "internal.repository.postgres.idempotency.Get"

	query := `
		SELECT
			id,
			subject_id,
			operation,
			resource_id,
			idempotency_key,
			request_hash,
			response_status,
			response_body,
			response_headers,
			created_at,
			expires_at
		FROM idempotency_records
		WHERE subject_id = $1 AND operation = $2 AND idempotency_key = $3;
	`

	record := &domain.IdempotencyRecord{}

	row := r.executor(ctx).QueryRow(ctx, query, subjectID, operation, idempotencyKey)
	if err := row.Scan(
		&record.ID,
		&record.SubjectID,
		&record.Operation,
		&record.ResourceID,
		&record.IdempotencyKey,
		&record.RequestHash,
		&record.ResponseStatus,
		&record.ResponseBody,
		&record.ResponseHeaders,
		&record.CreatedAt,
		&record.ExpiresAt,
	); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("%s: scan row: %w", op, err)
	}

	return record, nil
}

func (r *IdempotencyRecordsRepository) CreateIfAbsent(ctx context.Context, record domain.IdempotencyRecord) (bool, error) {
	const op = "internal.repository.postgres.idempotency.Create"

	query := `
	INSERT INTO idempotency_records (
		id,
		subject_id,
		operation,
		resource_id,
		idempotency_key,
		request_hash,
		response_status,
		response_body,
		response_headers,
		expires_at 
	) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
	ON CONFLICT ON CONSTRAINT uq_idempotency_subject_operation_key
	DO NOTHING;
	`

	tag, err := r.executor(ctx).Exec(
		ctx, query,
		record.ID,
		record.SubjectID,
		record.Operation,
		record.ResourceID,
		record.IdempotencyKey,
		record.RequestHash,
		record.ResponseStatus,
		record.ResponseBody,
		record.ResponseHeaders,
		record.ExpiresAt,
	)
	if err != nil {
		return false, fmt.Errorf("%s: %w", op, err)
	}

	return tag.RowsAffected() == 1, nil
}
