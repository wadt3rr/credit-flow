package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/wadt3rr/credit-flow/services/application/internal/application"
)

type transactionManager struct {
	pool *pgxpool.Pool
}

var _ application.TransactionManager = (*transactionManager)(nil)

func NewTransactionManager(pool *pgxpool.Pool) application.TransactionManager {
	return &transactionManager{
		pool: pool,
	}
}

type txContexKey struct{}

func contextWithTx(ctx context.Context, tx pgx.Tx) context.Context {
	return context.WithValue(ctx, txContexKey{}, tx)
}

func txFromContext(ctx context.Context) (pgx.Tx, bool) {
	tx, ok := ctx.Value(txContexKey{}).(pgx.Tx)
	return tx, ok
}

func (m *transactionManager) WithinTransaction(
	ctx context.Context,
	fn func(ctx context.Context) error,
) error {
	const op = "services.application.internal.infrastructure.postgres.transaction_manager.WithinTransaction"

	if fn == nil {
		return errors.New("transaction callback is nil")
	}

	tx, err := m.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("%s: begin transaction: %w", op, err)
	}

	defer func() {
		rollbackErr := tx.Rollback(ctx)
		if rollbackErr != nil && !errors.Is(rollbackErr, pgx.ErrTxClosed) {
			err = errors.Join(err, fmt.Errorf("rollback transaction: %w", rollbackErr))
		}
	}()

	txCtx := contextWithTx(ctx, tx)

	if err = fn(txCtx); err != nil {
		return fmt.Errorf("%s: execute transaction: %w", op, err)
	}

	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("%s: commit transaction: %w", op, err)
	}

	return nil
}
