package postgres

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"testing"
	"time"
	"uuid"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/wadt3rr/credit-flow/services/application/internal/domain"
)

func TestClaimBatch(t *testing.T) {
	pool := newTestPool(t)
	cleanOutbox(t, pool)

	publisherID := uuid.NewV7()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	event := insertOutboxEvent(t, ctx, pool)

	repository := NewOutboxRepository(pool)

	events, err := repository.ClaimBatch(ctx, publisherID)
	if err != nil {
		t.Fatalf("claim batch: %v", err)
	}

	if len(events) != 1 {
		t.Fatalf("expected 1 event, got: %d", len(events))
	}

	if events[0].ID != event.ID {
		t.Fatalf("expected equal IDs, got %s != %s", events[0].ID, event.ID)
	}

	if events[0].AggregateID != event.AggregateID {
		t.Fatalf("expected equal AggregateIDs, got %s != %s", events[0].AggregateID, event.AggregateID)
	}

	if events[0].AggregateType != event.AggregateType {
		t.Fatalf("expected equal AggregateTypes, got %s != %s", events[0].AggregateType, event.AggregateType)
	}

	if events[0].EventType != event.EventType {
		t.Fatalf("expected equal EventTypes, got %s != %s", events[0].EventType, event.EventType)
	}

	var got, want any

	if err := json.Unmarshal(events[0].Payload, &got); err != nil {
		t.Fatalf("unmarshal actual payload: %v", err)
	}

	if err := json.Unmarshal(event.Payload, &want); err != nil {
		t.Fatalf("unmarshal expected payload: %v", err)
	}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("expected equal payloads, got %s != %s",
			events[0].Payload, event.Payload)
	}

	if events[0].Attempts != 1 {
		t.Fatalf("expected 1 attempt, got: %d", events[0].Attempts)
	}

	query2 := `SELECT locked_by, locked_until, published_at FROM outbox_events WHERE id = $1`

	row := pool.QueryRow(ctx, query2, event.ID)

	var lockedBy uuid.UUID
	var lockedUntil, publishedAt pgtype.Timestamptz

	if err := row.Scan(&lockedBy, &lockedUntil, &publishedAt); err != nil {
		t.Fatalf("scan outbox event: %v", err)
	}

	if lockedBy != publisherID {
		t.Fatalf("expected lock by: %s, got: %s", publisherID, lockedBy)
	}

	if !lockedUntil.Valid {
		t.Fatal("expected locked_until to be set")
	}

	if publishedAt.Valid {
		t.Fatal("expected published_at to be NULL")
	}

}

func TestClaimBatchSkipsPendingEventBeforeNextAttempt(t *testing.T) {
	pool := newTestPool(t)
	cleanOutbox(t, pool)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	event := insertOutboxEvent(t, ctx, pool)

	_, err := pool.Exec(ctx, `
		UPDATE outbox_events
		SET next_attempt_at = NOW() + INTERVAL '1 hour'
		WHERE id = $1
	`, event.ID)
	if err != nil {
		t.Fatalf("update next_attempt_at: %v", err)
	}

	repository := NewOutboxRepository(pool)

	events, err := repository.ClaimBatch(ctx, uuid.NewV7())
	if err != nil {
		t.Fatalf("claim batch: %v", err)
	}

	if len(events) != 0 {
		t.Fatalf("expected no events, got: %d", len(events))
	}

	var status string
	var attempts int32

	err = pool.QueryRow(ctx, `
		SELECT status, attempts
		FROM outbox_events
		WHERE id = $1
	`, event.ID).Scan(&status, &attempts)
	if err != nil {
		t.Fatalf("scan outbox event: %v", err)
	}

	if status != "pending" {
		t.Fatalf("expected status pending, got: %s", status)
	}

	if attempts != 0 {
		t.Fatalf("expected 0 attempts, got: %d", attempts)
	}
}

func TestClaimBatchReclaimsExpiredLease(t *testing.T) {
	pool := newTestPool(t)
	cleanOutbox(t, pool)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	event := insertOutboxEvent(t, ctx, pool)

	repository := NewOutboxRepository(pool)

	publisherAID := uuid.NewV7()
	publisherBID := uuid.NewV7()

	events, err := repository.ClaimBatch(ctx, publisherAID)
	if err != nil {
		t.Fatalf("claim batch: %v", err)
	}

	_, err = pool.Exec(ctx, `UPDATE outbox_events SET locked_until = NOW() - INTERVAL '40 seconds' WHERE id = $1`, event.ID)
	if err != nil {
		t.Fatalf("update locked_intil: %v", err)
	}

	events, err = repository.ClaimBatch(ctx, publisherBID)
	if err != nil {
		t.Fatalf("reclaim batch: %v", err)
	}

	if len(events) != 1 {
		t.Fatalf("expected 1 event, got: %d", len(events))
	}

	if len(events) != 1 {
		t.Fatalf("expected 1 event, got: %d", len(events))
	}

	if events[0].ID != event.ID {
		t.Fatalf("expected equal IDs, got %s != %s", events[0].ID, event.ID)
	}

	if events[0].Attempts != 2 {
		t.Fatalf("expected 2 attempts, got: %d", events[0].Attempts)
	}

	row := pool.QueryRow(ctx, `SELECT locked_by, locked_until, published_at, status FROM outbox_events WHERE id = $1`, event.ID)

	var lockedBy uuid.UUID
	var lockedUntil, publishedAt pgtype.Timestamptz
	var status string

	if err := row.Scan(&lockedBy, &lockedUntil, &publishedAt, &status); err != nil {
		t.Fatalf("scan outbox event: %v", err)
	}

	if lockedBy != publisherBID {
		t.Fatalf("expected lock by: %s, got: %s", publisherBID, lockedBy)
	}

	if !lockedUntil.Valid {
		t.Fatal("expected locked_until to be set")
	}

	if publishedAt.Valid {
		t.Fatal("expected published_at to be NULL")
	}

	if status != "claimed" {
		t.Fatalf("expected status claimed, got: %q", status)
	}

}

func TestClaimBatchSkipsPublishedEvents(t *testing.T) {
	pool := newTestPool(t)
	cleanOutbox(t, pool)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	event := insertOutboxEvent(t, ctx, pool)

	repository := NewOutboxRepository(pool)

	publisherID := uuid.NewV7()

	// Захватываем событие.
	events, err := repository.ClaimBatch(ctx, publisherID)
	if err != nil {
		t.Fatalf("claim batch: %v", err)
	}

	if len(events) != 1 {
		t.Fatalf("expected 1 event, got: %d", len(events))
	}

	// Помечаем событие как опубликованное.
	if err := repository.MarkPublished(ctx, event.ID, publisherID); err != nil {
		t.Fatalf("mark published: %v", err)
	}

	// Пытаемся получить события повторно.
	events, err = repository.ClaimBatch(ctx, uuid.NewV7())
	if err != nil {
		t.Fatalf("claim batch after publishing: %v", err)
	}

	if len(events) != 0 {
		t.Fatalf("expected no events, got: %d", len(events))
	}

	// Проверяем состояние записи непосредственно в БД.
	var (
		status      string
		attempts    int32
		publishedAt pgtype.Timestamptz
		lockedBy    pgtype.UUID
		lockedUntil pgtype.Timestamptz
	)

	err = pool.QueryRow(ctx, `
		SELECT status, attempts, published_at, locked_by, locked_until
		FROM outbox_events
		WHERE id = $1
	`, event.ID).Scan(
		&status,
		&attempts,
		&publishedAt,
		&lockedBy,
		&lockedUntil,
	)
	if err != nil {
		t.Fatalf("scan outbox event: %v", err)
	}

	if status != "published" {
		t.Fatalf("expected status published, got: %q", status)
	}

	if attempts != 1 {
		t.Fatalf("expected 1 attempt, got: %d", attempts)
	}

	if !publishedAt.Valid {
		t.Fatal("expected published_at to be set")
	}

	if lockedBy.Valid {
		t.Fatal("expected locked_by to be NULL")
	}

	if lockedUntil.Valid {
		t.Fatal("expected locked_until to be NULL")
	}
}

func TestClaimBatchMovesPendingEventToFailed(t *testing.T) {
	pool := newTestPool(t)
	cleanOutbox(t, pool)

	ctx := context.Background()
	event := insertOutboxEvent(t, ctx, pool)

	_, err := pool.Exec(ctx, `
        UPDATE outbox_events
        SET attempts = 10
        WHERE id = $1
    `, event.ID)
	if err != nil {
		t.Fatalf("set attempts: %v", err)
	}

	publisherID := uuid.NewV7()
	repo := NewOutboxRepository(pool)

	events, err := repo.ClaimBatch(ctx, publisherID)
	if err != nil {
		t.Fatalf("claim batch: %v", err)
	}

	if len(events) != 0 {
		t.Fatalf("expected no events, got %d", len(events))
	}

	var status string
	var attempts int32
	var lockedByIsNull, lockedUntilIsNull bool

	err = pool.QueryRow(ctx, `
        SELECT status, attempts,
               locked_by IS NULL,
               locked_until IS NULL
        FROM outbox_events
        WHERE id = $1
    `, event.ID).Scan(
		&status,
		&attempts,
		&lockedByIsNull,
		&lockedUntilIsNull,
	)
	if err != nil {
		t.Fatalf("get event: %v", err)
	}

	if status != "failed" {
		t.Errorf("expected status failed, got %s", status)
	}
	if attempts != 10 {
		t.Errorf("expected attempts 10, got %d", attempts)
	}
	if !lockedByIsNull || !lockedUntilIsNull {
		t.Error("expected lease fields to be NULL")
	}
}

func TestClaimBatchMovesExpiredClaimedEventToFailed(t *testing.T) {
	pool := newTestPool(t)
	cleanOutbox(t, pool)

	ctx := context.Background()
	event := insertOutboxEvent(t, ctx, pool)

	previousPublisherID := uuid.NewV7()

	_, err := pool.Exec(ctx, `
        UPDATE outbox_events
        SET
            status = 'claimed',
            attempts = 10,
            locked_by = $2,
            locked_until = NOW() - INTERVAL '1 minute'
        WHERE id = $1
    `, event.ID, previousPublisherID)
	if err != nil {
		t.Fatalf("prepare event: %v", err)
	}

	publisherID := uuid.NewV7()
	repo := NewOutboxRepository(pool)

	events, err := repo.ClaimBatch(ctx, publisherID)
	if err != nil {
		t.Fatalf("claim batch: %v", err)
	}

	if len(events) != 0 {
		t.Fatalf("expected no events, got %d", len(events))
	}

	var status string
	var attempts int32
	var lockedByIsNull, lockedUntilIsNull bool

	err = pool.QueryRow(ctx, `
        SELECT status, attempts,
               locked_by IS NULL,
               locked_until IS NULL
        FROM outbox_events
        WHERE id = $1
    `, event.ID).Scan(
		&status,
		&attempts,
		&lockedByIsNull,
		&lockedUntilIsNull,
	)
	if err != nil {
		t.Fatalf("get event: %v", err)
	}

	if status != "failed" {
		t.Errorf("expected status failed, got %s", status)
	}
	if attempts != 10 {
		t.Errorf("expected attempts 10, got %d", attempts)
	}
	if !lockedByIsNull || !lockedUntilIsNull {
		t.Error("expected lease fields to be NULL")
	}
}

func TestClaimBatchAllowsLastAttempt(t *testing.T) {
	pool := newTestPool(t)
	cleanOutbox(t, pool)

	ctx := context.Background()
	event := insertOutboxEvent(t, ctx, pool)

	_, err := pool.Exec(ctx, `
        UPDATE outbox_events
        SET attempts = 9
        WHERE id = $1
    `, event.ID)
	if err != nil {
		t.Fatalf("set attempts: %v", err)
	}

	publisherID := uuid.NewV7()
	repo := NewOutboxRepository(pool)

	events, err := repo.ClaimBatch(ctx, publisherID)
	if err != nil {
		t.Fatalf("claim batch: %v", err)
	}

	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}

	if events[0].ID != event.ID {
		t.Errorf("expected event %s, got %s", event.ID, events[0].ID)
	}

	if events[0].Attempts != 10 {
		t.Errorf("expected attempts 10, got %d", events[0].Attempts)
	}

	var status string
	var lockedBy uuid.UUID
	var lockedUntilIsNull bool

	err = pool.QueryRow(ctx, `
        SELECT status, locked_by, locked_until IS NULL
        FROM outbox_events
        WHERE id = $1
    `, event.ID).Scan(&status, &lockedBy, &lockedUntilIsNull)
	if err != nil {
		t.Fatalf("get event: %v", err)
	}

	if status != "claimed" {
		t.Errorf("expected status claimed, got %s", status)
	}
	if lockedBy != publisherID {
		t.Errorf("expected publisher %s, got %s", publisherID, lockedBy)
	}
	if lockedUntilIsNull {
		t.Error("expected locked_until to be set")
	}
}

func TestMarkPublished(t *testing.T) {
	pool := newTestPool(t)
	cleanOutbox(t, pool)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	event := insertOutboxEvent(t, ctx, pool)
	publisherID := uuid.NewV7()

	repository := NewOutboxRepository(pool)

	events, err := repository.ClaimBatch(ctx, publisherID)
	if err != nil {
		t.Fatalf("claim batch: %v", err)
	}

	if len(events) != 1 {
		t.Fatalf("expected 1 event, got: %d", len(events))
	}

	if events[0].ID != event.ID {
		t.Fatalf("expected event ID %s, got %s", event.ID, events[0].ID)
	}

	err = repository.MarkPublished(ctx, event.ID, publisherID)
	if err != nil {
		t.Fatalf("mark published: %v", err)
	}

	query := `SELECT locked_by, locked_until, published_at FROM outbox_events WHERE id = $1`

	row := pool.QueryRow(ctx, query, event.ID)

	var lockedBy pgtype.UUID
	var lockedUntil, publishedAt pgtype.Timestamptz

	if err := row.Scan(&lockedBy, &lockedUntil, &publishedAt); err != nil {
		t.Fatalf("scan outbox event: %v", err)
	}

	if !publishedAt.Valid {
		t.Fatal("expected published_at to be set")
	}

	if lockedBy.Valid {
		t.Fatal("expected locked_by to be NULL")
	}

	if lockedUntil.Valid {
		t.Fatal("expected locked_until to be NULL")
	}

}

func TestMarkPublishedByAnotherPublisher(t *testing.T) {
	pool := newTestPool(t)
	publisherAID := uuid.NewV7()
	publisherBID := uuid.NewV7()

	cleanOutbox(t, pool)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	event := insertOutboxEvent(t, ctx, pool)

	repository := NewOutboxRepository(pool)

	events, err := repository.ClaimBatch(ctx, publisherAID)
	if err != nil {
		t.Fatalf("claim batch: %v", err)
	}

	if len(events) != 1 {
		t.Fatalf("expected 1 event, got: %d", len(events))
	}

	if events[0].ID != event.ID {
		t.Fatalf("expected event ID %s, got %s", event.ID, events[0].ID)
	}

	err = repository.MarkPublished(ctx, event.ID, publisherBID)
	if !errors.Is(err, domain.ErrLeaseLost) {
		t.Fatalf("expected %v, got: %v", domain.ErrLeaseLost, err)
	}

	query := `SELECT locked_by, locked_until, published_at FROM outbox_events WHERE id = $1`

	row := pool.QueryRow(ctx, query, event.ID)

	var lockedBy pgtype.UUID
	var lockedUntil, publishedAt pgtype.Timestamptz

	if err := row.Scan(&lockedBy, &lockedUntil, &publishedAt); err != nil {
		t.Fatalf("scan outbox event: %v", err)
	}

	if publishedAt.Valid {
		t.Fatal("expected published_at to be NULL")
	}

	if !lockedBy.Valid {
		t.Fatal("expected locked_by to be set")
	}

	if lockedBy.Bytes != publisherAID {
		t.Fatalf("expected locked_by %s, got %s", publisherAID, lockedBy.Bytes)
	}

	if !lockedUntil.Valid {
		t.Fatal("expected locked_until to be set")
	}
}

func TestReleaseClaim(t *testing.T) {
	pool := newTestPool(t)
	cleanOutbox(t, pool)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	event := insertOutboxEvent(t, ctx, pool)
	publisherAID := uuid.NewV7()

	repository := NewOutboxRepository(pool)

	events, err := repository.ClaimBatch(ctx, publisherAID)
	if err != nil {
		t.Fatalf("claim batch: %v", err)
	}

	if len(events) != 1 {
		t.Fatalf("expected 1 event, got: %d", len(events))
	}

	if events[0].ID != event.ID {
		t.Fatalf("expected event ID %s, got %s", event.ID, events[0].ID)
	}

	publishErr := errors.New("kafka unavailable")
	err = repository.ReleaseClaim(ctx, event.ID, publisherAID, publishErr)
	if err != nil {
		t.Fatalf("release claim: %v", err)
	}

	query := `SELECT locked_by, locked_until, published_at, last_error, attempts FROM outbox_events WHERE id = $1`

	row := pool.QueryRow(ctx, query, event.ID)

	var lockedBy pgtype.UUID
	var lockedUntil, publishedAt pgtype.Timestamptz
	var lastError string
	var attempts int32

	if err := row.Scan(&lockedBy, &lockedUntil, &publishedAt, &lastError, &attempts); err != nil {
		t.Fatalf("scan outbox event: %v", err)
	}

	if publishedAt.Valid {
		t.Fatal("expected published_at to be NULL")
	}

	if lockedBy.Valid {
		t.Fatal("expected locked_by to be NULL")
	}

	if lockedUntil.Valid {
		t.Fatal("expected locked_until to be NULL")
	}

	if publishErr.Error() != lastError {
		t.Fatalf("expected last_error %q, got %q", publishErr.Error(), lastError)
	}

	if attempts != 1 {
		t.Fatalf("expected 1 attempt, got: %d", attempts)
	}
}

func TestReleaseClaimAnotherPublisher(t *testing.T) {
	pool := newTestPool(t)
	cleanOutbox(t, pool)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	event := insertOutboxEvent(t, ctx, pool)
	publisherAID := uuid.NewV7()
	publisherBID := uuid.NewV7()

	repository := NewOutboxRepository(pool)

	events, err := repository.ClaimBatch(ctx, publisherAID)
	if err != nil {
		t.Fatalf("claim batch: %v", err)
	}

	if len(events) != 1 {
		t.Fatalf("expected 1 event, got: %d", len(events))
	}

	if events[0].ID != event.ID {
		t.Fatalf("expected event ID %s, got %s", event.ID, events[0].ID)
	}

	publishErr := errors.New("kafka unavailable")

	err = repository.ReleaseClaim(ctx, event.ID, publisherBID, publishErr)
	if !errors.Is(err, domain.ErrLeaseLost) {
		t.Fatalf("expected %v, got: %v", domain.ErrLeaseLost, err)
	}

	query := `SELECT locked_by, locked_until, published_at, last_error FROM outbox_events WHERE id = $1`

	row := pool.QueryRow(ctx, query, event.ID)

	var lockedBy pgtype.UUID
	var lockedUntil, publishedAt pgtype.Timestamptz
	var lastError pgtype.Text

	if err := row.Scan(&lockedBy, &lockedUntil, &publishedAt, &lastError); err != nil {
		t.Fatalf("scan outbox event: %v", err)
	}

	if publishedAt.Valid {
		t.Fatal("expected published_at to be NULL")
	}

	if !lockedBy.Valid {
		t.Fatal("expected locked_by to be set")
	}

	if !lockedUntil.Valid {
		t.Fatal("expected locked_until to be set")
	}

	if lastError.Valid {
		t.Fatalf("expected last_error to be NULL, got %q", lastError.String)
	}

	if !bytes.Equal(lockedBy.Bytes[:], publisherAID[:]) {
		t.Fatalf("expected publisher %s to own lease", publisherAID)
	}

}

func newTestPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Fatal("TEST_DATABASE_URL is not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("create pool: %v", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Fatalf("ping database: %v", err)
	}
	t.Cleanup(pool.Close)

	return pool
}

func cleanOutbox(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()

	query := `TRUNCATE TABLE outbox_events;`

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := pool.Exec(ctx, query)
	if err != nil {
		t.Fatalf("truncate outbox_events: %v", err)
	}

}

func insertOutboxEvent(t *testing.T, ctx context.Context, pool *pgxpool.Pool) domain.OutboxEvent {
	t.Helper()

	event := domain.OutboxEvent{
		ID:            uuid.NewV7(),
		AggregateType: "application",
		AggregateID:   uuid.NewV7(),
		EventType:     "ApplicationCreated",
		Payload:       json.RawMessage(`{"application_id":"test"}`),
	}

	query := `INSERT INTO outbox_events(id, aggregate_type, aggregate_id, event_type, payload) VALUES ($1, $2, $3, $4, $5);`
	_, err := pool.Exec(ctx, query, event.ID, event.AggregateType, event.AggregateID, event.EventType, event.Payload)
	if err != nil {
		t.Fatalf("insert values into outbox_events: %v", err)
	}

	return event
}
