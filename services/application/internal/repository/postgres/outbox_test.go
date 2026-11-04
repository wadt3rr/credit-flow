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
