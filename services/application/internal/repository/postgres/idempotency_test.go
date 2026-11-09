package postgres

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"uuid"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/wadt3rr/credit-flow/services/application/internal/domain"
)

func newIdempotencyTestRepository(t *testing.T) (*IdempotencyRecordsRepository, *pgxpool.Pool) {
	t.Helper()

	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
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

	return NewIdempotencyRecordsRepository(pool), pool
}

func insertTestApplication(t *testing.T, pool *pgxpool.Pool) uuid.UUID {
	t.Helper()

	id := uuid.New()

	_, err := pool.Exec(
		context.Background(),
		`
		INSERT INTO applications (
			id, company_id, product_id, amount, term, purpose, status
		) VALUES ($1, $2, $3, $4, $5, $6, $7)
		`,
		id,
		uuid.New(),
		uuid.New(),
		int64(500000),
		12,
		"Test application",
		"DRAFT",
	)
	if err != nil {
		t.Fatalf("insert test application: %v", err)
	}

	t.Cleanup(func() {
		_, err := pool.Exec(
			context.Background(),
			`DELETE FROM applications WHERE id = $1`,
			id,
		)
		if err != nil {
			t.Errorf("cleanup test application: %v", err)
		}
	})

	return id
}

func TestIdempotencyRecordsRepository_Get_NotFound(t *testing.T) {
	repo, _ := newIdempotencyTestRepository(t)

	record, err := repo.Get(
		context.Background(),
		uuid.New(),
		"create_application_draft",
		"test-idempotency-key-1",
	)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if record != nil {
		t.Fatalf("expected nil record, got %+v", record)
	}
}

func TestIdempotencyRecordsRepository_CreateAndGet_WithResponse(t *testing.T) {
	repo, pool := newIdempotencyTestRepository(t)

	ctx := context.Background()
	applicationID := insertTestApplication(t, pool)

	recordID := uuid.New()
	subjectID := uuid.New()
	idempotencyKey := "test-idempotency-key-2"

	status := 201
	body := json.RawMessage(`{"id":"test-id","status":"DRAFT"}`)
	headers := json.RawMessage(`{"Location":"/applications/test-id"}`)

	record := domain.IdempotencyRecord{
		ID:              recordID,
		SubjectID:       subjectID,
		Operation:       "create_application_draft",
		ResourceID:      applicationID,
		IdempotencyKey:  idempotencyKey,
		RequestHash:     "test-request-hash",
		ResponseStatus:  &status,
		ResponseBody:    &body,
		ResponseHeaders: &headers,
		ExpiresAt:       time.Now().Add(24 * time.Hour),
	}

	created, err := repo.CreateIfAbsent(ctx, record)
	if err != nil {
		t.Fatalf("CreateIfAbsent: %v", err)
	}
	if !created {
		t.Fatal("expected record to be created")
	}

	t.Cleanup(func() {
		_, err := pool.Exec(ctx,
			`DELETE FROM idempotency_records WHERE id = $1`,
			recordID,
		)
		if err != nil {
			t.Errorf("cleanup idempotency record: %v", err)
		}
	})

	got, err := repo.Get(ctx, subjectID, record.Operation, idempotencyKey)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if got == nil {
		t.Fatal("expected record, got nil")
	}

	if got.ID != recordID {
		t.Errorf("ID: got %v, want %v", got.ID, recordID)
	}

	if got.ResourceID != applicationID {
		t.Errorf("ResourceID: got %v, want %v", got.ResourceID, applicationID)
	}

	if got.RequestHash != record.RequestHash {
		t.Errorf("RequestHash: got %q, want %q", got.RequestHash, record.RequestHash)
	}

	if got.ResponseStatus == nil || *got.ResponseStatus != status {
		t.Errorf("ResponseStatus: got %v, want %d", got.ResponseStatus, status)
	}

	if got.ResponseBody == nil {
		t.Fatal("ResponseBody is nil")
	}
	require.JSONEq(t, string(body), string(*got.ResponseBody))

	if got.ResponseHeaders == nil {
		t.Fatal("ResponseHeaders is nil")
	}
	require.JSONEq(t, string(headers), string(*got.ResponseHeaders))

	if got.CreatedAt.IsZero() {
		t.Error("CreatedAt must be populated by database")
	}

	if !got.ExpiresAt.Equal(record.ExpiresAt.Truncate(time.Microsecond)) {
		t.Errorf(
			"ExpiresAt: got %v, want %v",
			got.ExpiresAt,
			record.ExpiresAt.Truncate(time.Microsecond),
		)
	}
}

func TestIdempotencyRecordsRepository_CreateAndGet_NullResponse(t *testing.T) {
	repo, pool := newIdempotencyTestRepository(t)

	ctx := context.Background()
	applicationID := insertTestApplication(t, pool)

	recordID := uuid.New()
	subjectID := uuid.New()

	record := domain.IdempotencyRecord{
		ID:             recordID,
		SubjectID:      subjectID,
		Operation:      "create_application_draft",
		ResourceID:     applicationID,
		IdempotencyKey: "test-idempotency-key-3",
		RequestHash:    "test-request-hash",
		ExpiresAt:      time.Now().Add(24 * time.Hour),
	}

	created, err := repo.CreateIfAbsent(ctx, record)
	if err != nil {
		t.Fatalf("CreateIfAbsent: %v", err)
	}
	if !created {
		t.Fatal("expected record to be created")
	}

	t.Cleanup(func() {
		_, err := pool.Exec(ctx,
			`DELETE FROM idempotency_records WHERE id = $1`,
			recordID,
		)
		if err != nil {
			t.Errorf("cleanup idempotency record: %v", err)
		}
	})

	got, err := repo.Get(ctx, subjectID, record.Operation, record.IdempotencyKey)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if got == nil {
		t.Fatal("expected record, got nil")
	}

	if got.ResponseStatus != nil {
		t.Errorf("expected nil ResponseStatus, got %v", *got.ResponseStatus)
	}

	if got.ResponseBody != nil {
		t.Errorf("expected nil ResponseBody, got %s", *got.ResponseBody)
	}

	if got.ResponseHeaders != nil {
		t.Errorf("expected nil ResponseHeaders, got %s", *got.ResponseHeaders)
	}
}

func TestIdempotencyRecordsRepository_CreateIfAbsent_AlreadyExists(t *testing.T) {
	repo, pool := newIdempotencyTestRepository(t)

	ctx := context.Background()
	applicationID := insertTestApplication(t, pool)

	subjectID := uuid.New()
	operation := "create_application"
	idempotencyKey := "test-idempotency-key-4"

	first := domain.IdempotencyRecord{
		ID:             uuid.New(),
		SubjectID:      subjectID,
		Operation:      operation,
		ResourceID:     applicationID,
		IdempotencyKey: idempotencyKey,
		RequestHash:    "first-request-hash",
		ExpiresAt:      time.Now().Add(24 * time.Hour),
	}

	created, err := repo.CreateIfAbsent(ctx, first)
	if err != nil {
		t.Fatalf("first CreateIfAbsent: %v", err)
	}
	if !created {
		t.Fatal("expected first record to be created")
	}

	t.Cleanup(func() {
		_, err := pool.Exec(ctx,
			`DELETE FROM idempotency_records WHERE id = $1`,
			first.ID,
		)
		if err != nil {
			t.Errorf("cleanup idempotency record: %v", err)
		}
	})

	second := domain.IdempotencyRecord{
		ID:             uuid.New(),
		SubjectID:      subjectID,
		Operation:      operation,
		ResourceID:     applicationID,
		IdempotencyKey: idempotencyKey,
		RequestHash:    "second-request-hash",
		ExpiresAt:      time.Now().Add(24 * time.Hour),
	}

	created, err = repo.CreateIfAbsent(ctx, second)
	if err != nil {
		t.Fatalf("second CreateIfAbsent: %v", err)
	}
	if created {
		t.Fatal("expected second record not to be created")
	}

	got, err := repo.Get(ctx, subjectID, operation, idempotencyKey)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if got == nil {
		t.Fatal("expected existing record, got nil")
	}

	if got.ID != first.ID {
		t.Errorf("ID: got %v, want %v", got.ID, first.ID)
	}

	if got.RequestHash != first.RequestHash {
		t.Errorf("RequestHash: got %q, want %q", got.RequestHash, first.RequestHash)
	}
}
