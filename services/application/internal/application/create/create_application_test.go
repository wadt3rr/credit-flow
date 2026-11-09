package application

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"testing"
	"time"
	"uuid"

	"github.com/stretchr/testify/require"
	"github.com/wadt3rr/credit-flow/services/application/internal/application"
	"github.com/wadt3rr/credit-flow/services/application/internal/domain"
)

type fakeApplicationRepository struct {
	createFn func(context.Context, domain.Application) (domain.Application, error)
	calls    int
	created  domain.Application
}

func (f *fakeApplicationRepository) Create(
	ctx context.Context,
	app domain.Application,
) (domain.Application, error) {
	f.calls++
	f.created = app

	if f.createFn != nil {
		return f.createFn(ctx, app)
	}

	return app, nil
}

type fakeApplicationHistoryRepository struct {
	createFn func(context.Context, domain.ApplicationHistory) error
	calls    int
	created  domain.ApplicationHistory
}

func (f *fakeApplicationHistoryRepository) Create(
	ctx context.Context,
	history domain.ApplicationHistory,
) error {
	f.calls++
	f.created = history

	if f.createFn != nil {
		return f.createFn(ctx, history)
	}

	return nil
}

type fakeIdempotencyRepository struct {
	getFn            func(context.Context, uuid.UUID, string, string) (*domain.IdempotencyRecord, error)
	createIfAbsentFn func(context.Context, domain.IdempotencyRecord) (bool, error)

	getCalls    int
	createCalls int
	created     domain.IdempotencyRecord
}

func (f *fakeIdempotencyRepository) Get(
	ctx context.Context,
	subjectID uuid.UUID,
	operation string,
	key string,
) (*domain.IdempotencyRecord, error) {
	f.getCalls++

	if f.getFn != nil {
		return f.getFn(ctx, subjectID, operation, key)
	}

	return nil, nil
}

func (f *fakeIdempotencyRepository) CreateIfAbsent(
	ctx context.Context,
	record domain.IdempotencyRecord,
) (bool, error) {
	f.createCalls++
	f.created = record

	if f.createIfAbsentFn != nil {
		return f.createIfAbsentFn(ctx, record)
	}

	return true, nil
}

type fakeTransactionManager struct {
	calls      int
	committed  bool
	rolledBack bool
}

func (f *fakeTransactionManager) WithinTransaction(
	ctx context.Context,
	fn func(context.Context) error,
) error {
	f.calls++

	err := fn(ctx)
	if err != nil {
		f.rolledBack = true
		return err
	}

	f.committed = true
	return nil
}

func TestCreateApplication_Execute_Success(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Microsecond)

	input := CreateApplicationInput{
		SubjectID:      uuid.NewV7(),
		IdempotencyKey: "test-idempotency-key-01",
		CompanyID:      uuid.NewV7(),
		ProductID:      uuid.NewV7(),
		Amount:         50000000,
		Term:           12,
		Purpose:        "Working capital",
	}

	appRepo := &fakeApplicationRepository{
		createFn: func(
			_ context.Context,
			app domain.Application,
		) (domain.Application, error) {
			app.CreatedAt = now
			app.UpdatedAt = now
			return app, nil
		},
	}

	historyRepo := &fakeApplicationHistoryRepository{}
	idempotencyRepo := &fakeIdempotencyRepository{}
	txManager := &fakeTransactionManager{}

	useCase := NewCreateApplication(
		appRepo,
		historyRepo,
		idempotencyRepo,
		txManager,
	)

	output, err := useCase.Execute(context.Background(), input)

	require.NoError(t, err)
	require.NotNil(t, output)

	// HTTP response.
	require.Equal(t, http.StatusCreated, output.StatusCode)
	require.NotEmpty(t, output.Body)

	// Location header.
	require.Equal(
		t,
		"/applications/"+appRepo.created.ID.String(),
		output.Headers["Location"],
	)

	var response applicationResponse
	require.NoError(t, json.Unmarshal(output.Body, &response))

	require.Equal(t, appRepo.created.ID, response.ID)
	require.Equal(t, input.CompanyID, response.CompanyID)
	require.Equal(t, input.ProductID, response.ProductID)
	require.Equal(t, input.Amount, response.Amount)
	require.Equal(t, input.Term, response.Term)
	require.Equal(t, input.Purpose, response.Purpose)
	require.Equal(t, domain.ApplicationStatusDraft, response.Status)
	require.Equal(t, now, response.CreatedAt)
	require.Equal(t, now, response.UpdatedAt)

	// Application.
	require.Equal(t, 1, appRepo.calls)
	require.Equal(t, domain.ApplicationStatusDraft, appRepo.created.Status)

	// History.
	require.Equal(t, 1, historyRepo.calls)
	require.Equal(t, appRepo.created.ID, historyRepo.created.ApplicationID)
	require.Nil(t, historyRepo.created.PrevStatus)
	require.Equal(t, domain.ApplicationStatusDraft, historyRepo.created.NewStatus)
	require.Equal(t, domain.ActorTypeUser, historyRepo.created.ActorType)
	require.NotNil(t, historyRepo.created.ActorID)
	require.Equal(t, input.SubjectID, *historyRepo.created.ActorID)

	// Idempotency.
	require.Equal(t, 1, idempotencyRepo.createCalls)
	require.Equal(t, input.SubjectID, idempotencyRepo.created.SubjectID)
	require.Equal(t, createApplicationOperation, idempotencyRepo.created.Operation)
	require.Equal(t, input.IdempotencyKey, idempotencyRepo.created.IdempotencyKey)
	require.Equal(t, appRepo.created.ID, idempotencyRepo.created.ResourceID)
	require.NotEmpty(t, idempotencyRepo.created.RequestHash)
	require.NotNil(t, idempotencyRepo.created.ResponseStatus)
	require.Equal(t, http.StatusCreated, *idempotencyRepo.created.ResponseStatus)
	require.NotNil(t, idempotencyRepo.created.ResponseBody)
	require.NotNil(t, idempotencyRepo.created.ResponseHeaders)

	// Transaction.
	require.Equal(t, 1, txManager.calls)
	require.True(t, txManager.committed)
	require.False(t, txManager.rolledBack)
}

func TestCreateApplication_Execute_ReturnsCachedResponse(t *testing.T) {
	input := CreateApplicationInput{
		SubjectID:      uuid.NewV7(),
		IdempotencyKey: "test-idempotency-key-01",
		CompanyID:      uuid.NewV7(),
		ProductID:      uuid.NewV7(),
		Amount:         50000000,
		Term:           12,
		Purpose:        "Working capital",
	}

	reqHash, err := hashCreateApplicationRequest(input)
	require.NoError(t, err)

	applicationID := uuid.NewV7()
	now := time.Now().UTC().Truncate(time.Microsecond)

	cachedResponse := applicationResponse{
		ID:        applicationID,
		CompanyID: input.CompanyID,
		ProductID: input.ProductID,
		Amount:    input.Amount,
		Term:      input.Term,
		Purpose:   input.Purpose,
		Status:    domain.ApplicationStatusDraft,
		CreatedAt: now,
		UpdatedAt: now,
	}

	responseHeadersJSON, err := json.Marshal(map[string]string{
		"Location": "/applications/" + applicationID.String(),
	})
	require.NoError(t, err)

	responseHeaders := json.RawMessage(responseHeadersJSON)

	responseBodyJSON, err := json.Marshal(cachedResponse)
	require.NoError(t, err)

	responseBody := json.RawMessage(responseBodyJSON)

	responseStatus := http.StatusCreated

	record := &domain.IdempotencyRecord{
		SubjectID:       input.SubjectID,
		Operation:       createApplicationOperation,
		IdempotencyKey:  input.IdempotencyKey,
		RequestHash:     reqHash,
		ResourceID:      applicationID,
		ExpiresAt:       time.Now().Add(time.Hour),
		ResponseStatus:  &responseStatus,
		ResponseHeaders: &responseHeaders,
		ResponseBody:    &responseBody,
	}

	appRepo := &fakeApplicationRepository{}
	historyRepo := &fakeApplicationHistoryRepository{}
	idempotencyRepo := &fakeIdempotencyRepository{
		getFn: func(
			_ context.Context,
			subjectID uuid.UUID,
			operation string,
			key string,
		) (*domain.IdempotencyRecord, error) {
			require.Equal(t, input.SubjectID, subjectID)
			require.Equal(t, createApplicationOperation, operation)
			require.Equal(t, input.IdempotencyKey, key)

			return record, nil
		},
	}
	txManager := &fakeTransactionManager{}

	useCase := NewCreateApplication(
		appRepo,
		historyRepo,
		idempotencyRepo,
		txManager,
	)

	output, err := useCase.Execute(context.Background(), input)

	require.NoError(t, err)
	require.NotNil(t, output)

	require.Equal(t, http.StatusCreated, output.StatusCode)
	require.Equal(t, map[string]string{
		"Location": "/applications/" + applicationID.String(),
	}, output.Headers)
	require.JSONEq(t, string(responseBody), string(output.Body))

	require.Equal(t, 1, idempotencyRepo.getCalls)
	require.Equal(t, 0, idempotencyRepo.createCalls)

	require.Equal(t, 0, appRepo.calls)
	require.Equal(t, 0, historyRepo.calls)
	require.Equal(t, 0, txManager.calls)

	require.False(t, txManager.committed)
	require.False(t, txManager.rolledBack)
}

func TestCreateApplication_Execute_IdempotencyKeyConflict(t *testing.T) {
	input := CreateApplicationInput{
		SubjectID:      uuid.NewV7(),
		IdempotencyKey: "test-idempotency-key-01",
		CompanyID:      uuid.NewV7(),
		ProductID:      uuid.NewV7(),
		Amount:         50000000,
		Term:           12,
		Purpose:        "Working capital",
	}

	record := &domain.IdempotencyRecord{
		RequestHash: "different-request-hash",
		ExpiresAt:   time.Now().Add(time.Hour),
	}

	appRepo := &fakeApplicationRepository{}
	historyRepo := &fakeApplicationHistoryRepository{}
	idempotencyRepo := &fakeIdempotencyRepository{
		getFn: func(
			_ context.Context,
			_ uuid.UUID,
			_ string,
			_ string,
		) (*domain.IdempotencyRecord, error) {
			return record, nil
		},
	}
	txManager := &fakeTransactionManager{}

	useCase := NewCreateApplication(
		appRepo,
		historyRepo,
		idempotencyRepo,
		txManager,
	)

	output, err := useCase.Execute(context.Background(), input)

	require.ErrorIs(t, err, application.ErrIdempotencyKeyConflict)
	require.Nil(t, output)

	require.Equal(t, 1, idempotencyRepo.getCalls)
	require.Equal(t, 0, idempotencyRepo.createCalls)
	require.Equal(t, 0, appRepo.calls)
	require.Equal(t, 0, historyRepo.calls)
	require.Equal(t, 0, txManager.calls)
}

func TestCreateApplication_Execute_ApplicationCreationError(t *testing.T) {
	input := CreateApplicationInput{
		SubjectID:      uuid.NewV7(),
		IdempotencyKey: "test-idempotency-key-01",
		CompanyID:      uuid.NewV7(),
		ProductID:      uuid.NewV7(),
		Amount:         50000000,
		Term:           12,
		Purpose:        "Working capital",
	}

	expectedErr := errors.New("database error")

	appRepo := &fakeApplicationRepository{
		createFn: func(
			_ context.Context,
			_ domain.Application,
		) (domain.Application, error) {
			return domain.Application{}, expectedErr
		},
	}

	historyRepo := &fakeApplicationHistoryRepository{}
	idempotencyRepo := &fakeIdempotencyRepository{}
	txManager := &fakeTransactionManager{}

	useCase := NewCreateApplication(
		appRepo,
		historyRepo,
		idempotencyRepo,
		txManager,
	)

	output, err := useCase.Execute(context.Background(), input)

	require.ErrorIs(t, err, expectedErr)
	require.Nil(t, output)

	require.Equal(t, 1, appRepo.calls)
	require.Equal(t, 0, historyRepo.calls)
	require.Equal(t, 0, idempotencyRepo.createCalls)

	require.Equal(t, 1, txManager.calls)
	require.False(t, txManager.committed)
	require.True(t, txManager.rolledBack)
}

func TestCreateApplication_Execute_HistoryCreationError(t *testing.T) {
	input := CreateApplicationInput{
		SubjectID:      uuid.NewV7(),
		IdempotencyKey: "test-idempotency-key-01",
		CompanyID:      uuid.NewV7(),
		ProductID:      uuid.NewV7(),
		Amount:         50000000,
		Term:           12,
		Purpose:        "Working capital",
	}

	expectedErr := errors.New("history database error")

	appRepo := &fakeApplicationRepository{}
	historyRepo := &fakeApplicationHistoryRepository{
		createFn: func(
			_ context.Context,
			_ domain.ApplicationHistory,
		) error {
			return expectedErr
		},
	}
	idempotencyRepo := &fakeIdempotencyRepository{}
	txManager := &fakeTransactionManager{}

	useCase := NewCreateApplication(
		appRepo,
		historyRepo,
		idempotencyRepo,
		txManager,
	)

	output, err := useCase.Execute(context.Background(), input)

	require.ErrorIs(t, err, expectedErr)
	require.Nil(t, output)

	require.Equal(t, 1, appRepo.calls)
	require.Equal(t, 1, historyRepo.calls)
	require.Equal(t, 0, idempotencyRepo.createCalls)

	require.Equal(t, 1, txManager.calls)
	require.False(t, txManager.committed)
	require.True(t, txManager.rolledBack)
}

func TestCreateApplication_Execute_IdempotencyRecordCreationError(t *testing.T) {
	input := CreateApplicationInput{
		SubjectID:      uuid.NewV7(),
		IdempotencyKey: "test-idempotency-key-01",
		CompanyID:      uuid.NewV7(),
		ProductID:      uuid.NewV7(),
		Amount:         50000000,
		Term:           12,
		Purpose:        "Working capital",
	}

	expectedErr := errors.New("idempotency database error")

	appRepo := &fakeApplicationRepository{}
	historyRepo := &fakeApplicationHistoryRepository{}
	idempotencyRepo := &fakeIdempotencyRepository{
		createIfAbsentFn: func(
			_ context.Context,
			_ domain.IdempotencyRecord,
		) (bool, error) {
			return false, expectedErr
		},
	}
	txManager := &fakeTransactionManager{}

	useCase := NewCreateApplication(
		appRepo,
		historyRepo,
		idempotencyRepo,
		txManager,
	)

	output, err := useCase.Execute(context.Background(), input)

	require.ErrorIs(t, err, expectedErr)
	require.Nil(t, output)

	require.Equal(t, 1, appRepo.calls)
	require.Equal(t, 1, historyRepo.calls)
	require.Equal(t, 1, idempotencyRepo.createCalls)

	require.Equal(t, 1, txManager.calls)
	require.False(t, txManager.committed)
	require.True(t, txManager.rolledBack)
}

func TestCreateApplication_Execute_ReturnsConcurrentRequestResponse(t *testing.T) {
	input := CreateApplicationInput{
		SubjectID:      uuid.NewV7(),
		IdempotencyKey: "test-idempotency-key-01",
		CompanyID:      uuid.NewV7(),
		ProductID:      uuid.NewV7(),
		Amount:         50000000,
		Term:           12,
		Purpose:        "Working capital",
	}

	reqHash, err := hashCreateApplicationRequest(input)
	require.NoError(t, err)

	winnerApplicationID := uuid.NewV7()
	now := time.Now().UTC().Truncate(time.Microsecond)

	cachedResponse := applicationResponse{
		ID:        winnerApplicationID,
		CompanyID: input.CompanyID,
		ProductID: input.ProductID,
		Amount:    input.Amount,
		Term:      input.Term,
		Purpose:   input.Purpose,
		Status:    domain.ApplicationStatusDraft,
		CreatedAt: now,
		UpdatedAt: now,
	}

	responseBodyJSON, err := json.Marshal(cachedResponse)
	require.NoError(t, err)

	responseBody := json.RawMessage(responseBodyJSON)

	responseHeadersJSON, err := json.Marshal(map[string]string{
		"Location": "/applications/" + winnerApplicationID.String(),
	})
	require.NoError(t, err)

	responseHeaders := json.RawMessage(responseHeadersJSON)

	responseStatus := http.StatusCreated

	winnerRecord := &domain.IdempotencyRecord{
		SubjectID:       input.SubjectID,
		Operation:       createApplicationOperation,
		IdempotencyKey:  input.IdempotencyKey,
		RequestHash:     reqHash,
		ResourceID:      winnerApplicationID,
		ExpiresAt:       time.Now().Add(time.Hour),
		ResponseStatus:  &responseStatus,
		ResponseHeaders: &responseHeaders,
		ResponseBody:    &responseBody,
	}

	getCount := 0

	appRepo := &fakeApplicationRepository{}
	historyRepo := &fakeApplicationHistoryRepository{}
	idempotencyRepo := &fakeIdempotencyRepository{
		getFn: func(
			_ context.Context,
			_ uuid.UUID,
			_ string,
			_ string,
		) (*domain.IdempotencyRecord, error) {
			getCount++

			if getCount == 1 {
				return nil, nil
			}

			return winnerRecord, nil
		},
		createIfAbsentFn: func(
			_ context.Context,
			_ domain.IdempotencyRecord,
		) (bool, error) {
			return false, nil
		},
	}

	txManager := &fakeTransactionManager{}

	useCase := NewCreateApplication(
		appRepo,
		historyRepo,
		idempotencyRepo,
		txManager,
	)

	output, err := useCase.Execute(context.Background(), input)

	require.NoError(t, err)
	require.NotNil(t, output)

	require.Equal(t, http.StatusCreated, output.StatusCode)
	require.Equal(t, map[string]string{
		"Location": "/applications/" + winnerApplicationID.String(),
	}, output.Headers)
	require.JSONEq(t, string(responseBody), string(output.Body))

	require.Equal(t, 2, idempotencyRepo.getCalls)
	require.Equal(t, 1, idempotencyRepo.createCalls)

	require.Equal(t, 1, appRepo.calls)
	require.Equal(t, 1, historyRepo.calls)

	require.Equal(t, 1, txManager.calls)
	require.False(t, txManager.committed)
	require.True(t, txManager.rolledBack)
}
