package application

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"
	"unicode/utf8"
	"uuid"

	"github.com/wadt3rr/credit-flow/services/application/internal/domain"
)

type CreateApplicationInput struct {
	SubjectID      uuid.UUID
	IdempotencyKey string

	CompanyID uuid.UUID
	ProductID uuid.UUID
	Amount    int64
	Term      int
	Purpose   string
}

type CreateApplicationOutput struct {
	StatusCode int
	Headers    map[string]string
	Body       json.RawMessage
}

type applicationResponse struct {
	ID        uuid.UUID                `json:"id"`
	CompanyID uuid.UUID                `json:"companyId"`
	ProductID uuid.UUID                `json:"productId"`
	Amount    int64                    `json:"amount"`
	Term      int                      `json:"term"`
	Purpose   string                   `json:"purpose"`
	Status    domain.ApplicationStatus `json:"status"`
	CreatedAt time.Time                `json:"createdAt"`
	UpdatedAt time.Time                `json:"updatedAt"`
}

type IdempotencyRepository interface {
	Get(
		ctx context.Context,
		subjectID uuid.UUID,
		operation string,
		idempotencyKey string,
	) (*domain.IdempotencyRecord, error)

	CreateIfAbsent(
		ctx context.Context,
		record domain.IdempotencyRecord,
	) (bool, error)
}

type TransactionManager interface {
	WithinTransaction(
		ctx context.Context,
		fn func(ctx context.Context) error,
	) error
}

type ApplicationRepository interface {
	Create(ctx context.Context, app domain.Application) (domain.Application, error)
}

type ApplicationHistoryRepository interface {
	Create(ctx context.Context, history domain.ApplicationHistory) error
}

type CreateApplication struct {
	applications ApplicationRepository
	history      ApplicationHistoryRepository
	idempotency  IdempotencyRepository
	txManager    TransactionManager
}

func NewCreateApplication(
	applications ApplicationRepository,
	history ApplicationHistoryRepository,
	idempotency IdempotencyRepository,
	txManager TransactionManager,
) *CreateApplication {
	return &CreateApplication{
		applications: applications,
		history:      history,
		idempotency:  idempotency,
		txManager:    txManager,
	}
}

func (c *CreateApplication) Execute(ctx context.Context, input CreateApplicationInput) (*CreateApplicationOutput, error) {
	if err := validateCreateApplicationInput(input); err != nil {
		return nil, fmt.Errorf("validate input: %w", err)
	}

	reqHash, err := hashCreateApplicationRequest(input)
	if err != nil {
		return nil, fmt.Errorf("hash input: %w", err)
	}

	resp, err := c.findIdempotentResponse(ctx, input, reqHash)
	if err != nil {
		return nil, fmt.Errorf("find idempotent response: %w", err)
	}

	if resp != nil {
		return resp, nil
	}

	now := time.Now().UTC()

	appID := uuid.NewV7()
	historyID := uuid.NewV7()
	idempotencyID := uuid.NewV7()

	expiresAt := now.Add(24 * time.Hour)

	subjectID := input.SubjectID

	app := domain.Application{
		ID:        appID,
		CompanyID: input.CompanyID,
		ProductID: input.ProductID,
		Amount:    input.Amount,
		Term:      input.Term,
		Purpose:   input.Purpose,
		Status:    domain.ApplicationStatusDraft,
	}

	history := domain.ApplicationHistory{
		ID:            historyID,
		ApplicationID: appID,
		PrevStatus:    nil,
		NewStatus:     domain.ApplicationStatusDraft,
		ActorType:     domain.ActorTypeUser,
		ActorID:       &subjectID,
	}

	idempotency := domain.IdempotencyRecord{
		ID:             idempotencyID,
		SubjectID:      input.SubjectID,
		Operation:      createApplicationOperation,
		ResourceID:     appID,
		IdempotencyKey: input.IdempotencyKey,
		RequestHash:    reqHash,
		ExpiresAt:      expiresAt,
	}

	var output CreateApplicationOutput

	err = c.txManager.WithinTransaction(ctx, func(txCtx context.Context) error {
		createdApp, err := c.applications.Create(txCtx, app)
		if err != nil {
			return fmt.Errorf("create application: %w", err)
		}

		if err := c.history.Create(txCtx, history); err != nil {
			return fmt.Errorf("create application history: %w", err)
		}

		response := applicationResponse{
			ID:        createdApp.ID,
			CompanyID: createdApp.CompanyID,
			ProductID: createdApp.ProductID,
			Amount:    createdApp.Amount,
			Term:      createdApp.Term,
			Purpose:   createdApp.Purpose,
			Status:    createdApp.Status,
			CreatedAt: createdApp.CreatedAt,
			UpdatedAt: createdApp.UpdatedAt,
		}

		body, err := json.Marshal(response)
		if err != nil {
			return fmt.Errorf("marshal application response: %w", err)
		}

		headers := map[string]string{
			"Location": "/applications/" + createdApp.ID.String(),
		}

		headersJSON, err := json.Marshal(headers)
		if err != nil {
			return fmt.Errorf("marshal response headers: %w", err)
		}

		statusCode := http.StatusCreated
		responseBody := json.RawMessage(body)
		responseHeaders := json.RawMessage(headersJSON)

		idempotency.ResponseStatus = &statusCode
		idempotency.ResponseBody = &responseBody
		idempotency.ResponseHeaders = &responseHeaders

		created, err := c.idempotency.CreateIfAbsent(txCtx, idempotency)
		if err != nil {
			return fmt.Errorf("create idempotency record: %w", err)
		}

		if !created {
			return ErrIdempotencyRecordAlreadyExists
		}

		output = CreateApplicationOutput{
			StatusCode: statusCode,
			Headers:    headers,
			Body:       body,
		}

		return nil
	})
	if err != nil {
		if errors.Is(err, ErrIdempotencyRecordAlreadyExists) {
			resp, lookupErr := c.findIdempotentResponse(ctx, input, reqHash)
			if lookupErr != nil {
				return nil, fmt.Errorf("find concurrent idempotent response: %w", lookupErr)
			}

			if resp == nil {
				return nil, errors.New("idempotency record not found after conflict")
			}

			return resp, nil
		}

		return nil, fmt.Errorf("create application transaction: %w", err)
	}

	return &output, nil

}

const createApplicationOperation = "create_application"

func validateCreateApplicationInput(input CreateApplicationInput) error {
	switch {
	case input.SubjectID == uuid.Nil():
		return errors.New("subject ID is required")

	case strings.TrimSpace(input.IdempotencyKey) == "":
		return errors.New("idempotency key is required")

	case input.CompanyID == uuid.Nil():
		return errors.New("company ID is required")

	case input.ProductID == uuid.Nil():
		return errors.New("product ID is required")

	case input.Amount <= 0:
		return errors.New("amount must be positive")

	case input.Term <= 0:
		return errors.New("term must be positive")

	case strings.TrimSpace(input.Purpose) == "":
		return errors.New("purpose is required")

	case utf8.RuneCountInString(input.Purpose) > 1000:
		return errors.New("purpose must not exceed 1000 characters")
	}

	keyLength := utf8.RuneCountInString(input.IdempotencyKey)
	if keyLength < 16 || keyLength > 256 {
		return errors.New("idempotency key must be between 16 and 256 characters")
	}

	return nil
}

func hashCreateApplicationRequest(input CreateApplicationInput) (string, error) {
	payload := struct {
		CompanyID uuid.UUID
		ProductID uuid.UUID
		Amount    int64
		Term      int
		Purpose   string
	}{
		CompanyID: input.CompanyID,
		ProductID: input.ProductID,
		Amount:    input.Amount,
		Term:      input.Term,
		Purpose:   input.Purpose,
	}

	data, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("marshal request: %w", err)
	}

	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

func (c *CreateApplication) findIdempotentResponse(
	ctx context.Context,
	input CreateApplicationInput,
	requestHash string,
) (*CreateApplicationOutput, error) {

	record, err := c.idempotency.Get(ctx, input.SubjectID, createApplicationOperation, input.IdempotencyKey)
	if err != nil {
		return nil, fmt.Errorf(
			"get idempotency record: %w",
			err,
		)
	}

	if record == nil {
		return nil, nil
	}

	if record.RequestHash != requestHash {
		return nil, ErrIdempotencyKeyConflict
	}

	if !time.Now().Before(record.ExpiresAt) {
		return nil, ErrIdempotencyResponseExpired
	}

	if record.ResponseStatus == nil {
		return nil, ErrIdempotencyResponseExpired
	}

	if record.ResponseHeaders == nil {
		return nil, ErrIdempotencyResponseExpired
	}

	if record.ResponseBody == nil {
		return nil, ErrIdempotencyResponseExpired
	}

	var headers map[string]string

	if err := json.Unmarshal(*record.ResponseHeaders, &headers); err != nil {
		return nil, fmt.Errorf("unmarshal idempotency response headers: %w", err)
	}

	if headers == nil {
		return nil, errors.New("idempotency response headers are empty")
	}

	return &CreateApplicationOutput{
		StatusCode: *record.ResponseStatus,
		Headers:    headers,
		Body:       *record.ResponseBody,
	}, nil
}
