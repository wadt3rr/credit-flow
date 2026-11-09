package domain

import (
	"encoding/json"
	"time"
	"uuid"
)

type IdempotencyRecord struct {
	ID             uuid.UUID
	SubjectID      uuid.UUID
	Operation      string
	ResourceID     uuid.UUID
	IdempotencyKey string
	RequestHash    string

	ResponseStatus  *int
	ResponseBody    *json.RawMessage
	ResponseHeaders *json.RawMessage

	CreatedAt time.Time
	ExpiresAt time.Time
}
