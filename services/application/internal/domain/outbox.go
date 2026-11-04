package domain

import (
	"encoding/json"
	"errors"
	"uuid"
)

var ErrLeaseLost = errors.New("outbox event lease lost")

type OutboxEvent struct {
	ID            uuid.UUID
	AggregateType string
	AggregateID   uuid.UUID
	EventType     string
	Payload       json.RawMessage
	Attempts      int32
}
