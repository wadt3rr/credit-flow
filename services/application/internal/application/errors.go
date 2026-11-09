package application

import "errors"

var (
	ErrIdempotencyKeyConflict         = errors.New("idempotency key conflict")
	ErrIdempotencyResponseExpired     = errors.New("idempotency response expired")
	ErrIdempotencyRecordAlreadyExists = errors.New("idempotency record already exists")

	ErrApplicationNotSubmitted = errors.New("application has not been submitted")
	ErrNilProductSnapshot      = errors.New("product snapshot wasn't provided")
)
