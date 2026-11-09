package domain

import (
	"time"
	"uuid"
)

type ActorType string

const (
	ActorTypeUser    ActorType = "USER"
	ActorTypeAdmin   ActorType = "ADMIN"
	ActorTypeService ActorType = "SERVICE"
)

type ApplicationHistory struct {
	ID            uuid.UUID
	ApplicationID uuid.UUID
	PrevStatus    *ApplicationStatus
	NewStatus     ApplicationStatus
	ActorType     ActorType
	ActorID       *uuid.UUID
	Reason        *string
	CreatedAt     time.Time
}
