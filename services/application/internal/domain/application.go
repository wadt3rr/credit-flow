package domain

import (
	"time"
	"uuid"
)

type Application struct {
	ID              uuid.UUID
	CompanyID       uuid.UUID
	ProductID       uuid.UUID
	Amount          int64
	Term            int
	Purpose         string
	Status          ApplicationStatus
	Version         int
	ProductSnapshot *ProductSnapshot
	SubmittedAt     *time.Time
	CreatedAt       time.Time
	UpdatedAt       time.Time
}

type ApplicationStatus string

const (
	ApplicationStatusDraft       ApplicationStatus = "DRAFT"
	ApplicationStatusVerifying   ApplicationStatus = "VERIFYING"
	ApplicationStatusSubmitted   ApplicationStatus = "SUBMITTED"
	ApplicationStatusUnderReview ApplicationStatus = "UNDER_REVIEW"
	ApplicationStatusApproved    ApplicationStatus = "APPROVED"
	ApplicationStatusRejected    ApplicationStatus = "REJECTED"
	ApplicationStatusCancelled   ApplicationStatus = "CANCELLED"
)

type ProductSnapshot struct {
	ProductTitle    string
	InterestRateBps int64 // процент 13,5 хранится как 1350
	MinimalSum      int64
	MaximumSum      int64
}