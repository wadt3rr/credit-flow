-- +goose Up
CREATE TABLE application_history (
    id UUID PRIMARY KEY,
    application_id UUID NOT NULL REFERENCES applications(id) ON DELETE RESTRICT,
    prev_status VARCHAR(32) CHECK (prev_status IN ('DRAFT', 'SUBMITTED', 'CANCELLED', 'VERIFYING', 'UNDER_REVIEW', 'REJECTED', 'APPROVED' )),
    new_status VARCHAR(32) NOT NULL CHECK (new_status IN ('DRAFT', 'SUBMITTED', 'CANCELLED', 'VERIFYING', 'UNDER_REVIEW', 'REJECTED', 'APPROVED' )),
    actor_type VARCHAR(16) NOT NULL CHECK (actor_type IN ('USER', 'ADMIN', 'SERVICE')),
    actor_id UUID CHECK (
        (actor_type IN ('USER', 'ADMIN') AND actor_id IS NOT NULL)
        OR
        (actor_type = 'SERVICE')
    ),
    reason TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
-- +goose Down
DROP TABLE IF EXISTS application_history;
