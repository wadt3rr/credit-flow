-- +goose Up
CREATE TABLE idempotency_records (
    id UUID PRIMARY KEY,
    subject_id UUID NOT NULL,
    operation TEXT NOT NULL,
    resource_id UUID NOT NULL REFERENCES applications(id),
    idempotency_key TEXT NOT NULL,
    request_hash TEXT NOT NULL,
    response_status INTEGER,
    response_body JSONB,
    response_headers JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ NOT NULL CHECK (expires_at > created_at),

    CONSTRAINT uq_idempotency_subject_operation_key
        UNIQUE (subject_id, operation, idempotency_key),
    CONSTRAINT chk_response_complete CHECK (
        (
            response_status IS NULL
            AND response_body IS NULL
            AND response_headers IS NULL
        )
        OR
        (
            response_status IS NOT NULL
            AND response_body IS NOT NULL
            AND response_headers IS NOT NULL
        )
    )
);

-- +goose Down
DROP TABLE IF EXISTS idempotency_records;