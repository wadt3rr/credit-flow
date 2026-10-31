-- +goose Up

CREATE TABLE applications (
    id UUID PRIMARY KEY,
    company_id UUID NOT NULL,
    product_id UUID NOT NULL,
    amount BIGINT NOT NULL CHECK (amount > 0),
    term	INTEGER	NOT NULL CHECK (term > 0),
    purpose	TEXT	NOT NULL,
    status	VARCHAR(32)	NOT NULL CHECK (status IN ('DRAFT', 'SUBMITTED', 'CANCELLED', 'VERIFYING', 'UNDER_REVIEW', 'REJECTED', 'APPROVED' )),
    version	INTEGER	NOT NULL DEFAULT 1 CHECK (version > 0),
    product_snapshot	JSONB,
    submitted_at	TIMESTAMPTZ,
    created_at	TIMESTAMPTZ	NOT NULL DEFAULT NOW(),
    updated_at	TIMESTAMPTZ	NOT NULL DEFAULT NOW(),

    CONSTRAINT chk_application_snapshot 
        CHECK (
            (product_snapshot IS NULL AND submitted_at IS NULL)
            OR
            (product_snapshot IS NOT NULL AND submitted_at IS NOT NULL)
        )
);


-- +goose Down

DROP TABLE IF EXISTS applications;