-- Compatible with PostgreSQL 12+ and CockroachDB 22.1+.
-- job_status is a denormalized table for O(1) status lookups. It is updated
-- within the same transaction as each lifecycle event, so it is always
-- consistent with jobs and job_attempts.
--
-- Three states only: running, completed, failed.
-- Granular state (pending, pending_retry, cancelled) is available via
-- jobs_overview. Callers that only need to know "is this job done?" read
-- this table; response payloads stay in job_attempts.
CREATE TABLE job_status (
    job_id      UUID         PRIMARY KEY REFERENCES jobs(id),
    status      VARCHAR(10)  NOT NULL,       -- 'running' | 'completed' | 'failed'
    updated_at  TIMESTAMPTZ  NOT NULL DEFAULT now()
    -- response is intentionally NOT stored here; read from job_attempts when needed
);
