-- Compatible with PostgreSQL 12+ and CockroachDB 22.1+.
-- gen_random_uuid(): built-in in both (CockroachDB ≥ v20.2).
-- JSONB, TIMESTAMPTZ, UNIQUE constraints, and plain CREATE INDEX are all
-- supported identically by both engines.
CREATE TABLE jobs (
    id               UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    idempotency_key  TEXT        NOT NULL,               -- deduplication key; auto-generated if not supplied
    name             TEXT        NOT NULL,               -- job type identifier
    namespace        TEXT        NOT NULL,               -- logical grouping
    metadata         JSONB,                              -- arbitrary caller-supplied key-value pairs; "system" key reserved for context propagation
    request          JSONB       NOT NULL,               -- input payload (max 1MB)
    max_attempts     INT         NOT NULL,
    retry_until      TIMESTAMPTZ,                        -- optional; overrides max_attempts — keep retrying until this time elapses
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    creator_sha      TEXT        NOT NULL,               -- git SHA of the service that dispatched
    creator_host     TEXT        NOT NULL,               -- hostname of the service that dispatched
    backoff_policy   JSONB       NOT NULL,               -- per-attempt delay config (e.g. {"initial_interval": 5000000000, "multiplier": 2.0, "max_interval": 300000000000})
    deadline         TIMESTAMPTZ,                        -- optional; job is skipped after this

    UNIQUE (name, idempotency_key)    -- scoped per job type
);

CREATE INDEX ON jobs (namespace, name, created_at);
