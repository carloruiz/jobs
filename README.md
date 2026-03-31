# Job System Design

## Overview

This library provides a durable job execution system backed by CockroachDB. Its
purpose is to ensure jobs complete reliably despite system crashes and
transient failures, while remaining simple enough for a small team to own and
operate.

The system is intentionally modest in scope. It is designed to handle business
logic jobs — both user-triggered events and scheduled cron jobs — at a scale
appropriate for a fast-growing product over roughly two years. After that
point, you are expected to outgrow it and migrate to a more capable system.

**This system is well-suited for:**
- User-triggered events (e.g. send welcome email, provision account)
- Background cron jobs (e.g. nightly billing, data cleanup)
- Workloads with modest concurrency (hundreds of concurrent jobs, not
  thousands)

**This system is not suited for:**
- Big data pipelines or batch workloads where job volume can exceed thousands
  concurrently
- Workloads requiring strict FIFO ordering across namespaces
- Workloads requiring priority scheduling
- Real-time job dispatch with sub-second latency requirements

---

## What This System Is Missing

This is a deliberately minimal system. The following features are absent by design:

| Missing Feature | Notes |
|---|---|
| Priority scheduling | All jobs within a namespace are treated equally. High-priority work must use a dedicated namespace and worker pool. |
| FIFO ordering guarantees | Jobs are claimed in an approximate order but no strict ordering is enforced across workers. |
| Fan-out / chaining | No first-class support for job DAGs or spawning child jobs. |
| Rate limiting per job type | Workers claim up to a configured limit but there is no per-type throttle. |
| Multi-tenant isolation | All workers share the same tables; logical separation is by namespace only. |

---

## System Behavior and Guarantees

### Guarantees

- **At-least-once execution**: Every job will be attempted at least once. Lease
  expiry ensures stalled jobs are re-claimed by another worker.
- **Local retry preference**: On transient failure, the executing worker
  retains the lease and retries the job locally after the configured backoff.
  The lease is only released to the pool on graceful termination. This
  minimizes unnecessary claim contention and preserves handler locality
  (in-process state, local caches).
- **No silent drops**: Unknown job types are logged and released, never
  silently ignored.
- **Durable state**: All job and attempt state is persisted in CockroachDB
  before execution begins.

### Requirements

- **Jobs must be idempotent**: Because a job may be retried after a partial
  execution (e.g. a worker crash mid-handler), handlers must be safe to run
  more than once with the same input. Use database-level upserts or external
  idempotency keys where necessary.

### Notes

- **Checkpointing**: Though there is not explicity first-class support for 
  checkpointing, checkpointing can be achieved by embedding jobs within other 
  jobs. First-class support for checkpointing can be a follow up.
- **Failure hooks**: Callers can register a callback that runs once a job has
  permanently failed (exhausted all retries). This hook is called exactly once,
  after the final failed attempt is recorded.
- **TODO**: What happens when a server crashes on the last job attempt? The
  lease will eventually expire, and a new worker will claim the job. But
  `attempt_no` will already equal `max_attempts`, so the job will be marked
  permanently failed without executing. Is this the right behavior? Consider
  whether to count "did not finish" separately from "failed with an error".

---

## Data Model

Three tables form the foundation: `jobs`, `job_attempts`.

### `jobs`

An append-only log of work to be done. Each row represents one unit of work and
is immutable after insertion. Jobs are partitioned logically by `name` (the job
type) and `namespace` (the named namespace).

```sql
CREATE TABLE jobs (
    id               UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    idempotency_key  TEXT        NOT NULL,               -- deduplication key; auto-generated if not supplied
    name             TEXT        NOT NULL,               -- job type identifier
    namespace        TEXT        NOT NULL,               -- logical grouping
    metadata         JSONB,                              -- arbitrary caller-supplied key-value pairs
    request          JSONB       NOT NULL,               -- input payload (max 1MB)
    max_attempts     INT         NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    creator_sha      TEXT        NOT NULL,               -- git SHA of the service that dispatched
    creator_host     TEXT        NOT NULL,               -- hostname of the service that dispatched
    backoff_policy   JSONB       NOT NULL,               -- per-attempt delay config (e.g. {"delay_seconds": [5, 30, 300]})
    deadline         TIMESTAMPTZ,                        -- optional; job is skipped after this

    UNIQUE (name, idempotency_key)    -- scoped per job type
);

CREATE INDEX ON jobs (namespace, name, created_at);
```

### `job_attempts`

An append-only log of every execution. Each attempt corresponds to one try at
executing a job. Attempts use `(job_id, attempt_no)` as the primary key so that
rows for the same job are stored contiguously on disk, achieving the same
proximity benefit that `INTERLEAVE IN PARENT` provided without the deprecated
feature.

```sql
CREATE TABLE job_attempts (
    job_id           UUID        NOT NULL REFERENCES jobs(id),
    attempt_no       INT         NOT NULL,              -- 1-indexed
    response         JSONB,                             -- output payload; NULL until complete
    error            JSONB,                             -- structured error; set on failure
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at      TIMESTAMPTZ,                       -- NULL while running
    executor_host    TEXT        NOT NULL,              -- hostname of the worker
    executor_sha     TEXT        NOT NULL,              -- git SHA of the worker binary

    PRIMARY KEY (job_id, attempt_no)
);
```

> **Payload limit:** Request payloads are stored on the `jobs` table (max 1MB).
> Response payloads on `job_attempts` are also capped at 1MB. Larger payloads
> should be stored externally (e.g. object storage) with a reference URI in the
> job request.

> **Why JSONB?** Payloads are human-readable directly in any SQL client without
> additional tooling. They are also queryable via JSON operators, enabling
> ad-hoc investigation (e.g. `WHERE request->>'user_id' = '123'`). Binary
> encoding (e.g. protobuf) is explicitly avoided — see the Serialization
> section for rationale.

### `jobs_overview` Debug View

The canonical way to observe job state. Joins `jobs` with the most recent
attempt, including jobs that have never been attempted.

```sql
CREATE VIEW jobs_overview AS
SELECT
    j.id           AS job_id,
    j.name,
    j.namespace,
    j.max_attempts,
    j.deadline,
    j.created_at,
    a.attempt_no,
    j.request,
    a.response,
    a.error,
    a.started_at,
    a.finished_at,
    a.executor_host,
    CASE
        WHEN a.attempt_no IS NULL                   THEN 'pending'
        WHEN a.finished_at IS NULL                  THEN 'running'
        WHEN a.error IS NOT NULL
             AND a.attempt_no >= j.max_attempts     THEN 'failed'
        WHEN a.error IS NOT NULL                    THEN 'pending_retry'
        ELSE                                             'complete'
    END            AS status
FROM jobs j
LEFT JOIN LATERAL (
    SELECT * FROM job_attempts
    WHERE job_id = j.id
    ORDER BY attempt_no DESC
    LIMIT 1
) a ON true;
```

To filter or limit results, query the view with parameters rather than relying on the view alone:

```sql
-- Filter by job type, namespace, and state; order by creation time; limit results
SELECT * FROM jobs_overview
WHERE name      = $1    -- filter by job type
  AND namespace = $2    -- filter by namespace
  AND status    = $3    -- 'pending' | 'running' | 'failed' | 'complete' | 'pending_retry'
ORDER BY created_at DESC
LIMIT $4;
```

This view is used for debugging, not for claim logic.

---

## Queue Mechanics

### Dispatching a Job

`Publisher` is the struct responsible for dispatching jobs. `Dispatch()`
creates the job row and, if no conflict is found, **prefers local execution**:
it immediately acquires the lease and runs the handler in a goroutine, rather
than creating an unlocked lease and waiting for a worker to claim it. If a
conflict is found (same idempotency key), `Dispatch()` checks whether the
existing job is actively running; if it is, it polls for the result and returns
it. `Run()` does the same but blocks until completion.

Preferring local execution in `Dispatch()` avoids a round-trip through the
claim poll cycle: the dispatching process acts as both publisher and executor.
Falling back to an unlocked lease (async worker claim) only occurs when local
execution cannot proceed — for example, when a race prevents immediate lease
acquisition.

Namespace is a system-wide configuration (set on `Worker` / `System` at
startup) and is not a per-call parameter.

A top-level `System` struct composes both `Publisher` and `Worker`, promoting
their methods for convenience.

```go
// System is the top-level entry point. Publisher and Worker are promoted.
type System struct {
    *Publisher
    *Worker
}

// Publisher dispatches jobs into the system.
type Publisher struct {
    namespace string
    leases    leases.Store
    worker    *Worker
}
```

Full implementation details — including idempotency key handling, deduplication
behavior, and the `Run()` polling path for duplicate keys — are in the
[Idempotency Key](#idempotency-key) section below.

### Idempotency Key

Every job row carries an `idempotency_key`. Its purpose is to prevent duplicate
jobs from being created when a caller retries a failed `Dispatch()` or `Run()`
call (e.g. due to a network timeout after the INSERT succeeded).

**Rules:**
- For `Dispatch()`: `idempotencyKey` is optional. Pass `""` to auto-generate a
  UUID, opting out of deduplication — safe for fire-and-forget callers that do
  not retry.
- For `Run()`: `idempotencyKey` is **required** (non-empty). `Run()` blocks
  until the job completes; if the caller retries after a timeout, the second
  call polls for the result of the first execution rather than launching a
  duplicate.

**Signatures:**

```go
// Dispatch creates the job row and prefers to acquire the lease and run the handler
// locally in a goroutine. If the job already exists, Dispatch polls job_status until
// the job reaches a terminal state and returns.
// req must be a valid JSON-encoded payload.
func (p *Publisher) Dispatch(
    ctx context.Context, db DBTX, name string, req json.RawMessage, idempotencyKey string,
) (*Job, error) {
    key := idempotencyKey
    if key == "" {
        key = uuid.NewString()
    }

    // Begin a transaction (or savepoint if db is already a pgx.Tx).
    tx, err := db.Begin(ctx)
    if err != nil {
        return nil, fmt.Errorf("begin tx: %w", err)
    }
    defer tx.Rollback(ctx)

    var job Job
    var isNew bool
    err = tx.QueryRow(ctx, `
        INSERT INTO jobs (id, idempotency_key, name, namespace, creator_sha, creator_host, request)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (name, idempotency_key) DO UPDATE SET id = jobs.id
        RETURNING id, idempotency_key, name, namespace, (xmax = 0) AS inserted`,
        uuid.New(), key, name, p.namespace, buildSHA, hostname, req,
    ).Scan(&job.ID, &job.IdempotencyKey, &job.Name, &job.Namespace, &isNew)
    if err != nil {
        return nil, fmt.Errorf("insert job: %w", err)
    }

    if !isNew {
        if err := tx.Commit(ctx); err != nil {
            return nil, fmt.Errorf("commit: %w", err)
        }
        return &job, nil
    }

    // Newly created job: create and acquire the lease atomically with the job row.
    lease, err := p.leases.CreateAndAcquire(ctx, tx, p.namespace, job.ID.String(), hostname, leaseDuration)
    if err != nil {
        return nil, fmt.Errorf("create and acquire lease: %w", err)
    }

    // Insert the first attempt row.
    attempt := &Attempt{JobID: job.ID, AttemptNo: 1, LeaseToken: lease.Token}
    if _, err = tx.Exec(ctx,
        `INSERT INTO job_attempts (job_id, attempt_no, executor_host, executor_sha)
         VALUES ($1, 1, $2, $3)`,
        job.ID, hostname, buildSHA,
    ); err != nil {
        return nil, fmt.Errorf("insert attempt: %w", err)
    }

    if err := tx.Commit(ctx); err != nil {
        return nil, fmt.Errorf("commit: %w", err)
    }

    // TODO: we should be using the job system context but after extracting
    // relevant context fields.
    go p.worker.runWithRetry(context.WithoutCancel(ctx), db, &job, attempt)

    return &job, nil
}

// Run creates the job, acquires the lease locally, and executes the handler in-process.
// idempotencyKey is required. If a job with this (name, key) already exists, Run polls
// job_status until the job reaches a terminal state, then returns the result.
// req must be a valid JSON-encoded payload. dest is a pointer to the response type that
// the result will be unmarshaled into.
func (s *System) Run(
    ctx context.Context, db DBTX, name string, req json.RawMessage, idempotencyKey string, dest any,
) error {
    if idempotencyKey == "" {
        return fmt.Errorf("Run: idempotencyKey is required")
    }

    // Begin a transaction (or savepoint if db is already a pgx.Tx).
    tx, err := db.Begin(ctx)
    if err != nil {
        return fmt.Errorf("begin tx: %w", err)
    }
    defer tx.Rollback(ctx)

    var job Job
    var isNew bool
    err = tx.QueryRow(ctx, `
        INSERT INTO jobs (id, idempotency_key, name, namespace, creator_sha, creator_host, request)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (name, idempotency_key) DO UPDATE SET id = jobs.id
        RETURNING id, idempotency_key, name, namespace, (xmax = 0) AS inserted`,
        uuid.New(), idempotencyKey, name, s.namespace, buildSHA, hostname, req,
    ).Scan(&job.ID, &job.IdempotencyKey, &job.Name, &job.Namespace, &isNew)
    if err != nil {
        return fmt.Errorf("insert job: %w", err)
    }

    if !isNew {
        if err := tx.Commit(ctx); err != nil {
            return fmt.Errorf("commit: %w", err)
        }
        // Job already exists — poll until terminal, then unmarshal the stored result.
        raw, err := s.pollForResult(ctx, db, job.ID)
        if err != nil {
            return err
        }
        return json.Unmarshal(raw, dest)
    }

    // Newly created job: acquire the lease locally and execute in-process.
    if _, err := s.leases.CreateAndAcquire(ctx, tx, s.namespace, job.ID.String(), hostname, leaseDuration); err != nil {
        return fmt.Errorf("create and acquire lease: %w", err)
    }

    attempt := &Attempt{JobID: job.ID, AttemptNo: 1}
    if _, err = tx.Exec(ctx,
        `INSERT INTO job_attempts (job_id, attempt_no, executor_host, executor_sha)
         VALUES ($1, 1, $2, $3)`,
        job.ID, hostname, buildSHA,
    ); err != nil {
        return fmt.Errorf("insert attempt: %w", err)
    }

    if err := tx.Commit(ctx); err != nil {
        return fmt.Errorf("commit: %w", err)
    }

    raw, err := s.Worker.runWithRetry(ctx, db, &job, attempt)
    if err != nil {
        return err
    }
    return json.Unmarshal(raw, dest)
}

// pollForCompletion polls job_status until the job reaches a terminal state.
// Used by Dispatch() when a running duplicate is detected.
func (p *Publisher) pollForCompletion(ctx context.Context, db DBTX, jobID uuid.UUID) error {
    ticker := time.NewTicker(1 * time.Second)
    defer ticker.Stop()
    for {
        select {
        case <-ctx.Done():
            return ctx.Err()
        case <-ticker.C:
            var status string
            err := db.QueryRowContext(ctx,
                `SELECT status FROM job_status WHERE job_id = $1`, jobID,
            ).Scan(&status)
            if err != nil {
                return err
            }
            switch status {
            case "complete":
                return nil
            case "failed", "cancelled":
                return fmt.Errorf("job %s: %s", jobID, status)
            }
            // pending, running, pending_retry — keep polling
        }
    }
}

// pollForResult polls job_status until the job reaches a terminal state,
// then fetches the response from job_attempts. Used by Run() for duplicate detection.
// See the Optimizations section for an optimized implementation of polling results
func (s *System) pollForResult(ctx context.Context, db DBTX, jobID uuid.UUID) (json.RawMessage, error) {
    ticker := time.NewTicker(500 * time.Millisecond)
    defer ticker.Stop()
    for {
        select {
        case <-ctx.Done():
            return nil, ctx.Err()
        case <-ticker.C:
            var status string
            err := db.QueryRowContext(ctx,
                `SELECT status FROM job_status WHERE job_id = $1`, jobID,
            ).Scan(&status)
            if err != nil {
                return nil, err
            }
            switch status {
            case "complete":
                var resp json.RawMessage
                err = db.QueryRowContext(ctx,
                    `SELECT response FROM job_attempts
                     WHERE job_id = $1 ORDER BY attempt_no DESC LIMIT 1`,
                    jobID,
                ).Scan(&resp)
                return resp, err
            case "failed", "cancelled":
                return nil, fmt.Errorf("job %s: %s", jobID, status)
            }
            // pending, running, pending_retry — keep polling
        }
    }
}
```

**Deduplication behavior:**

`Dispatch()` uses `INSERT ... ON CONFLICT (name, idempotency_key) DO UPDATE SET
id = jobs.id`. If a row already exists for the `(name, key)` pair, the insert
is a no-op and the existing job is returned unchanged. The caller receives the
same job ID on every call with the same key.

`Run()` additionally detects whether the INSERT created a new row (via `xmax =
0`). If a conflict was found, `Run()` blocks in `pollForResult` until the
original execution completes, then returns its stored response — so a caller
retrying a timed-out `Run()` does not launch a duplicate.

**Choosing an idempotency key:**

The key should be derived from the caller's intent, not from internal IDs. Good examples:

```go
// Stable key for a per-user, per-day billing job.
key := fmt.Sprintf("billing:%s:%s", userID, time.Now().UTC().Format("2006-01-02"))
sys.Dispatch(ctx, db, "charge_subscription", req, key)

// Stable key for a webhook delivery attempt.
key := fmt.Sprintf("webhook:%s:%d", webhookID, deliveryAttempt)
sys.Dispatch(ctx, db, "deliver_webhook", req, key)

// Fire-and-forget: no retries expected, omit the key.
sys.Dispatch(ctx, db, "send_notification", req, "")
```

### Claiming Jobs

Workers poll for available work by acquiring leases from the `leases` table. Claim logic runs inside a transaction to prevent double-claims. In the same transaction, a new `job_attempts` row is inserted for each claimed job — this ensures that a job attempt only exists in the DB if a worker has actually committed to running it. Workers only claim job types that are registered locally, ensuring unknown job types are never silently dropped.

```go
func (w *Worker) claimBatch(ctx context.Context) ([]*Attempt, error) {
    tx, err := w.db.BeginTx(ctx, nil)
    if err != nil {
        return nil, err
    }
    defer tx.Rollback()

    // Acquire up to `batchSize` leases from this namespace.
    leases, err := w.leases.AcquireMany(ctx, tx, w.namespace, w.batchSize, w.identity, leaseDuration)
    if err != nil {
        return nil, fmt.Errorf("acquire leases: %w", err)
    }
    if len(leases) == 0 {
        return nil, tx.Commit()
    }

    jobIDs := make([]string, len(leases))
    for i, l := range leases {
        jobIDs[i] = l.Resource
    }

    // Load job metadata and last attempt number for each claimed job.
    rows, err := tx.QueryContext(ctx, `
        SELECT j.id, j.name, j.max_attempts, j.backoff_policy, j.deadline, j.request,
               COALESCE(a.attempt_no, 0) AS last_attempt_no,
               a.started_at
        FROM jobs j
        LEFT JOIN LATERAL (
            SELECT * FROM job_attempts
            WHERE job_id = j.id
            ORDER BY attempt_no DESC LIMIT 1
        ) a ON true
        WHERE j.id = ANY($1)
    `, pq.Array(jobIDs))
    if err != nil {
        return nil, fmt.Errorf("load jobs: %w", err)
    }

    var attempts []*Attempt
    for rows.Next() {
        // ... scan job fields and lastAttemptNo
        nextAttemptNo := lastAttemptNo + 1

        // Validate before inserting the attempt. If invalid, release the lease
        // and skip (do not insert a job_attempts row).
        if err := w.validate(job, nextAttemptNo); err != nil {
            w.handleValidationError(ctx, tx, job, err)
            continue
        }

        // Insert the new attempt in the same tx as the lease acquisition.
        // These two operations are atomic: either both succeed or neither does.
        _, err = tx.ExecContext(ctx, `
            INSERT INTO job_attempts (job_id, attempt_no, executor_host, executor_sha)
            VALUES ($1, $2, $3, $4)`,
            job.ID, nextAttemptNo, hostname, buildSHA,
        )
        if err != nil {
            return nil, fmt.Errorf("insert attempt: %w", err)
        }
        attempts = append(attempts, &Attempt{JobID: job.ID, AttemptNo: nextAttemptNo})
    }

    return attempts, tx.Commit()
}
```

### Claim-Time Validation

Before executing, each claimed job is validated:

1. **Registered job type** — if `name` is not in the local registry, the job is
   released and skipped. This is safe when the number of job types is small (<
   ~30).
2. **Max attempts** — if `nextAttemptNo > max_attempts`, the job is marked
   permanently failed and the lease is deleted.
3. **Deadline** — if `deadline` is non-nil and has passed, the job is marked
   failed with reason `"deadline exceeded"` and the lease is deleted.
4. **Backoff** — if the elapsed time since the previous attempt's `started_at`
   is less than the configured delay, the lease is released and the job is
   skipped until the next poll cycle.
5. **Stale job guard (dev only)** — in local development environments, jobs
   older than a configurable threshold are skipped to prevent re-executing
   stale work from a previous session.

```go
func (w *Worker) validate(job *Job, nextAttemptNo int) error {
    if _, ok := w.registry[job.Name]; !ok {
        return fmt.Errorf("unknown job type %q", job.Name)
    }
    if nextAttemptNo > job.MaxAttempts {
        return ErrMaxAttemptsExceeded
    }
    if job.Deadline != nil && time.Now().After(*job.Deadline) {
        return ErrDeadlineExceeded
    }
    if w.devMode && time.Since(job.CreatedAt) > w.staleThreshold {
        return ErrStaleJobSkipped
    }
    return nil
}
```

---

## Execution and State Management

Job state is not stored as an explicit column. It is derived from the
relationship between `jobs` and `job_attempts` (see `jobs_overview` view
above).

### Execution Flow

```
Dispatch → [pending] → Claim → [running] → Complete → [complete]
                                         ↘ Fail     → [pending_retry] or [failed]
```

1. Worker claims a batch of leases (in a transaction).
2. In the same transaction, for each lease, the job is validated and a new
   `job_attempts` row is inserted. The lease acquisition and attempt insertion
   are atomic.
3. The transaction commits. The handler is invoked with the request payload.
4. On success: the `response` and `finished_at` columns are written in a
   transaction; the lease is deleted in the same transaction.
5. On transient failure: the `error` and `finished_at` columns are written on
   the current attempt; **the lease is retained**. The worker sleeps for the
   configured backoff duration, inserts a new `job_attempts` row, and
   re-executes the handler locally — no other worker can claim the job while
   this worker holds the lease.
6. On permanent failure (no attempts remaining): the `error` and `finished_at`
   columns are written; the lease is deleted.
7. On graceful termination: leases for any in-flight jobs are released,
   allowing another worker to re-claim them.

### Writing the Response

```go
// complete must be called within a transaction so the response write and lease
// deletion are atomic — a crash between them would leave the job in an inconsistent state.
func (w *Worker) complete(ctx context.Context, tx DBTX, job *Job, attempt *Attempt, resp json.RawMessage) error {
    _, err := tx.ExecContext(ctx,
        `UPDATE job_attempts SET response = $1, finished_at = now()
         WHERE job_id = $2 AND attempt_no = $3`,
        resp, attempt.JobID, attempt.AttemptNo,
    )
    if err != nil {
        return err
    }
    // Lease is no longer needed — delete it entirely.
    return w.leases.Delete(ctx, tx, job.ID.String())
}
```

### Recording a Failure

```go
// fail records the error on the current attempt. On permanent failure, the lease
// is deleted. On transient failure, the lease is retained — the caller (runWithRetry)
// is responsible for sleeping the backoff duration and inserting the next attempt row.
func (w *Worker) fail(ctx context.Context, tx DBTX, job *Job, attempt *Attempt, execErr error) error {
    _, err := tx.ExecContext(ctx,
        `UPDATE job_attempts SET error = $1, finished_at = now()
         WHERE job_id = $2 AND attempt_no = $3`,
        execErr.Error(), attempt.JobID, attempt.AttemptNo,
    )
    if err != nil {
        return err
    }

    if attempt.AttemptNo >= job.MaxAttempts {
        // Permanently failed — delete the lease so no worker ever re-claims it.
        return w.leases.Delete(ctx, tx, job.ID.String())
    }

    // Transient failure — lease is retained. runWithRetry will sleep the backoff
    // duration, insert the next job_attempts row, and retry locally.
    return nil
}
```

### Local Retry Loop

The worker retries failed jobs locally rather than releasing the lease and waiting for another worker to re-claim. This eliminates unnecessary claim contention and keeps the job close to in-process state (caches, connections).

```go
// runWithRetry executes the job handler, retrying locally on transient failure.
// The lease is held for the entire retry loop; it is deleted on success or
// permanent failure, and released on context cancellation (graceful termination).
func (w *Worker) runWithRetry(ctx context.Context, db DBTX, job *Job, attempt *Attempt) (json.RawMessage, error) {
    for {
        resp, execErr := w.registry[job.Name].Handle(ctx, attempt.Request)
        if execErr == nil {
            tx, _ := db.BeginTx(ctx, nil)
            if err := w.complete(ctx, tx, job, attempt, resp); err != nil {
                tx.Rollback()
                return nil, err
            }
            return resp, tx.Commit()
        }

        // Record the failure atomically.
        tx, _ := db.BeginTx(ctx, nil)
        if err := w.fail(ctx, tx, job, attempt, execErr); err != nil {
            tx.Rollback()
            return nil, err
        }
        tx.Commit()

        if attempt.AttemptNo >= job.MaxAttempts {
            // Permanent failure — lease already deleted inside fail().
            return nil, execErr
        }

        // Transient failure: sleep backoff, then insert the next attempt row and loop.
        backoff := w.backoffFor(job, attempt.AttemptNo)
        select {
        case <-ctx.Done():
            // Graceful termination: release the lease so another worker can re-claim.
            _ = w.leases.Release(context.Background(), db, job.ID.String(), attempt.LeaseToken)
            return nil, ctx.Err()
        case <-time.After(backoff):
        }

        nextAttemptNo := attempt.AttemptNo + 1
        if _, err := db.ExecContext(ctx,
            `INSERT INTO job_attempts (job_id, attempt_no, executor_host, executor_sha)
             VALUES ($1, $2, $3, $4)`,
            job.ID, nextAttemptNo, hostname, buildSHA,
        ); err != nil {
            return nil, fmt.Errorf("insert next attempt: %w", err)
        }
        attempt = &Attempt{
            JobID:      job.ID,
            AttemptNo:  nextAttemptNo,
            Request:    attempt.Request,
            LeaseToken: attempt.LeaseToken,
        }
    }
}
```

### Backoff

Backoff is enforced at claim time by comparing `started_at` of the most recent attempt against the configured delay for the current attempt number.

```go
var policy BackoffPolicy
if err := json.Unmarshal(job.BackoffPolicy, &policy); err != nil {
    return fmt.Errorf("unmarshal backoff policy: %w", err)
}
delayIdx := min(attempt.AttemptNo-1, len(policy.DelaySeconds)-1)
nextAllowedAt := attempt.StartedAt.Add(time.Duration(policy.DelaySeconds[delayIdx]) * time.Second)
if time.Now().Before(nextAllowedAt) {
    // Release lease and skip; worker will re-encounter this job on the next poll.
    return ErrBackoffNotElapsed
}
```

---

## Serialization and Typed Job Contracts

**Invariant: all request and response payloads must be valid JSON.** This is
enforced at every public interface boundary:

- `Dispatch(req json.RawMessage, ...)` and `Run(req json.RawMessage, ...)`
  accept `json.RawMessage`, which is a named `[]byte` type that signals the
  JSON contract to callers. Passing non-JSON bytes is a programming error; the
  system validates the payload is non-nil and well-formed before inserting it.
- Handlers registered via `JobFn` automatically produce valid JSON responses
  via `json.Marshal`. Custom `Handler` implementations that produce raw bytes
  must ensure their output is valid JSON — this is checked at registration time
  with a sentinel validation.
- The `request` and `response` columns are typed `JSONB` in CockroachDB, which
  enforces validity at the database level as a final backstop.

The job system's internal boundary carries `json.RawMessage` (not `[]byte`) to
make this contract explicit throughout the call stack. Any handler that returns
something that is not valid JSON will receive a registration-time panic, not a
silent runtime failure.

### JobFn

Each job type defines its own `Request` and `Response` structs. `JobFn` is a
generic adapter that handles JSON marshaling/unmarshaling centrally. Individual
handlers receive and return concrete Go types; JSON encoding/decoding is never
their responsibility.

```go
// JobFn adapts a typed function to the internal json.RawMessage boundary.
// Req and Resp must be JSON-serializable; the invariant is enforced at
// unmarshal (request) and marshal (response) time.
type JobFn[Req, Resp any] func(ctx context.Context, req Req) (Resp, error)

func (f JobFn[Req, Resp]) Handle(ctx context.Context, raw json.RawMessage) (json.RawMessage, error) {
    var req Req
    if err := json.Unmarshal(raw, &req); err != nil {
        return nil, fmt.Errorf("unmarshal request: %w", err)
    }
    resp, err := f(ctx, req)
    if err != nil {
        return nil, err
    }
    return json.Marshal(resp)
}
```

### Example Job Definition

```go
// jobs/send_welcome_email.go

type SendWelcomeEmailRequest struct {
    UserID string `json:"user_id"`
    Email  string `json:"email"`
}

type SendWelcomeEmailResponse struct {
    MessageID string `json:"message_id"`
}
```

Call sites register jobs directly using `JobFn`:

```go
js.Register("send_welcome_email", jobs.JobFn(func(ctx context.Context, req SendWelcomeEmailRequest) (SendWelcomeEmailResponse, error) {
    msgID, err := mailer.Send(ctx, req.Email, welcomeTemplate)
    if err != nil {
        return SendWelcomeEmailResponse{}, err
    }
    return SendWelcomeEmailResponse{MessageID: msgID}, nil
}))
```

At dispatch time, callers marshal their typed request before passing it to `Dispatch` or `Run`:

```go
req, err := json.Marshal(SendWelcomeEmailRequest{UserID: "u_123", Email: "user@example.com"})
if err != nil {
    return err
}
_, err = sys.Dispatch(ctx, db, "send_welcome_email", json.RawMessage(req), "")
```

> **Why not proto?** Protobuf makes job attempt rows opaque in the database,
> requiring additional tooling to inspect payloads. At this stage, the
> maintenance overhead of `.proto` files and generated code is not justified.
> JSON via `JobFn` provides equivalent compile-time type safety with full
> debuggability in any SQL client at zero extra cost. The JSON invariant is
> enforced at every layer, so there is no safety trade-off.

---

## Locking and Heartbeats

Locking is delegated entirely to the `leases` library. The job system does not own lock state.

### Leases Interface

```go
type Store interface {
    Create(ctx context.Context, db DBTX, group, resource string) (*Lease, error)
    CreateAndAcquire(ctx context.Context, db DBTX, group, resource, owner string, duration time.Duration) (*Lease, error)
    Delete(ctx context.Context, db DBTX, resource string) error

    Acquire(ctx context.Context, db DBTX, resource string, owner string, duration time.Duration) (*Lease, error)
    Release(ctx context.Context, db DBTX, resource string, token LeaseToken) error

    AcquireMany(ctx context.Context, db DBTX, group string, limit int, owner string, duration time.Duration) ([]Lease, error)

    Heartbeat(ctx context.Context, db DBTX, resource string, token LeaseToken, duration time.Duration) (*Lease, error)
    HeartbeatMany(ctx context.Context, db DBTX, items []HeartbeatRequest, duration time.Duration) ([]Lease, error)
}
```

- `Create`: called during `Dispatch()` to create an unlocked lease for
  asynchronous worker claim.
- `CreateAndAcquire`: called during `Run()` to atomically create and hold the
  lease locally, preventing other workers from claiming it.
- `Delete`: called on job completion, permanent failure, or cancellation to
  remove the lease row entirely.
- `AcquireMany`: called during the claim loop to grab available leases.
- `Release`: called only during graceful termination so another worker can
  re-claim in-flight jobs. On transient failure, the lease is **retained** —
  the worker retries locally without releasing.
- `HeartbeatMany`: called periodically by a background goroutine to keep active
  claims alive.

### Heartbeat Loop

A background goroutine sends heartbeats for all currently-held leases. If
heartbeats fail consistently (e.g. DB unavailable), the worker self-terminates
to release all claims via lease expiry.

```go
func (w *Worker) heartbeatLoop(ctx context.Context) {
    ticker := time.NewTicker(heartbeatInterval)
    defer ticker.Stop()

    consecutiveFailures := 0

    for {
        select {
        case <-ctx.Done():
            return
        case <-ticker.C:
            reqs := w.activeLeasesAsHeartbeatRequests()
            if len(reqs) == 0 {
                continue
            }
            _, err := w.leases.HeartbeatMany(ctx, w.db, reqs, leaseDuration)
            if err != nil {
                consecutiveFailures++
                slog.ErrorContext(ctx, "heartbeat failed", "err", err, "consecutive_failures", consecutiveFailures)
                if consecutiveFailures >= maxHeartbeatFailures {
                    slog.ErrorContext(ctx, "too many heartbeat failures, self-terminating")
                    // TODO: raise SIGTERM on the process so the OS/supervisor can
                    // perform a clean shutdown and restart instead of a hard exit.
                    w.shutdown()
                    return
                }
                continue
            }
            consecutiveFailures = 0
        }
    }
}
```

---

## Cancellation and Signals

Job cancellation is handled by a special system-internal `__system:cancel_job`.
When a cancel request arrives at the HTTP API, the system dispatches a
`__system:cancel_job` via `Run()` (synchronously in-process). The cancel job is
generic — it can cancel any target job — and its implementation is entirely
internal to the job system.

The cancel endpoint accepts a `broadcast` query parameter:
- `broadcast=true` (default): the cancel job broadcasts a signal to all workers
  so the running handler can begin stopping immediately, then marks the job as
  cancelled in the DB and deletes its lease.
- `broadcast=false`: skips the broadcast; if the job is not being executed
  locally, the cancel is applied to the DB state only and the method returns.
  Useful for cancelling pending (not yet running) jobs without incurring
  broadcast overhead.

```go
// cancelJobRequest is the request payload for the internal cancel job.
type cancelJobRequest struct {
    TargetJobID uuid.UUID `json:"target_job_id"`
    Broadcast   bool      `json:"broadcast"`
}

// registerSystemJobs wires up internal system jobs at startup.
func (s *System) registerSystemJobs() {
    s.Register("__system:cancel_job", jobs.JobFn(func(ctx context.Context, req cancelJobRequest) (struct{}, error) {
        if req.Broadcast {
            // Broadcast first so the running handler can begin stopping immediately.
            if err := s.broadcastCancel(ctx, req.TargetJobID); err != nil {
                return struct{}{}, fmt.Errorf("broadcast cancel: %w", err)
            }
        }

        // Mark the target job's latest open attempt as cancelled and delete its lease.
        // Both operations must succeed atomically.
        tx, err := s.db.BeginTx(ctx, nil)
        if err != nil {
            return struct{}{}, err
        }
        defer tx.Rollback()

        _, err = tx.ExecContext(ctx,
            `UPDATE job_attempts
             SET error = '{"code":"CANCELLED"}', finished_at = now()
             WHERE job_id = $1 AND finished_at IS NULL
             ORDER BY attempt_no DESC LIMIT 1`,
            req.TargetJobID,
        )
        if err != nil {
            return struct{}{}, fmt.Errorf("mark cancelled: %w", err)
        }
        if err := s.leases.Delete(ctx, tx, req.TargetJobID.String()); err != nil {
            return struct{}{}, fmt.Errorf("delete lease: %w", err)
        }
        return struct{}{}, tx.Commit()
    }))
}
```

Each worker exposes an internal HTTP endpoint that receives cancel
notifications from the broadcast. When a cancel notification is received, the
worker looks up the job in its in-memory set of active jobs and cancels the
associated context. A goroutine is dispatched to wait for the running handler
goroutine to fully exit before the cancel is considered complete.

```go
// activeJob holds the cancel function and a WaitGroup for a running job goroutine.
type activeJob struct {
    cancel func()
    done   sync.WaitGroup
}

// Each running job gets a cancellable context stored by job ID.
func (w *Worker) handleCancelNotify(rw http.ResponseWriter, r *http.Request) {
    var req cancelJobRequest
    if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
        http.Error(rw, "bad request", http.StatusBadRequest)
        return
    }
    w.mu.Lock()
    entry, ok := w.activeJobs[req.TargetJobID]
    w.mu.Unlock()
    if ok {
        entry.cancel() // cancels the context passed to the handler
        // Dispatch a goroutine to wait for the handler goroutine to fully exit.
        go func() {
            entry.done.Wait()
        }()
    }
    rw.WriteHeader(http.StatusOK)
}
```

If the job completes before the cancellation arrives, the cancel is a no-op. If
the handler respects context cancellation, it will stop work promptly.

---

## HTTP Management API

The management API exposes job lifecycle operations. It is intended for
internal use (operators, support tooling) rather than external consumers.

| Method | Path | Description |
|---|---|---|
| `POST` | `/api/v1/jobs/:id/cancel?broadcast=true\|false` | Cancel a job. `broadcast=true` (default) signals all workers; `broadcast=false` skips broadcast for pending jobs. |
| `POST` | `/api/v1/jobs/:id/retry` | Re-enqueue a permanently failed job via an internal retry job |
| `GET` | `/api/v1/jobs/:id/status` | Fast O(1) status lookup via `job_status` table |
| `GET` | `/api/v1/jobs/:id` | Full job detail including attempt history |

### Cancel

The cancel endpoint creates and runs a `__system:cancel_job` in-process, which
handles broadcast (if requested), DB update, and lease deletion atomically.

```go
func (api *API) Cancel(ctx context.Context, jobID uuid.UUID, broadcast bool) error {
    req, _ := json.Marshal(cancelJobRequest{TargetJobID: jobID, Broadcast: broadcast})
    _, err := api.sys.Run(ctx, api.db, "__system:cancel_job", req, uuid.NewString())
    return err
}
```

### Retry

Retry creates a special `__system:retry_job` via `Dispatch()`. This retry job
is entirely internal to the job system and can retry any permanently-failed
target job.

#### Design

The core problem is that the target job's `max_attempts` has already been
exhausted — the normal claim-time validation (`nextAttemptNo > max_attempts`)
would immediately mark the job failed again without executing. The retry job
sidesteps this by **acting as the executor itself**: rather than re-enqueuing
the target job for a worker to claim, the retry job's own handler directly
inserts attempt rows for the target job and invokes its handler via `runOnce`,
sharing the execution code path with normal job execution.

The retry job has a very high `max_attempts` (equal to the original job's
`max_attempts`) so it gets a full fresh set of attempts. Each attempt of the
retry job corresponds to exactly one attempt of the target job:

- If the target handler **succeeds**, the retry job records success on the
  target attempt row and returns successfully — the retry job is done.
- If the target handler **fails**, the retry job records failure on the target
  attempt row and returns an error — the retry job's own retry mechanism (its
  `runWithRetry` loop) handles backoff and re-execution.

This means the normal `runWithRetry` code path is shared: the retry job's
execution is itself subject to `runWithRetry`, so backoff, lease heartbeating,
graceful termination, and failure recording all work identically for retried
jobs as for original jobs.

#### Logic outline

```
__system:retry_job handler (one attempt = one target job handler invocation):
  1. Load target job from DB (handler name, request, last attempt_no)
  2. nextAttemptNo = last_attempt_no + 1
  3. INSERT INTO job_attempts (job_id=target.id, attempt_no=nextAttemptNo, ...)
     — this bypasses the normal claim-time max_attempts check; the retry job
       is responsible for bounding total attempts via its own max_attempts.
  4. Call w.runOnce(ctx, db, targetJob, targetAttempt)
       runOnce: invoke handler once; call complete() on success, fail() on error.
  5. If runOnce returns nil → target job is complete; return struct{}{}, nil
     If runOnce returns error → return the error
       → retry job's runWithRetry loop sleeps backoff, increments its own
         attempt_no, and loops back to step 1 on the next retry job attempt.
```

`runOnce` is a helper extracted from `runWithRetry` that executes the handler a
single time and records the result — `runWithRetry` is then a loop over
`runOnce`. Both regular jobs and the retry job share this path.

#### Code

```go
// retryJobRequest is the request payload for the internal retry job.
type retryJobRequest struct {
    TargetJobID uuid.UUID `json:"target_job_id"`
}

// registerSystemJobs also registers the retry job handler.
// (continued from the cancel_job registration above)
s.Register("__system:retry_job", jobs.JobFn(func(ctx context.Context, req retryJobRequest) (struct{}, error) {
    // Load target job and find the next attempt number.
    var targetJob Job
    var lastAttemptNo int
    row := s.db.QueryRowContext(ctx,
        `SELECT j.id, j.name, j.request, j.backoff_policy, j.deadline,
                COALESCE(MAX(a.attempt_no), 0)
         FROM jobs j
         LEFT JOIN job_attempts a ON a.job_id = j.id
         WHERE j.id = $1
         GROUP BY j.id, j.name, j.request, j.backoff_policy, j.deadline`,
        req.TargetJobID,
    )
    if err := row.Scan(
        &targetJob.ID, &targetJob.Name, &targetJob.Request,
        &targetJob.BackoffPolicy, &targetJob.Deadline, &lastAttemptNo,
    ); err != nil {
        return struct{}{}, fmt.Errorf("load target job: %w", err)
    }

    nextAttemptNo := lastAttemptNo + 1

    // Insert the next attempt row for the target job directly, bypassing
    // normal claim-time max_attempts validation. The retry job's own
    // max_attempts bounds the total number of additional attempts.
    if _, err := s.db.ExecContext(ctx,
        `INSERT INTO job_attempts (job_id, attempt_no, executor_host, executor_sha)
         VALUES ($1, $2, $3, $4)`,
        targetJob.ID, nextAttemptNo, hostname, buildSHA,
    ); err != nil {
        return struct{}{}, fmt.Errorf("insert target attempt: %w", err)
    }

    targetAttempt := &Attempt{
        JobID:     targetJob.ID,
        AttemptNo: nextAttemptNo,
        Request:   targetJob.Request,
        // Note: no LeaseToken — the retry job holds the lease, not the target job.
    }

    // Execute the target job's handler once, recording success/failure on the
    // target job's attempt row. This shares the runOnce code path with normal
    // job execution; the retry job's runWithRetry loop provides the outer retry.
    if err := s.Worker.runOnce(ctx, s.db, &targetJob, targetAttempt); err != nil {
        // Return the error so the retry job's own runWithRetry loop retries.
        return struct{}{}, err
    }
    return struct{}{}, nil
}))
```

When the retry job is dispatched, it sets `max_attempts` on itself equal to the
original job's `max_attempts`, giving the target job a full second set of
attempts:

```go
func (api *API) Retry(ctx context.Context, jobID uuid.UUID) error {
    // Look up the original job's max_attempts to size the retry budget.
    var origMaxAttempts int
    if err := api.db.QueryRowContext(ctx,
        `SELECT max_attempts FROM jobs WHERE id = $1`, jobID,
    ).Scan(&origMaxAttempts); err != nil {
        return fmt.Errorf("load job: %w", err)
    }

    req, _ := json.Marshal(retryJobRequest{TargetJobID: jobID})
    _, err := api.sys.Dispatch(ctx, api.db, "__system:retry_job", req,
        fmt.Sprintf("retry:%s", jobID), // stable key; prevents duplicate retries
        jobs.WithMaxAttempts(origMaxAttempts),
    )
    return err
}
```

Retried jobs can themselves be cancelled (via the cancel endpoint) and
re-retried (by calling retry again). Re-retrying dispatches a new
`__system:retry_job` with a fresh `max_attempts` budget.

---

## Observability

### Structured Logging

All worker and handler activity is logged using `slog`. Context is propagated
through to handlers so that request-scoped values (trace IDs, user IDs) appear
in logs. Always use the `Context` variants of slog methods (`InfoContext`,
`ErrorContext`, `WarnContext`) so that context-attached values are included in
log output.

```go
func (w *Worker) execute(ctx context.Context, job *Job, attempt *Attempt) error {
    log := slog.With("job_id", job.ID, "job_name", job.Name, "attempt_no", attempt.AttemptNo)
    log.InfoContext(ctx, "job started")

    resp, err := w.registry[job.Name].Handle(ctx, attempt.Request)
    if err != nil {
        log.ErrorContext(ctx, "job failed", "err", err)
        return err
    }

    log.InfoContext(ctx, "job completed")
    return w.complete(ctx, attempt.ID, resp)
}
```

### OpenTelemetry

The worker emits OTel spans and metrics:

- **Spans**: one span per job execution, with `job.name`, `job.id`, `attempt.no` as attributes.
- **Metrics**:
  - `jobs.executed` — counter, tagged by job name and outcome (`success` / `failure`)
  - `jobs.duration` — histogram of execution time in milliseconds
  - `jobs.queue_depth` — gauge of unclaimed jobs per namespace (sampled during claim polling)

```go
func (w *Worker) execute(ctx context.Context, job *Job, attempt *Attempt) error {
    ctx, span := tracer.Start(ctx, "job.execute",
        trace.WithAttributes(
            attribute.String("job.name", job.Name),
            attribute.String("job.id", job.ID.String()),
            attribute.Int("attempt.no", attempt.AttemptNo),
        ),
    )
    defer span.End()
    // ...
}
```

---

## Graceful Termination

When a worker process receives SIGTERM, it enters a 2-minute grace period to
allow in-flight jobs to finish before the process exits.

### Shutdown Sequence

1. **Stop accepting new claims** — the poll loop exits immediately; no new
   leases are acquired.
2. **Drain in-flight jobs** — wait up to 2 minutes for all running handlers to
   complete naturally.
3. **Cancel remaining jobs** — if any jobs are still running after 2 minutes,
   cancel their contexts. Handlers that respect context cancellation will stop
   promptly.
4. **Release leases** — for any jobs that did not finish, release their leases
   so another worker can re-claim them.
5. **Exit** — the process exits cleanly.

```go
func (w *Worker) Shutdown() {
    // 1. Stop the poll loop.
    w.stopPoll()

    const gracePeriod = 2 * time.Minute

    // 2. Wait for in-flight jobs to finish, up to the grace period.
    done := make(chan struct{})
    go func() {
        w.activeWg.Wait()
        close(done)
    }()

    select {
    case <-done:
        // All jobs finished cleanly within the grace period.
        return
    case <-time.After(gracePeriod):
        // Grace period elapsed — cancel all remaining job contexts.
    }

    // 3. Cancel contexts of all still-running jobs.
    w.mu.Lock()
    for _, entry := range w.activeJobs {
        entry.cancel()
    }
    w.mu.Unlock()

    // 4. Wait for all goroutines to exit after cancellation.
    w.activeWg.Wait()

    // 5. Release leases for jobs that did not finish naturally.
    // Completed/failed jobs already deleted or released their leases inside runWithRetry;
    // only the context-cancelled jobs still hold leases at this point.
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()
    w.mu.Lock()
    for _, entry := range w.activeJobs {
        _ = w.leases.Release(ctx, w.db, entry.jobID.String(), entry.leaseToken)
    }
    w.mu.Unlock()
}
```

`Shutdown()` is wired to OS signals in `main`:

```go
sigCh := make(chan os.Signal, 1)
signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
<-sigCh
worker.Shutdown()
```

### Interaction with Local Retry

Because the worker retains leases during transient failures and retries
locally, graceful termination must account for jobs that are currently sleeping
between retry attempts. These jobs are tracked in `w.activeJobs` alongside
actively-executing jobs. Cancelling their context in step 3 wakes them from the
backoff `select` in `runWithRetry`, which triggers an immediate
`leases.Release` before the goroutine exits.

This guarantees that no lease is left held by a stopped process — even jobs
mid-backoff are cleanly returned to the pool for another worker to claim.

---

## Dependency Injection

Handlers are registered as closures, capturing their dependencies (DB pool,
HTTP clients, third-party SDKs) at startup. The job system has no knowledge of
application-level dependencies — it only holds a map of `name → handler
function`.

Wiring happens in a single location (e.g. `main.go` or a dedicated wiring
file). This is the only site that knows about both the job system and
application internals.

```go
// main.go or wiring.go

func wire(db *pgxpool.Pool, mailer *Mailer, billingClient *BillingClient) *jobs.System {
    // Namespace is configured once on the System; it is not a per-call argument.
    sys := jobs.NewSystem(db, leaseStore, jobs.WorkerConfig{
        Namespace:    "default",
        BatchSize:    10,
        PollInterval: 2 * time.Second,
    })

    sys.Register("send_welcome_email", jobs.JobFn(func(ctx context.Context, req SendWelcomeEmailRequest) (SendWelcomeEmailResponse, error) {
        return mailer.Send(ctx, req.Email, welcomeTemplate)
    }))
    sys.Register("charge_subscription", jobs.JobFn(func(ctx context.Context, req ChargeRequest) (ChargeResponse, error) {
        return billingClient.Charge(ctx, req)
    }))
    sys.Register("cleanup_expired_sessions", jobs.JobFn(func(ctx context.Context, req CleanupRequest) (CleanupResponse, error) {
        return cleanupSessions(ctx, db, req)
    }))

    return sys
}
```

Each job file can export its own `Register()` function so job registration
stays co-located with the job definition, without requiring all jobs to be
wired in one central file:

```go
// jobs/send_welcome_email.go

func Register(sys *jobs.System, mailer *Mailer) {
    sys.Register("send_welcome_email", jobs.JobFn(func(ctx context.Context, req SendWelcomeEmailRequest) (SendWelcomeEmailResponse, error) {
        msgID, err := mailer.Send(ctx, req.Email, welcomeTemplate)
        if err != nil {
            return SendWelcomeEmailResponse{}, err
        }
        return SendWelcomeEmailResponse{MessageID: msgID}, nil
    }))
}
```

Adding a new job type only requires calling its `Register()` function at
startup; the central wiring file does not need to be modified.

---

## Job Cleanup and Archival

### Why cleanup matters

The `jobs` and `job_attempts` tables grow without bound. Unbounded growth has
real performance consequences:

- **Index bloat**: range scans over `(namespace, name, created_at)` slow down
  as the index covers millions of completed rows that will never be claimed
  again.
- **Vacuum / GC overhead**: CockroachDB's MVCC garbage collection must process
  historical versions for every row ever updated or deleted. A large hot table
  increases GC pause time and storage amplification.
- **Lease polling latency**: `AcquireMany` joins `leases` against `jobs` — if
  `jobs` is large and the index is cold, this query degrades.

### Why smart indexing alone is not enough

Partial indexes (e.g. `WHERE finished_at IS NULL`) help claim-time queries but
do not reduce table size. The table still grows unboundedly, compaction still
has to scan all rows during GC, and backup/restore times grow linearly.
Indexing is a complement to archival, not a substitute.

### Recommended approach: background archival to `job_history`

Move terminal rows (`complete`, `failed`, `cancelled`) older than 30 days to a
cold `job_history` table. The hot `jobs` table stays small; historical data
remains queryable.

```sql
-- Cold archive table (identical schema to jobs; kept on cheaper/slower storage if available).
CREATE TABLE job_history (LIKE jobs INCLUDING ALL);
CREATE TABLE job_attempt_history (LIKE job_attempts INCLUDING ALL);
```

The archival job runs as a registered system job on a nightly cron schedule:

```go
s.Register("__system:archive_jobs", jobs.JobFn(func(ctx context.Context, req struct{}) (struct{}, error) {
    cutoff := time.Now().UTC().AddDate(0, 0, -30)

    tx, err := s.db.BeginTx(ctx, nil)
    if err != nil {
        return struct{}{}, err
    }
    defer tx.Rollback()

    // Move old terminal jobs in batches to avoid long-running transactions.
    // terminal = complete, failed, or cancelled (no open lease).
    _, err = tx.ExecContext(ctx, `
        WITH archived AS (
            DELETE FROM jobs
            WHERE created_at < $1
              AND id NOT IN (SELECT resource::uuid FROM leases)
            RETURNING *
        )
        INSERT INTO job_history SELECT * FROM archived`,
        cutoff,
    )
    if err != nil {
        return struct{}{}, fmt.Errorf("archive jobs: %w", err)
    }

    _, err = tx.ExecContext(ctx, `
        WITH archived AS (
            DELETE FROM job_attempts
            WHERE job_id NOT IN (SELECT id FROM jobs)
            RETURNING *
        )
        INSERT INTO job_attempt_history SELECT * FROM archived`,
    )
    if err != nil {
        return struct{}{}, fmt.Errorf("archive job_attempts: %w", err)
    }

    return struct{}{}, tx.Commit()
}))
```

> **Batch size**: for very large tables, archive in smaller batches (e.g.
> `LIMIT 1000` per transaction) to avoid holding locks. Run the archival job
> repeatedly until the backlog is cleared.

> **CRDB Row-Level TTL**: CockroachDB 22.2+ supports [row-level
> TTL](https://www.cockroachlabs.com/docs/stable/row-level-ttl.html) natively
> (`ttl_expiration_expression`). This automatically deletes rows past a
> threshold without custom code, but does not support migrating rows to a cold
> table. Use TTL if you do not need to retain history; use the archival job if
> you do.

---

## Job Status Lookup

### Problem

Multiple callers — API handlers, support tooling, monitoring dashboards — need
to look up the current status of a job by ID. The `jobs_overview` view answers
this correctly, but it involves a correlated subquery (`LATERAL JOIN` against
`job_attempts`) that becomes expensive under concurrent load and large table
sizes.

### Why a separate status table is the right answer

The alternatives are:

| Approach | Pros | Cons |
|---|---|---|
| Query `jobs_overview` on demand | No extra state | Join + subquery on every read; expensive at scale |
| Add `status` column to `jobs` | Simple to query | `jobs` is append-only by design; adding a mutable column breaks that invariant |
| Dedicated `job_status` table | O(1) lookup; no joins; easy to index; decoupled from execution tables | Requires updates at lifecycle events; one more table to keep consistent |

A dedicated `job_status` table is the cleanest option. It is updated within the
same transaction as each lifecycle event, so it is always consistent with the
execution tables. Reads are a simple primary-key lookup.

### Should `job_status` store the response payload?

**No. `job_status` does not store the response.**

`job_status` is a high-load table: it is updated on every state transition and
read by every status poll. Storing response payloads here would increase row
size, amplify write cost, and put unnecessary pressure on the primary-key
index. The table must stay lightweight — just enough to answer "is this job
done?" without payload baggage.

Response payloads remain in `job_attempts`. Callers that need the response read
it from `job_attempts` directly after observing `status = 'complete'`. Callers
that care only about job side effects (e.g. "did the email send?") check
`job_status` and stop there — they never need to read `job_attempts` at all.
This split lets each use-case pay only for what it actually needs.

```
Caller pattern A — cares about side effects only:
  SELECT status FROM job_status WHERE job_id = $1
  → done when status = 'complete'

Caller pattern B — cares about the response value:
  SELECT status FROM job_status WHERE job_id = $1
  → on 'complete', SELECT response FROM job_attempts
      WHERE job_id = $1 ORDER BY attempt_no DESC LIMIT 1
```

`Run()` follows pattern B internally because it must return the response to the
caller. `Dispatch()` follows pattern A — it waits for completion but does not
surface the response.

### Schema

```sql
CREATE TABLE job_status (
    job_id      UUID         PRIMARY KEY REFERENCES jobs(id),
    status      VARCHAR(10)  NOT NULL,       -- 'running' | 'failed' | 'completed'
    updated_at  TIMESTAMPTZ  NOT NULL DEFAULT now()
    -- response is intentionally NOT stored here; read from job_attempts when needed
);
```

### Lifecycle update points

`job_status` exists only to answer one question: **is this job
done?** It has three states: `running`, `completed`, `failed`. It is
not written until a worker begins execution, and is updated only on
terminal events. More granular state tracking (pending,
pending_retry, cancelled) is available in the `jobs_overview` debug
view, which derives state from `jobs` and `job_attempts`.

| Event | Status written | Where |
|---|---|---|
| Worker claims lease + inserts attempt | `running` | Inside `claimBatch` tx |
| Job completes successfully | `completed` | Inside `complete()` tx |
| Transient failure (retry remaining) | _(no change — stays `running`)_ | — |
| Permanent failure (no retries) | `failed` | Inside `fail()` tx |
| Job cancelled | `failed` | Inside `__system:cancel_job` tx |
| Manual retry dispatched | `running` | Inside `__system:retry_job` tx |

Example — updating status inside `complete()`:

```go
func (w *Worker) complete(ctx context.Context, tx DBTX, job *Job, attempt *Attempt, resp json.RawMessage) error {
    _, err := tx.Exec(ctx,
        `UPDATE job_attempts SET response = $1, finished_at = now()
         WHERE job_id = $2 AND attempt_no = $3`,
        resp, attempt.JobID, attempt.AttemptNo,
    )
    if err != nil {
        return err
    }

    // Update denormalized status in the same tx.
    _, err = tx.Exec(ctx,
        `UPDATE job_status SET status = 'completed', updated_at = now()
         WHERE job_id = $1`,
        attempt.JobID,
    )
    if err != nil {
        return err
    }

    return w.leases.Delete(ctx, tx, job.ID.String())
}
```

### Lookup

```go
func (api *API) GetJobStatus(ctx context.Context, jobID uuid.UUID) (*JobStatus, error) {
    var s JobStatus
    err := api.db.QueryRowContext(ctx,
        `SELECT job_id, status, attempt_no, updated_at FROM job_status WHERE job_id = $1`,
        jobID,
    ).Scan(&s.JobID, &s.Status, &s.AttemptNo, &s.UpdatedAt)
    if err != nil {
        return nil, err
    }
    return &s, nil
}
```

This is a single primary-key scan — effectively O(1) regardless of how many jobs exist.

> **`jobs_overview` is still useful** for debugging and ad-hoc queries where
> you want attempt history alongside status. `job_status` is the fast path for
> programmatic lookups.

---

## Optimizations

### Batched Status Polling

`pollForCompletion` and `pollForResult` each run an independent query per
caller per tick. At 50-100 concurrent pollers (e.g. API handlers waiting on
`Run()` or retried `Dispatch()` calls), this means 50-100 connections checked
out of the pool every second for simple point reads. The DB handles the load
fine, but the connection pool becomes the bottleneck — polling callers starve
actual work (job inserts, lease operations, attempt writes) of connections.

**Solution:** A single background goroutine batches all active poll
subscriptions into one query per tick.

```go
// statusPoller batches concurrent poll requests into a single DB round-trip per tick.
type statusPoller struct {
    mu       sync.Mutex
    subs     map[uuid.UUID][]chan statusResult // job_id → waiting callers
    db       DBTX
    interval time.Duration
}

type statusResult struct {
    Status   string
    Response json.RawMessage // only populated for pollForResult callers
    Err      error
}

// Subscribe registers interest in a job's status. The returned channel receives
// exactly one result when the job reaches a terminal state.
func (sp *statusPoller) Subscribe(jobID uuid.UUID, wantResponse bool) <-chan statusResult {
    ch := make(chan statusResult, 1)
    sp.mu.Lock()
    sp.subs[jobID] = append(sp.subs[jobID], ch)
    sp.mu.Unlock()
    return ch
}

// run is the background loop. One query per tick, regardless of subscriber count.
func (sp *statusPoller) run(ctx context.Context) {
    ticker := time.NewTicker(sp.interval)
    defer ticker.Stop()

    for {
        select {
        case <-ctx.Done():
            return
        case <-ticker.C:
            sp.mu.Lock()
            ids := make([]uuid.UUID, 0, len(sp.subs))
            for id := range sp.subs {
                ids = append(ids, id)
            }
            sp.mu.Unlock()

            if len(ids) == 0 {
                continue
            }

            rows, err := sp.db.Query(ctx,
                `SELECT job_id, status FROM job_status WHERE job_id = ANY($1)`,
                ids,
            )
            if err != nil {
                continue // next tick will retry
            }

            for rows.Next() {
                var jobID uuid.UUID
                var status string
                rows.Scan(&jobID, &status)

                switch status {
                case "complete", "failed", "cancelled":
                    sp.mu.Lock()
                    waiters := sp.subs[jobID]
                    delete(sp.subs, jobID)
                    sp.mu.Unlock()

                    result := statusResult{Status: status}
                    for _, ch := range waiters {
                        ch <- result
                        close(ch)
                    }
                }
                // pending, running, pending_retry — keep polling
            }
            rows.Close()
        }
    }
}
```

This reduces connection usage from O(N) to O(1) per tick. Callers block on a
channel instead of holding a connection:

```go
func (p *Publisher) pollForCompletion(ctx context.Context, jobID uuid.UUID) error {
    ch := p.poller.Subscribe(jobID, false)
    select {
    case <-ctx.Done():
        return ctx.Err()
    case result := <-ch:
        if result.Err != nil {
            return result.Err
        }
        if result.Status != "complete" {
            return fmt.Errorf("job %s: %s", jobID, result.Status)
        }
        return nil
    }
}
```
