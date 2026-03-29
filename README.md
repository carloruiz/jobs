# Job System Design

## Overview

This library provides a durable job execution system backed by CockroachDB. Its purpose is to ensure jobs complete reliably despite system crashes and transient failures, while remaining simple enough for a small team to own and operate.

The system is intentionally modest in scope. It is designed to handle business logic jobs — both user-triggered events and scheduled cron jobs — at a scale appropriate for a fast-growing product over roughly two years. After that point, you are expected to outgrow it and migrate to a more capable system.

**This system is well-suited for:**
- User-triggered events (e.g. send welcome email, provision account)
- Background cron jobs (e.g. nightly billing, data cleanup)
- Workloads with modest concurrency (hundreds of concurrent jobs, not thousands)

**This system is not suited for:**
- Big data pipelines or batch workloads where job volume can exceed thousands concurrently
- Workloads requiring strict FIFO ordering across queues
- Workloads requiring priority scheduling
- Real-time job dispatch with sub-second latency requirements

---

## What This System Is Missing

This is a deliberately minimal system. The following features are absent by design:

| Missing Feature | Notes |
|---|---|
| Priority scheduling | All jobs within a queue are treated equally. High-priority work must use a dedicated queue and worker pool. |
| FIFO ordering guarantees | Jobs are claimed in an approximate order but no strict ordering is enforced across workers. |
| Fan-out / chaining | No built-in support for job DAGs or spawning child jobs. |
| Rate limiting per job type | Workers claim up to a configured limit but there is no per-type throttle. |
| Dead letter queue | Permanently failed jobs stay in the DB and are retried manually via the management API. |
| Multi-tenant isolation | All workers share the same tables; logical separation is by queue name only. |

---

## Data Model

Three tables form the foundation: `jobs`, `job_attempts`, and `locks`.

### `jobs`

An append-only log of work to be done. Each row represents one unit of work and is immutable after insertion. Jobs are partitioned logically by `name` (the job type) and `queue` (the named queue).

```sql
CREATE TABLE jobs (
    id              UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    name            TEXT        NOT NULL,               -- job type identifier
    queue           TEXT        NOT NULL DEFAULT 'default',
    max_retries     INT         NOT NULL DEFAULT 3,
    backoff_seconds INT[]       NOT NULL DEFAULT '{5, 30, 300}', -- per-attempt delay
    deadline        TIMESTAMPTZ,                        -- optional; job is skipped after this
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    creator_sha     TEXT        NOT NULL                -- git SHA of the service that enqueued
);

CREATE INDEX ON jobs (queue, name, created_at);
```

### `job_attempts`

An append-only log of every execution. Each attempt corresponds to one try at executing a job. Attempts are stored contiguously on disk (clustered by `job_id`) because they are almost always read together with their parent job.

```sql
CREATE TABLE job_attempts (
    id               UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id           UUID        NOT NULL REFERENCES jobs(id),
    attempt_no       INT         NOT NULL,              -- 1-indexed
    request          JSONB       NOT NULL,              -- input payload (max 1MB)
    response         JSONB,                             -- output payload; NULL until complete
    error            TEXT,                              -- plain text; set on failure
    started_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at      TIMESTAMPTZ,                       -- NULL while running
    executor_host    TEXT        NOT NULL,              -- hostname of the worker
    executor_sha     TEXT        NOT NULL,              -- git SHA of the worker binary

    UNIQUE (job_id, attempt_no)
) INTERLEAVE IN PARENT jobs (job_id); -- store attempts next to their parent job
```

> **Payload limit:** Request and response payloads are capped at 1MB. Larger payloads should be stored externally (e.g. object storage) with a reference URI in the job request.

> **Why JSONB?** Payloads are human-readable directly in any SQL client without additional tooling. They are also queryable via JSON operators, enabling ad-hoc investigation (e.g. `WHERE request->>'user_id' = '123'`). Binary encoding (e.g. protobuf) is explicitly avoided — the debuggability cost outweighs any marginal efficiency gain.

### `locks`

A heartbeat table managed by the external `leases` library. One row exists per active job claim. The job system creates a lock row when a job is claimed and releases it on completion or failure.

```sql
-- Managed by the leases library; schema shown for reference.
CREATE TABLE leases (
    resource    TEXT        PRIMARY KEY,    -- job_id
    group_name  TEXT        NOT NULL,       -- queue name
    owner       TEXT        NOT NULL,       -- worker identity (host + process)
    token       UUID        NOT NULL,       -- fencing token; changes on each acquire
    expires_at  TIMESTAMPTZ NOT NULL
);
```

### `jobs_overview` Debug View

The canonical way to observe job state. Joins `jobs` with the most recent attempt, including jobs that have never been attempted.

```sql
CREATE VIEW jobs_overview AS
SELECT
    j.id           AS job_id,
    j.name,
    j.queue,
    j.max_retries,
    j.deadline,
    j.created_at,
    a.id           AS attempt_id,
    a.attempt_no,
    a.request,
    a.response,
    a.error,
    a.started_at,
    a.finished_at,
    a.executor_host,
    CASE
        WHEN a.id IS NULL                      THEN 'pending'
        WHEN a.finished_at IS NULL             THEN 'running'
        WHEN a.error IS NOT NULL
             AND a.attempt_no >= j.max_retries THEN 'failed'
        WHEN a.error IS NOT NULL               THEN 'pending_retry'
        ELSE                                        'complete'
    END            AS status
FROM jobs j
LEFT JOIN LATERAL (
    SELECT * FROM job_attempts
    WHERE job_id = j.id
    ORDER BY attempt_no DESC
    LIMIT 1
) a ON true;
```

This view is used for debugging, not for claim logic.

---

## Queue Mechanics

### Enqueueing a Job

A job is created in a single transaction that also creates its corresponding lock row. This ensures that a job is never visible to workers without an associated claimable lock.

```go
func (q *Queue) Enqueue(ctx context.Context, db DBTX, name, queue string, req []byte) (*Job, error) {
    job := &Job{
        ID:    uuid.New(),
        Name:  name,
        Queue: queue,
    }

    _, err := db.ExecContext(ctx,
        `INSERT INTO jobs (id, name, queue, creator_sha) VALUES ($1, $2, $3, $4)`,
        job.ID, job.Name, job.Queue, buildSHA,
    )
    if err != nil {
        return nil, fmt.Errorf("insert job: %w", err)
    }

    if err := q.leases.Create(ctx, db, queue, job.ID.String()); err != nil {
        return nil, fmt.Errorf("create lock: %w", err)
    }

    _, err = db.ExecContext(ctx,
        `INSERT INTO job_attempts (id, job_id, attempt_no, request, executor_host, executor_sha)
         VALUES ($1, $2, 1, $3, $4, $5)`,
        uuid.New(), job.ID, req, hostname, buildSHA,
    )
    return job, err
}
```

### Claiming Jobs

Workers poll for available work by acquiring leases from the `leases` table. Claim logic runs inside a transaction to prevent double-claims. Workers only claim job types that are registered locally, ensuring unknown job types are never silently dropped.

```go
func (w *Worker) claimBatch(ctx context.Context) ([]*Attempt, error) {
    tx, err := w.db.BeginTx(ctx, nil)
    if err != nil {
        return nil, err
    }
    defer tx.Rollback()

    // Acquire up to `batchSize` leases from this queue.
    leases, err := w.leases.AcquireMany(ctx, tx, w.queue, w.batchSize, w.identity, leaseDuration)
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

    // Load the most recent attempt for each claimed job.
    rows, err := tx.QueryContext(ctx, `
        SELECT j.id, j.name, j.max_retries, j.backoff_seconds, j.deadline,
               a.id, a.attempt_no, a.request
        FROM jobs j
        JOIN LATERAL (
            SELECT * FROM job_attempts
            WHERE job_id = j.id
            ORDER BY attempt_no DESC LIMIT 1
        ) a ON true
        WHERE j.id = ANY($1)
    `, pq.Array(jobIDs))
    // ... scan rows, validate conditions (see below), return attempts
}
```

### Claim-Time Validation

Before executing, each claimed job is validated:

1. **Registered job type** — if `name` is not in the local registry, the job is released and skipped. This is safe when the number of job types is small (< ~30).
2. **Max retries** — if `attempt_no > max_retries`, the job is marked permanently failed and the lease is released.
3. **Deadline** — if `deadline` is non-nil and has passed, the job is marked failed with reason `"deadline exceeded"` and the lease is released.
4. **Stale job guard (dev only)** — in local development environments, jobs older than a configurable threshold are skipped to prevent re-executing stale work from a previous session.

```go
func (w *Worker) validate(job *Job, attempt *Attempt) error {
    if _, ok := w.registry[job.Name]; !ok {
        return fmt.Errorf("unknown job type %q", job.Name)
    }
    if attempt.AttemptNo > job.MaxRetries {
        return ErrMaxRetriesExceeded
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

Job state is not stored as an explicit column. It is derived from the relationship between `jobs` and `job_attempts` (see `jobs_overview` view above).

### Execution Flow

```
Enqueue → [pending] → Claim → [running] → Complete → [complete]
                                        ↘ Fail     → [pending_retry] or [failed]
```

1. Worker claims a batch of leases.
2. For each lease, the latest attempt is loaded and validated.
3. The handler is invoked with the request payload.
4. On success: the `response` and `finished_at` columns are written; the lease is released.
5. On failure: the `error` and `finished_at` columns are written; a new `job_attempts` row is inserted for the next attempt (if retries remain); the lease is released.

### Writing the Response

```go
func (w *Worker) complete(ctx context.Context, attemptID uuid.UUID, resp []byte) error {
    _, err := w.db.ExecContext(ctx,
        `UPDATE job_attempts SET response = $1, finished_at = now() WHERE id = $2`,
        resp, attemptID,
    )
    return err
}
```

### Recording a Failure

```go
func (w *Worker) fail(ctx context.Context, tx DBTX, job *Job, attempt *Attempt, execErr error) error {
    // Record failure on the current attempt.
    _, err := tx.ExecContext(ctx,
        `UPDATE job_attempts SET error = $1, finished_at = now() WHERE id = $2`,
        execErr.Error(), attempt.ID,
    )
    if err != nil {
        return err
    }

    if attempt.AttemptNo >= job.MaxRetries {
        return nil // permanently failed; no new attempt row
    }

    // Schedule the next attempt (backoff is enforced at claim time via started_at).
    _, err = tx.ExecContext(ctx, `
        INSERT INTO job_attempts (id, job_id, attempt_no, request, executor_host, executor_sha)
        VALUES ($1, $2, $3, $4, $5, $6)`,
        uuid.New(), job.ID, attempt.AttemptNo+1, attempt.Request, hostname, buildSHA,
    )
    return err
}
```

### Backoff

Backoff is enforced at claim time by comparing `started_at` of the most recent attempt against the configured delay for the current attempt number.

```go
backoffDelay := job.BackoffSeconds[min(attempt.AttemptNo-1, len(job.BackoffSeconds)-1)]
nextAllowedAt := attempt.StartedAt.Add(time.Duration(backoffDelay) * time.Second)
if time.Now().Before(nextAllowedAt) {
    // Release lease and skip; worker will re-encounter this job on the next poll.
    return ErrBackoffNotElapsed
}
```

---

## Serialization and Typed Job Contracts

The job system's internal boundary is untyped: request and response payloads are `[]byte`. This keeps the infrastructure layer free of application-level types.

### Handler Interface

```go
// Handler is the low-level interface the job system works with internally.
type Handler interface {
    Handle(ctx context.Context, req []byte) ([]byte, error)
}
```

### TypedHandler Wrapper

Each job type defines its own `Request` and `Response` structs. A generic `TypedHandler` wrapper handles JSON marshaling/unmarshaling once, centrally. Individual handlers receive and return concrete Go types.

```go
// TypedHandler wraps a typed function into the Handler interface.
type TypedHandler[Req, Resp any] struct {
    fn func(ctx context.Context, req Req) (Resp, error)
}

func NewTypedHandler[Req, Resp any](fn func(ctx context.Context, req Req) (Resp, error)) *TypedHandler[Req, Resp] {
    return &TypedHandler[Req, Resp]{fn: fn}
}

func (h *TypedHandler[Req, Resp]) Handle(ctx context.Context, raw []byte) ([]byte, error) {
    var req Req
    if err := json.Unmarshal(raw, &req); err != nil {
        return nil, fmt.Errorf("unmarshal request: %w", err)
    }
    resp, err := h.fn(ctx, req)
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

func NewSendWelcomeEmailHandler(mailer *Mailer) jobs.Handler {
    return jobs.NewTypedHandler(func(ctx context.Context, req SendWelcomeEmailRequest) (SendWelcomeEmailResponse, error) {
        msgID, err := mailer.Send(ctx, req.Email, welcomeTemplate)
        if err != nil {
            return SendWelcomeEmailResponse{}, err
        }
        return SendWelcomeEmailResponse{MessageID: msgID}, nil
    })
}
```

> **Why not proto?** Binary encoding (protobuf) makes job attempt rows opaque in the database. Engineers cannot inspect request/response payloads in a SQL client without additional tooling. JSON via `TypedHandler` provides equivalent compile-time safety with full debuggability at zero extra cost.

---

## Locking and Heartbeats

Locking is delegated entirely to the `leases` library. The job system does not own lock state.

### Leases Interface

```go
type Store interface {
    Create(ctx context.Context, db DBTX, group, resource string) error
    Delete(ctx context.Context, db DBTX, resource string) error

    Acquire(ctx context.Context, db DBTX, resource string, owner string, duration time.Duration) (*Lease, error)
    Release(ctx context.Context, db DBTX, resource string, token LeaseToken) error

    AcquireMany(ctx context.Context, db DBTX, group string, limit int, owner string, duration time.Duration) ([]Lease, error)

    Heartbeat(ctx context.Context, db DBTX, resource string, token LeaseToken, duration time.Duration) (*Lease, error)
    HeartbeatMany(ctx context.Context, db DBTX, items []HeartbeatRequest, duration time.Duration) ([]Lease, error)
}
```

- `Create` / `Delete`: called during enqueue and permanent failure to manage the lock row lifecycle.
- `AcquireMany`: called during the claim loop to grab available leases.
- `Release`: called on job completion or non-retryable failure.
- `HeartbeatMany`: called periodically by a background goroutine to keep active claims alive.

### Heartbeat Loop

A background goroutine sends heartbeats for all currently-held leases. If heartbeats fail consistently (e.g. DB unavailable), the worker self-terminates to release all claims via lease expiry.

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
                slog.Error("heartbeat failed", "err", err, "consecutive_failures", consecutiveFailures)
                if consecutiveFailures >= maxHeartbeatFailures {
                    slog.Error("too many heartbeat failures, self-terminating")
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

Job cancellation requires notifying all workers, not just the worker currently executing the job (the cancelling caller may not know which worker holds the job).

Broadcast is delegated to an existing broadcast library:

```go
func (api *API) CancelJob(ctx context.Context, jobID uuid.UUID) error {
    body, _ := json.Marshal(CancelRequest{JobID: jobID})

    return broadcaster.Send(ctx, "myservice.internal", 8080, func(ctx context.Context, client *http.Client) error {
        resp, err := client.Post(
            "http://myservice.internal/api/v1/jobs/cancel",
            "application/json",
            bytes.NewReader(body),
        )
        if err != nil {
            return err
        }
        if resp.StatusCode != http.StatusOK {
            return fmt.Errorf("unexpected status: %d", resp.StatusCode)
        }
        return nil
    })
}
```

Each worker exposes an internal HTTP endpoint that receives cancel notifications. When a cancel is received, the worker looks up the job in its in-memory set of active jobs and cancels the associated context.

```go
// Each running job gets a cancellable context stored by job ID.
func (w *Worker) handleCancelNotify(rw http.ResponseWriter, r *http.Request) {
    var req CancelRequest
    if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
        http.Error(rw, "bad request", http.StatusBadRequest)
        return
    }
    w.mu.Lock()
    cancel, ok := w.activeJobs[req.JobID]
    w.mu.Unlock()
    if ok {
        cancel() // cancels the context passed to the handler
    }
    rw.WriteHeader(http.StatusOK)
}
```

If the job completes before the cancellation arrives, the cancel is a no-op. If the handler respects context cancellation, it will stop work promptly.

---

## HTTP Management API

The management API exposes job lifecycle operations. It is intended for internal use (operators, support tooling) rather than external consumers.

| Method | Path | Description |
|---|---|---|
| `POST` | `/api/v1/jobs/:id/cancel` | Cancel a running or pending job |
| `POST` | `/api/v1/jobs/:id/retry` | Re-enqueue a permanently failed job |
| `GET` | `/api/v1/jobs/:id` | Get job status and attempt history |
| `POST` | `/api/v1/jobs/cancel` (internal) | Broadcast cancel notification received by workers |

### Cancel

Marks the job's latest attempt as cancelled in the DB, then broadcasts a cancel signal to all workers.

```go
func (api *API) Cancel(ctx context.Context, jobID uuid.UUID) error {
    _, err := api.db.ExecContext(ctx,
        `UPDATE job_attempts SET error = 'cancelled', finished_at = now()
         WHERE job_id = $1 AND finished_at IS NULL
         ORDER BY attempt_no DESC LIMIT 1`,
        jobID,
    )
    if err != nil {
        return fmt.Errorf("mark cancelled: %w", err)
    }
    return api.broadcastCancel(ctx, jobID)
}
```

### Retry

Inserts a new `job_attempts` row for a permanently failed job, resetting its attempt counter to allow re-execution.

```go
func (api *API) Retry(ctx context.Context, jobID uuid.UUID) error {
    row := api.db.QueryRowContext(ctx,
        `SELECT COALESCE(MAX(attempt_no), 0), request FROM job_attempts WHERE job_id = $1`,
        jobID,
    )
    var lastAttemptNo int
    var request []byte
    if err := row.Scan(&lastAttemptNo, &request); err != nil {
        return fmt.Errorf("fetch attempt: %w", err)
    }
    _, err := api.db.ExecContext(ctx, `
        INSERT INTO job_attempts (id, job_id, attempt_no, request, executor_host, executor_sha)
        VALUES ($1, $2, $3, $4, $5, $6)`,
        uuid.New(), jobID, lastAttemptNo+1, request, "manual-retry", buildSHA,
    )
    return err
}
```

---

## Observability

### Structured Logging

All worker and handler activity is logged using `slog`. Context is propagated through to handlers so that request-scoped values (trace IDs, user IDs) appear in logs.

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
  - `jobs.queue_depth` — gauge of unclaimed jobs per queue (sampled during claim polling)

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

## Dependency Injection

Handlers are registered as closures, capturing their dependencies (DB pool, HTTP clients, third-party SDKs) at startup. The job system has no knowledge of application-level dependencies — it only holds a map of `name → Handler`.

Wiring happens in a single location (e.g. `main.go` or a dedicated wiring file). This is the only site that knows about both the job system and application internals.

```go
// main.go or wiring.go

func wire(db *pgxpool.Pool, mailer *Mailer, billingClient *BillingClient) *jobs.Worker {
    registry := jobs.Registry{
        "send_welcome_email": jobs.NewTypedHandler(NewSendWelcomeEmailHandler(mailer).Handle),
        "charge_subscription": jobs.NewTypedHandler(NewChargeSubscriptionHandler(db, billingClient).Handle),
        "cleanup_expired_sessions": jobs.NewTypedHandler(NewCleanupHandler(db).Handle),
    }

    return jobs.NewWorker(db, leaseStore, registry, jobs.WorkerConfig{
        Queue:     "default",
        BatchSize: 10,
        PollInterval: 2 * time.Second,
    })
}
```

Each handler constructor takes only the dependencies it needs. The job system itself receives only the opaque `Registry` map. This means application code never imports job system internals beyond the `Handler` interface, and the job system never imports application code.
