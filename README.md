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
| Fan-out / chaining | No built-in support for job DAGs or spawning child jobs. |
| Rate limiting per job type | Workers claim up to a configured limit but there is no per-type throttle. |
| Multi-tenant isolation | All workers share the same tables; logical separation is by namespace only. |

---

## System Behavior and Guarantees

### Guarantees

- **At-least-once execution**: Every job will be attempted at least once. Lease expiry ensures stalled jobs are re-claimed by another worker.
- **No silent drops**: Unknown job types are logged and released, never silently ignored.
- **Durable state**: All job and attempt state is persisted in CockroachDB before execution begins.

### Requirements

- **Jobs must be idempotent**: Because a job may be retried after a partial execution (e.g. a worker crash mid-handler), handlers must be safe to run more than once with the same input. Use database-level upserts or external idempotency keys where necessary.

### Notes

- **Checkpointing**: There is no mechanism for a long-running handler to save intermediate progress. If a handler is interrupted mid-way, it restarts from scratch. Checkpointing is a potential future feature.
- **Failure hooks**: Callers can register a callback that runs once a job has permanently failed (exhausted all retries). This hook is called exactly once, after the final failed attempt is recorded.
- **TODO**: What happens when a server crashes on the last job attempt? The lease will eventually expire, and a new worker will claim the job. But `attempt_no` will already equal `max_attempts`, so the job will be marked permanently failed without executing. Is this the right behavior? Consider whether to count "did not finish" separately from "failed with an error".

---

## Data Model

Three tables form the foundation: `jobs`, `job_attempts`, and `leases`.

### `jobs`

An append-only log of work to be done. Each row represents one unit of work and is immutable after insertion. Jobs are partitioned logically by `name` (the job type) and `namespace` (the named namespace).

```sql
CREATE TABLE jobs (
    id               UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    name             TEXT        NOT NULL,               -- job type identifier
    namespace        TEXT        NOT NULL,               -- logical grouping
    max_attempts     INT         NOT NULL,
    backoff_policy   JSONB       NOT NULL,               -- per-attempt delay config (e.g. {"delay_seconds": [5, 30, 300]})
    deadline         TIMESTAMPTZ,                        -- optional; job is skipped after this
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    creator_sha      TEXT        NOT NULL,               -- git SHA of the service that dispatched
    creator_host     TEXT        NOT NULL,               -- hostname of the service that dispatched
    metadata         JSONB,                              -- arbitrary caller-supplied key-value pairs
    logging_context  JSONB,                              -- logging fields to propagate into job execution
    request          JSONB       NOT NULL                -- input payload (max 1MB)
);

CREATE INDEX ON jobs (namespace, name, created_at);
```

### `job_attempts`

An append-only log of every execution. Each attempt corresponds to one try at executing a job. Attempts use `(job_id, attempt_no)` as the primary key so that rows for the same job are stored contiguously on disk, achieving the same proximity benefit that `INTERLEAVE IN PARENT` provided without the deprecated feature.

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

> **Payload limit:** Request payloads are stored on the `jobs` table (max 1MB). Response payloads on `job_attempts` are also capped at 1MB. Larger payloads should be stored externally (e.g. object storage) with a reference URI in the job request.

> **Why JSONB?** Payloads are human-readable directly in any SQL client without additional tooling. They are also queryable via JSON operators, enabling ad-hoc investigation (e.g. `WHERE request->>'user_id' = '123'`). Binary encoding (e.g. protobuf) is explicitly avoided — see the Serialization section for rationale.

### `leases`

A heartbeat table managed by the external `leases` library. One row exists per active job claim. The job system creates a lease row when a job is dispatched and deletes it on completion, permanent failure, or cancellation.

```sql
-- Managed by the leases library; schema shown for reference.
CREATE TABLE leases (
    resource    TEXT        PRIMARY KEY,    -- job_id
    group_name  TEXT        NOT NULL,       -- namespace name
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

`Publisher` is the struct responsible for dispatching jobs. `Dispatch()` creates the job row and an unlocked lease, then returns immediately — a worker will claim the lease and execute the job asynchronously. `Run()` creates the job, acquires the lease locally via `CreateAndAcquire`, creates the job attempt, and executes the handler in-process, blocking until completion.

A top-level `System` struct composes both `Publisher` and `Worker`, promoting their methods for convenience.

```go
// System is the top-level entry point. Publisher and Worker are promoted.
type System struct {
    *Publisher
    *Worker
}

// Publisher dispatches jobs into the system.
type Publisher struct {
    leases leases.Store
}

// Dispatch creates the job row and an unlocked lease, then returns immediately.
// No job attempt is created here — that happens when a worker claims the lease.
func (p *Publisher) Dispatch(ctx context.Context, db DBTX, name, namespace string, req []byte) (*Job, error) {
    job := &Job{
        ID:        uuid.New(),
        Name:      name,
        Namespace: namespace,
    }

    _, err := db.ExecContext(ctx,
        `INSERT INTO jobs (id, name, namespace, creator_sha, creator_host, request)
         VALUES ($1, $2, $3, $4, $5, $6)`,
        job.ID, job.Name, job.Namespace, buildSHA, hostname, req,
    )
    if err != nil {
        return nil, fmt.Errorf("insert job: %w", err)
    }

    // Create the lease (unlocked) so a worker can claim it asynchronously.
    if _, err := p.leases.Create(ctx, db, namespace, job.ID.String()); err != nil {
        return nil, fmt.Errorf("create lease: %w", err)
    }

    return job, nil
}

// Run creates the job, acquires the lease locally, and executes the handler in-process.
// It blocks until the job completes, returning the response.
func (s *System) Run(ctx context.Context, db DBTX, name, namespace string, req []byte) ([]byte, error) {
    job := &Job{
        ID:        uuid.New(),
        Name:      name,
        Namespace: namespace,
        Request:   req,
    }

    // 1. Create the job row.
    _, err := db.ExecContext(ctx,
        `INSERT INTO jobs (id, name, namespace, creator_sha, creator_host, request)
         VALUES ($1, $2, $3, $4, $5, $6)`,
        job.ID, job.Name, job.Namespace, buildSHA, hostname, req,
    )
    if err != nil {
        return nil, fmt.Errorf("insert job: %w", err)
    }

    // 2. Create and acquire the lease so no other worker claims it.
    if _, err := s.leases.CreateAndAcquire(ctx, db, namespace, job.ID.String(), hostname, leaseDuration); err != nil {
        return nil, fmt.Errorf("create and acquire lease: %w", err)
    }

    // 3. Create the first job attempt.
    attempt := &Attempt{JobID: job.ID, AttemptNo: 1}
    _, err = db.ExecContext(ctx,
        `INSERT INTO job_attempts (job_id, attempt_no, executor_host, executor_sha)
         VALUES ($1, 1, $2, $3)`,
        job.ID, hostname, buildSHA,
    )
    if err != nil {
        return nil, fmt.Errorf("insert attempt: %w", err)
    }

    // 4. Execute the job locally via the Worker's handler registry.
    resp, execErr := s.Worker.registry[name].Handle(ctx, req)
    if execErr != nil {
        _ = s.Worker.fail(ctx, db, job, attempt, execErr)
        return nil, fmt.Errorf("job failed: %w", execErr)
    }

    if err := s.Worker.complete(ctx, db, job, attempt, resp); err != nil {
        return nil, fmt.Errorf("complete job: %w", err)
    }
    return resp, nil
}
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

1. **Registered job type** — if `name` is not in the local registry, the job is released and skipped. This is safe when the number of job types is small (< ~30).
2. **Max attempts** — if `nextAttemptNo > max_attempts`, the job is marked permanently failed and the lease is deleted.
3. **Deadline** — if `deadline` is non-nil and has passed, the job is marked failed with reason `"deadline exceeded"` and the lease is deleted.
4. **Backoff** — if the elapsed time since the previous attempt's `started_at` is less than the configured delay, the lease is released and the job is skipped until the next poll cycle.
5. **Stale job guard (dev only)** — in local development environments, jobs older than a configurable threshold are skipped to prevent re-executing stale work from a previous session.

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

Job state is not stored as an explicit column. It is derived from the relationship between `jobs` and `job_attempts` (see `jobs_overview` view above).

### Execution Flow

```
Dispatch → [pending] → Claim → [running] → Complete → [complete]
                                         ↘ Fail     → [pending_retry] or [failed]
```

1. Worker claims a batch of leases (in a transaction).
2. In the same transaction, for each lease, the job is validated and a new `job_attempts` row is inserted. The lease acquisition and attempt insertion are atomic.
3. The transaction commits. The handler is invoked with the request payload.
4. On success: the `response` and `finished_at` columns are written; the lease is deleted.
5. On transient failure: the `error` and `finished_at` columns are written on the current attempt; the lease is released so another worker can re-claim. When a worker later claims the lease, it inserts a new `job_attempts` row in the same transaction as the lease acquisition.
6. On permanent failure (no attempts remaining): the `error` and `finished_at` columns are written; the lease is deleted.

### Writing the Response

```go
// complete must be called within a transaction so the response write and lease
// deletion are atomic — a crash between them would leave the job in an inconsistent state.
func (w *Worker) complete(ctx context.Context, tx DBTX, job *Job, attempt *Attempt, resp []byte) error {
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
// fail must be called within a transaction so the error write and lease
// update are atomic — a crash between them would leave the job in an inconsistent state.
func (w *Worker) fail(ctx context.Context, tx DBTX, job *Job, attempt *Attempt, execErr error) error {
    // Record the failure on the current attempt.
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

    // Transient failure — release the lease so another worker can re-claim.
    // The next job_attempts row is NOT inserted here; it will be inserted by
    // the worker that claims the lease on the next poll cycle (in the same tx
    // as the lease acquisition).
    return w.leases.Release(ctx, tx, job.ID.String(), attempt.LeaseToken)
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

The job system's internal boundary is untyped: request and response payloads are `[]byte`. This keeps the infrastructure layer free of application-level types.

### JobFn

Each job type defines its own `Request` and `Response` structs. `JobFn` is a generic adapter that handles JSON marshaling/unmarshaling centrally. Individual handlers receive and return concrete Go types.

```go
// JobFn adapts a typed function to the internal []byte boundary.
type JobFn[Req, Resp any] func(ctx context.Context, req Req) (Resp, error)

func (f JobFn[Req, Resp]) Handle(ctx context.Context, raw []byte) ([]byte, error) {
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

> **Why not proto?** Protobuf makes job attempt rows opaque in the database, requiring additional tooling to inspect payloads. At this stage, the maintenance overhead of `.proto` files and generated code is not justified. JSON via `JobFn` provides equivalent compile-time type safety with full debuggability in any SQL client at zero extra cost.

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

- `Create`: called during `Dispatch()` to create an unlocked lease for asynchronous worker claim.
- `CreateAndAcquire`: called during `Run()` to atomically create and hold the lease locally, preventing other workers from claiming it.
- `Delete`: called on job completion, permanent failure, or cancellation to remove the lease row entirely.
- `AcquireMany`: called during the claim loop to grab available leases.
- `Release`: called on transient failure so another worker can re-claim the job on the next poll.
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

Job cancellation is handled by a special system-internal `__system:cancel_job`. When a cancel request arrives at the HTTP API, the system dispatches a `__system:cancel_job` via `Run()` (synchronously in-process). The cancel job is generic — it can cancel any target job — and its implementation is entirely internal to the job system.

The cancel endpoint accepts a `broadcast` query parameter:
- `broadcast=true` (default): the cancel job broadcasts a signal to all workers so the running handler can begin stopping immediately, then marks the job as cancelled in the DB and deletes its lease.
- `broadcast=false`: skips the broadcast; if the job is not being executed locally, the cancel is applied to the DB state only and the method returns. Useful for cancelling pending (not yet running) jobs without incurring broadcast overhead.

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

Each worker exposes an internal HTTP endpoint that receives cancel notifications from the broadcast. When a cancel notification is received, the worker looks up the job in its in-memory set of active jobs and cancels the associated context. A goroutine is dispatched to wait for the running handler goroutine to fully exit before the cancel is considered complete.

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

If the job completes before the cancellation arrives, the cancel is a no-op. If the handler respects context cancellation, it will stop work promptly.

---

## HTTP Management API

The management API exposes job lifecycle operations. It is intended for internal use (operators, support tooling) rather than external consumers.

| Method | Path | Description |
|---|---|---|
| `POST` | `/api/v1/jobs/:id/cancel?broadcast=true\|false` | Cancel a job. `broadcast=true` (default) signals all workers; `broadcast=false` skips broadcast for pending jobs. |
| `POST` | `/api/v1/jobs/:id/retry` | Re-enqueue a permanently failed job via an internal retry job |
| `GET` | `/api/v1/jobs/:id` | Get job status and attempt history |

### Cancel

The cancel endpoint creates and runs a `__system:cancel_job` in-process, which handles broadcast (if requested), DB update, and lease deletion atomically.

```go
func (api *API) Cancel(ctx context.Context, jobID uuid.UUID, broadcast bool) error {
    req, _ := json.Marshal(cancelJobRequest{TargetJobID: jobID, Broadcast: broadcast})
    _, err := api.sys.Run(ctx, api.db, "__system:cancel_job", systemNamespace, req)
    return err
}
```

### Retry

Retry creates a special `__system:retry_job` via `Dispatch()`. This retry job is entirely internal to the job system and can retry any target job. When a worker picks it up and executes it, the retry job:

1. Extends the target job's `max_attempts` budget by the original `max_attempts` value, so the job gets a fresh set of attempts starting from `last_attempt_no + 1`.
2. Re-creates an unlocked lease for the target job (via `Create`) so workers can claim it on the next poll cycle.
3. Circumvents normal backoff and max-attempts checks for the target job — the retry job directly manipulates the DB state to allow prompt re-execution.

Retried jobs can themselves be cancelled (via the cancel endpoint) and re-retried (by calling retry again).

```go
// retryJobRequest is the request payload for the internal retry job.
type retryJobRequest struct {
    TargetJobID uuid.UUID `json:"target_job_id"`
}

// registerSystemJobs also registers the retry job handler.
// (continued from the cancel_job registration above)
s.Register("__system:retry_job", jobs.JobFn(func(ctx context.Context, req retryJobRequest) (struct{}, error) {
    tx, err := s.db.BeginTx(ctx, nil)
    if err != nil {
        return struct{}{}, err
    }
    defer tx.Rollback()

    // Fetch current max_attempts and last attempt_no.
    row := tx.QueryRowContext(ctx,
        `SELECT j.max_attempts, COALESCE(MAX(a.attempt_no), 0)
         FROM jobs j
         LEFT JOIN job_attempts a ON a.job_id = j.id
         WHERE j.id = $1
         GROUP BY j.max_attempts`,
        req.TargetJobID,
    )
    var maxAttempts, lastAttemptNo int
    if err := row.Scan(&maxAttempts, &lastAttemptNo); err != nil {
        return struct{}{}, fmt.Errorf("fetch job state: %w", err)
    }

    nextAttemptNo := lastAttemptNo + 1

    // Extend the attempt budget so the job can run up to max_attempts more times.
    _, err = tx.ExecContext(ctx,
        `UPDATE jobs SET max_attempts = $1 WHERE id = $2`,
        nextAttemptNo+maxAttempts-1, req.TargetJobID,
    )
    if err != nil {
        return struct{}{}, fmt.Errorf("extend attempt budget: %w", err)
    }

    // Re-create the lease (unlocked) so workers can claim the target job again.
    // The job_attempts row for the next attempt will be inserted by the worker
    // that claims the lease, in the same tx as the lease acquisition.
    if _, err := s.leases.Create(ctx, tx, systemNamespace, req.TargetJobID.String()); err != nil {
        return struct{}{}, fmt.Errorf("re-create lease: %w", err)
    }

    return struct{}{}, tx.Commit()
}))
```

The retry endpoint simply dispatches the internal job and returns:

```go
func (api *API) Retry(ctx context.Context, jobID uuid.UUID) error {
    req, _ := json.Marshal(retryJobRequest{TargetJobID: jobID})
    _, err := api.sys.Dispatch(ctx, api.db, "__system:retry_job", systemNamespace, req)
    return err
}
```

---

## Observability

### Structured Logging

All worker and handler activity is logged using `slog`. Context is propagated through to handlers so that request-scoped values (trace IDs, user IDs) appear in logs. Always use the `Context` variants of slog methods (`InfoContext`, `ErrorContext`, `WarnContext`) so that context-attached values are included in log output.

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

## Dependency Injection

Handlers are registered as closures, capturing their dependencies (DB pool, HTTP clients, third-party SDKs) at startup. The job system has no knowledge of application-level dependencies — it only holds a map of `name → handler function`.

Wiring happens in a single location (e.g. `main.go` or a dedicated wiring file). This is the only site that knows about both the job system and application internals.

```go
// main.go or wiring.go

func wire(db *pgxpool.Pool, mailer *Mailer, billingClient *BillingClient) *jobs.System {
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

Each job file can export its own `Register()` function so job registration stays co-located with the job definition, without requiring all jobs to be wired in one central file:

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

Adding a new job type only requires calling its `Register()` function at startup; the central wiring file does not need to be modified.
