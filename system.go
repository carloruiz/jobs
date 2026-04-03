package jobs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/carloruiz/leases"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

const leaseDuration = 30 * time.Second

var hostname = initHostname()

func initHostname() string {
	h, _ := os.Hostname()
	return h
}

// JobConfig holds per-job-type configuration set at registration time.
type JobConfig struct {
	// MaxAttempts is the maximum number of attempts for this job type.
	// Defaults to 3 if zero.
	MaxAttempts int
	// BackoffPolicy defines the retry delay schedule for this job type.
	BackoffPolicy BackoffPolicy
}

// Config holds startup configuration for a Runtime.
type Config struct {
	// Namespace is the logical grouping for all jobs dispatched by this Runtime.
	Namespace string
	// PollInterval controls how often the claim loop and completion pollers tick.
	// Defaults to 2s if zero.
	// TODO(PR 6): replaced by statusPoller which batches all active subscriptions.
	PollInterval time.Duration
	// HeartbeatInterval controls how often the heartbeat loop renews active leases.
	// Defaults to 10s if zero.
	HeartbeatInterval time.Duration
	// BatchSize is the maximum number of jobs claimed per poll cycle.
	// Defaults to 10 if zero.
	BatchSize int
	// BuildSHA is the git SHA of the binary used for tracing/attribution.
	// Set by the caller; leave empty if not needed.
	BuildSHA string
}

// registeredJob bundles a handler with its per-job-type configuration.
type registeredJob struct {
	handler Handler
	config  JobConfig
}

// activeJob holds the per-job state needed for heartbeating and cancellation.
type activeJob struct {
	cancel context.CancelFunc
	token  leases.LeaseToken
}

// Runtime is the top-level struct that handles both dispatching and executing
// jobs. Namespace is a system-wide configuration set at startup.
//
// TODO(PR 6): add statusPoller for batched completion polling.
type Runtime struct {
	namespace      string
	db             DB
	leases         leases.Store
	registry       map[string]registeredJob
	buildSHA       string
	pollInterval   time.Duration
	claimBatchSize int
	stopCh         chan struct{}

	heartbeatInterval time.Duration
	mu                sync.Mutex
	activeJobs        map[string]*activeJob
}

// NewRuntime constructs a Runtime with the given database, lease store, and config.
func NewRuntime(db DB, ls leases.Store, cfg Config) *Runtime {
	pollInterval := cfg.PollInterval
	if pollInterval <= 0 {
		pollInterval = 2 * time.Second
	}
	hbInterval := cfg.HeartbeatInterval
	if hbInterval <= 0 {
		hbInterval = 10 * time.Second
	}
	batchSize := cfg.BatchSize
	if batchSize <= 0 {
		batchSize = 10
	}
	return &Runtime{
		namespace:         cfg.Namespace,
		db:                db,
		leases:            ls,
		registry:          make(map[string]registeredJob),
		buildSHA:          cfg.BuildSHA,
		pollInterval:      pollInterval,
		heartbeatInterval: hbInterval,
		claimBatchSize:    batchSize,
		stopCh:            make(chan struct{}),
		activeJobs:        make(map[string]*activeJob),
	}
}

// Register registers a typed handler for the given job name with per-job
// configuration. Go infers the type parameters from fn, so callers never
// write explicit type annotations:
//
//	jobs.Register(rt, "send_email", jobs.JobConfig{MaxAttempts: 5}, sendEmailHandler)
func Register[Req, Resp any](r *Runtime, name string, cfg JobConfig, fn func(ctx context.Context, req Req) (Resp, error)) {
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = 3
	}
	r.registry[name] = registeredJob{handler: JobFn[Req, Resp](fn), config: cfg}
}

// Dispatch creates the job row and prefers to run the handler locally in a
// goroutine. If a job with the same (name, idempotencyKey) already exists, the
// existing job is returned immediately.
//
// Pass "" for idempotencyKey to auto-generate a UUID and opt out of
// deduplication — safe for fire-and-forget callers that do not retry.
//
// TODO(PR 7): use extractContext(ctx) to propagate caller context fields.
func (r *Runtime) Dispatch(
	ctx context.Context, db DB, name string, req any, idempotencyKey string,
) (*Job, error) {
	jreg, ok := r.registry[name]
	if !ok {
		return nil, fmt.Errorf("job %q is not registered", name)
	}

	raw, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	key := idempotencyKey
	if key == "" {
		key = uuid.NewString()
	}

	backoffJSON, err := json.Marshal(jreg.config.BackoffPolicy)
	if err != nil {
		return nil, fmt.Errorf("marshal backoff policy: %w", err)
	}

	tx, err := db.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	job, conflict, err := tryInsertJobRow(ctx, tx, insertJobParams{
		ID:          uuid.New(),
		Key:         key,
		Name:        name,
		Namespace:   r.namespace,
		BuildSHA:    r.buildSHA,
		CreatorHost: hostname,
		Request:     raw,
		MaxAttempts: jreg.config.MaxAttempts,
		BackoffJSON: backoffJSON,
	})
	if err != nil {
		return nil, fmt.Errorf("insert job: %w", err)
	}
	if conflict {
		// A job with this key already exists — fetch and return it.
		// No writes were made; let the deferred rollback clean up the tx.
		job, err = fetchJobByKey(ctx, tx, name, key)
		if err != nil {
			return nil, err
		}
		return &job, nil
	}

	lease, err := r.leases.CreateAndAcquire(ctx, tx, r.namespace, job.ID.String(), hostname, leaseDuration)
	if err != nil {
		return nil, fmt.Errorf("create and acquire lease: %w", err)
	}

	if err := insertJobAttemptRow(ctx, tx, insertAttemptParams{JobID: job.ID, AttemptNo: 1, Host: hostname, SHA: r.buildSHA}); err != nil {
		return nil, fmt.Errorf("insert attempt: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit: %w", err)
	}

	job.Request = raw
	job.BackoffPolicy = jreg.config.BackoffPolicy
	job.MaxAttempts = jreg.config.MaxAttempts

	attempt := &Attempt{
		JobID:      job.ID,
		AttemptNo:  1,
		Request:    raw,
		LeaseToken: lease.Token,
	}
	go r.runWithRetry(context.WithoutCancel(ctx), db, &job, attempt)
	return &job, nil
}

// Run creates the job, acquires the lease locally, and executes the handler
// in-process, blocking until completion. idempotencyKey is required.
//
// If a job with this (name, idempotencyKey) already exists, Run polls for the
// result of the existing execution rather than launching a duplicate.
// dest is a pointer to the response type; the result is unmarshaled into it.
//
// TODO(PR 7): use extractContext(ctx) to propagate caller context fields.
func (r *Runtime) Run(
	ctx context.Context, db DB, name string, req any, idempotencyKey string, dest any,
) error {
	if idempotencyKey == "" {
		return fmt.Errorf("Run: idempotencyKey is required")
	}

	jreg, ok := r.registry[name]
	if !ok {
		return fmt.Errorf("job %q is not registered", name)
	}

	raw, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}

	backoffJSON, err := json.Marshal(jreg.config.BackoffPolicy)
	if err != nil {
		return fmt.Errorf("marshal backoff policy: %w", err)
	}

	tx, err := db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	job, conflict, err := tryInsertJobRow(ctx, tx, insertJobParams{
		ID:          uuid.New(),
		Key:         idempotencyKey,
		Name:        name,
		Namespace:   r.namespace,
		BuildSHA:    r.buildSHA,
		CreatorHost: hostname,
		Request:     raw,
		MaxAttempts: jreg.config.MaxAttempts,
		BackoffJSON: backoffJSON,
	})
	if err != nil {
		return fmt.Errorf("insert job: %w", err)
	}
	if conflict {
		// Job already exists — poll for its result.
		// No writes were made; let the deferred rollback clean up the tx.
		job, err = fetchJobByKey(ctx, tx, name, idempotencyKey)
		if err != nil {
			return err
		}
		resultRaw, err := r.pollForResult(ctx, job.ID)
		if err != nil {
			return err
		}
		return json.Unmarshal(resultRaw, dest)
	}

	lease, err := r.leases.CreateAndAcquire(ctx, tx, r.namespace, job.ID.String(), hostname, leaseDuration)
	if err != nil {
		return fmt.Errorf("create and acquire lease: %w", err)
	}

	if err := insertJobAttemptRow(ctx, tx, insertAttemptParams{JobID: job.ID, AttemptNo: 1, Host: hostname, SHA: r.buildSHA}); err != nil {
		return fmt.Errorf("insert attempt: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit: %w", err)
	}

	job.Request = raw
	job.BackoffPolicy = jreg.config.BackoffPolicy
	job.MaxAttempts = jreg.config.MaxAttempts

	attempt := &Attempt{
		JobID:      job.ID,
		AttemptNo:  1,
		Request:    raw,
		LeaseToken: lease.Token,
	}

	resultRaw, err := r.runWithRetry(ctx, db, &job, attempt)
	if err != nil {
		return err
	}
	return json.Unmarshal(resultRaw, dest)
}

// pollForCompletion polls jobs_overview until the job reaches a terminal state.
// Used by Dispatch() when checking on a running duplicate.
// TODO(PR 5/6): replace with job_status table + statusPoller.
func (r *Runtime) pollForCompletion(ctx context.Context, jobID uuid.UUID) error {
	ticker := time.NewTicker(r.pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			var status string
			err := r.db.QueryRow(ctx,
				`SELECT status FROM jobs_overview WHERE job_id = $1`, jobID,
			).Scan(&status)
			if errors.Is(err, pgx.ErrNoRows) {
				continue // job not yet visible
			}
			if err != nil {
				return fmt.Errorf("poll status: %w", err)
			}
			switch status {
			case "complete":
				return nil
			case "failed":
				return fmt.Errorf("job %s permanently failed", jobID)
			}
			// pending, running, pending_retry — keep polling
		}
	}
}

// pollForResult polls for completion then fetches the response payload.
// Used by Run() when a duplicate idempotency key is detected.
// TODO(PR 5/6): replace with job_status table + statusPoller.
func (r *Runtime) pollForResult(ctx context.Context, jobID uuid.UUID) (json.RawMessage, error) {
	if err := r.pollForCompletion(ctx, jobID); err != nil {
		return nil, err
	}
	var resp json.RawMessage
	err := r.db.QueryRow(ctx,
		`SELECT response FROM job_attempts WHERE job_id = $1 ORDER BY attempt_no DESC LIMIT 1`,
		jobID,
	).Scan(&resp)
	return resp, err
}

// runWithRetry executes the handler, retrying locally on transient failure.
// The lease is held for the full retry loop: deleted on success or permanent
// failure, released on context cancellation (graceful termination).
//
// TODO(PR 8): integrate with graceful shutdown WaitGroup.
func (r *Runtime) runWithRetry(ctx context.Context, db DB, job *Job, attempt *Attempt) (json.RawMessage, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	r.registerActiveJob(job.ID, cancel, attempt.LeaseToken)
	defer r.deregisterActiveJob(job.ID)

	for {
		resp, execErr := r.registry[job.Name].handler.Handle(ctx, attempt.Request)
		if execErr == nil {
			if err := r.complete(ctx, db, job, attempt, resp); err != nil {
				return nil, fmt.Errorf("complete: %w", err)
			}
			return resp, nil
		}

		// Record the failure atomically; permanent indicates no retries remain.
		permanent, err := r.fail(ctx, db, job, attempt, execErr)
		if err != nil {
			return nil, fmt.Errorf("record failure: %w", err)
		}
		if permanent {
			return nil, execErr
		}

		// Transient failure: sleep backoff then insert the next attempt and retry.
		backoff := r.backoffFor(job, attempt.AttemptNo)
		select {
		case <-ctx.Done():
			// Graceful termination: release lease so another worker can re-claim.
			_ = r.leases.Release(context.Background(), r.db, job.ID.String(), attempt.LeaseToken)
			return nil, ctx.Err()
		case <-time.After(backoff):
		}

		nextNo := attempt.AttemptNo + 1
		if err := insertJobAttemptRow(ctx, db, insertAttemptParams{JobID: job.ID, AttemptNo: nextNo, Host: hostname, SHA: r.buildSHA}); err != nil {
			return nil, fmt.Errorf("insert next attempt: %w", err)
		}

		attempt = &Attempt{
			JobID:      job.ID,
			AttemptNo:  nextNo,
			Request:    attempt.Request,
			LeaseToken: attempt.LeaseToken,
		}
	}
}

// complete writes the response and deletes the lease atomically.
func (r *Runtime) complete(ctx context.Context, db DB, job *Job, attempt *Attempt, resp json.RawMessage) error {
	tx, err := db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx,
		`UPDATE job_attempts SET response = $1, finished_at = now()
		 WHERE job_id = $2 AND attempt_no = $3`,
		resp, attempt.JobID, attempt.AttemptNo,
	); err != nil {
		return err
	}
	if err := r.leases.Delete(ctx, tx, job.ID.String()); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// fail records the error on the current attempt and returns (permanent, err).
// permanent is true when no retries remain. On permanent failure, the lease is
// deleted inside the transaction. On transient failure, the lease is retained;
// keepRetrying sleeps the backoff and retries locally.
func (r *Runtime) fail(ctx context.Context, db DB, job *Job, attempt *Attempt, execErr error) (bool, error) {
	keepRetrying := attempt.AttemptNo < job.MaxAttempts ||
		(job.RetryUntil != nil && time.Now().Before(*job.RetryUntil))

	errJSON, err := json.Marshal(map[string]string{"message": execErr.Error()})
	if err != nil {
		return false, err
	}

	tx, err := db.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx,
		`UPDATE job_attempts SET error = $1, finished_at = now()
		 WHERE job_id = $2 AND attempt_no = $3`,
		errJSON, attempt.JobID, attempt.AttemptNo,
	); err != nil {
		return false, err
	}

	if !keepRetrying {
		// Permanent failure — delete the lease so no worker ever re-claims it.
		if err := r.leases.Delete(ctx, tx, job.ID.String()); err != nil {
			return false, err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return false, err
	}
	return !keepRetrying, nil
}

// backoffFor returns the delay before retrying attempt number attemptNo (1-based).
// The delay is capped by BackoffPolicy.MaxInterval when non-zero.
func (r *Runtime) backoffFor(job *Job, attemptNo int) time.Duration {
	return job.BackoffPolicy.Duration(attemptNo - 1)
}
