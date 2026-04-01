package jobs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/carloruiz/leases"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// claimedJob bundles the job metadata with its next attempt, produced by
// claimBatch and handed off to runWithRetry.
type claimedJob struct {
	job     *Job
	attempt *Attempt
}

// Start launches the background claim loop. It polls for available jobs at
// Config.PollInterval and executes each in a goroutine. Start is
// non-blocking; call Stop to terminate the loop.
//
// TODO(PR 4): start heartbeat loop alongside the claim loop.
// TODO(PR 8): drain in-flight goroutines on shutdown.
func (r *Runtime) Start(ctx context.Context) {
	go r.claimLoop(ctx)
}

// Stop signals the background claim loop to exit. Safe to call multiple times.
func (r *Runtime) Stop() {
	select {
	case <-r.stopCh:
		// already stopped
	default:
		close(r.stopCh)
	}
}

func (r *Runtime) claimLoop(ctx context.Context) {
	ticker := time.NewTicker(r.pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-r.stopCh:
			return
		case <-ticker.C:
			claimed, err := r.claimBatch(ctx)
			if err != nil {
				// TODO(PR 7): structured logging
				continue
			}
			for _, cj := range claimed {
				cj := cj
				// TODO(PR 4): register in activeJobs before launching.
				go r.runWithRetry(ctx, r.db, cj.job, cj.attempt)
			}
		}
	}
}

// claimBatch acquires up to batchSize available leases for this namespace,
// validates each corresponding job, and inserts a new attempt row for each
// valid job. Lease acquisition and attempt insertion are atomic within a
// single transaction.
//
// Jobs that fail validation are handled inline (lease released or permanently
// failed) and are not returned to the caller.
func (r *Runtime) claimBatch(ctx context.Context) ([]claimedJob, error) {
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	acquiredLeases, err := r.leases.AcquireMany(ctx, tx, r.namespace, r.batchSize, hostname, leaseDuration)
	if err != nil {
		return nil, fmt.Errorf("acquire leases: %w", err)
	}
	if len(acquiredLeases) == 0 {
		return nil, nil
	}

	jobIDs := make([]uuid.UUID, 0, len(acquiredLeases))
	tokenByID := make(map[uuid.UUID]leases.LeaseToken, len(acquiredLeases))
	for _, l := range acquiredLeases {
		id, parseErr := uuid.Parse(l.Resource)
		if parseErr != nil {
			continue
		}
		jobIDs = append(jobIDs, id)
		tokenByID[id] = l.Token
	}

	rows, err := tx.Query(ctx, `
		SELECT j.id, j.name, j.namespace, j.max_attempts, j.retry_until,
		       j.backoff_policy, j.deadline, j.request, j.created_at,
		       COALESCE(a.attempt_no, 0), a.started_at
		FROM jobs j
		LEFT JOIN LATERAL (
		    SELECT attempt_no, started_at FROM job_attempts
		    WHERE job_id = j.id
		    ORDER BY attempt_no DESC LIMIT 1
		) a ON true
		WHERE j.id = ANY($1)`,
		jobIDs,
	)
	if err != nil {
		return nil, fmt.Errorf("load jobs: %w", err)
	}

	type scannedRow struct {
		job           Job
		lastAttemptNo int
		lastStartedAt *time.Time
	}
	var scanned []scannedRow
	for rows.Next() {
		var sr scannedRow
		var backoffJSON []byte
		if scanErr := rows.Scan(
			&sr.job.ID, &sr.job.Name, &sr.job.Namespace,
			&sr.job.MaxAttempts, &sr.job.RetryUntil,
			&backoffJSON, &sr.job.Deadline, &sr.job.Request, &sr.job.CreatedAt,
			&sr.lastAttemptNo, &sr.lastStartedAt,
		); scanErr != nil {
			rows.Close()
			return nil, fmt.Errorf("scan job row: %w", scanErr)
		}
		if parseErr := json.Unmarshal(backoffJSON, &sr.job.BackoffPolicy); parseErr != nil {
			rows.Close()
			return nil, fmt.Errorf("unmarshal backoff policy for job %s: %w", sr.job.ID, parseErr)
		}
		scanned = append(scanned, sr)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows: %w", err)
	}

	var claimed []claimedJob
	for _, sr := range scanned {
		job := sr.job
		nextAttemptNo := sr.lastAttemptNo + 1
		token := tokenByID[job.ID]

		if valErr := r.validate(&job, nextAttemptNo, sr.lastStartedAt); valErr != nil {
			r.handleValidationError(ctx, tx, &job, sr.lastAttemptNo, token, valErr)
			continue
		}

		if _, err := tx.Exec(ctx,
			`INSERT INTO job_attempts (job_id, attempt_no, executor_host, executor_sha) VALUES ($1, $2, $3, $4)`,
			job.ID, nextAttemptNo, hostname, r.buildSHA,
		); err != nil {
			return nil, fmt.Errorf("insert attempt for job %s: %w", job.ID, err)
		}

		jobCopy := job
		claimed = append(claimed, claimedJob{
			job: &jobCopy,
			attempt: &Attempt{
				JobID:      job.ID,
				AttemptNo:  nextAttemptNo,
				Request:    job.Request,
				LeaseToken: token,
			},
		})
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit: %w", err)
	}
	return claimed, nil
}

// validate checks whether a job should be executed at the given attempt number.
// Returns nil when execution should proceed, or a sentinel error from errors.go
// when the job should be skipped or permanently failed.
//
// nextAttemptNo is 1-based. lastStartedAt is the started_at of the most recent
// attempt, or nil if no attempt has been made yet.
func (r *Runtime) validate(job *Job, nextAttemptNo int, lastStartedAt *time.Time) error {
	if _, ok := r.registry[job.Name]; !ok {
		return fmt.Errorf("unknown job type %q", job.Name)
	}
	if nextAttemptNo > job.MaxAttempts {
		if job.RetryUntil == nil || time.Now().After(*job.RetryUntil) {
			return ErrMaxAttemptsExceeded
		}
	}
	if job.Deadline != nil && time.Now().After(*job.Deadline) {
		return ErrDeadlineExceeded
	}
	// Backoff check: only applies when there has been a previous attempt.
	if lastStartedAt != nil && nextAttemptNo >= 2 {
		// Delay index is 0-based: before attempt N (1-based), use index N-2.
		delay := job.BackoffPolicy.Duration(nextAttemptNo - 2)
		if time.Now().Before(lastStartedAt.Add(delay)) {
			return ErrBackoffNotElapsed
		}
	}
	if r.devMode && time.Since(job.CreatedAt) > r.staleThreshold {
		return ErrStaleJobSkipped
	}
	return nil
}

// handleValidationError handles a failed validate() result inside claimBatch's
// transaction. Releases or permanently fails the job depending on the error type.
func (r *Runtime) handleValidationError(
	ctx context.Context, tx pgx.Tx, job *Job, lastAttemptNo int, token leases.LeaseToken, err error,
) {
	switch {
	case errors.Is(err, ErrBackoffNotElapsed), errors.Is(err, ErrStaleJobSkipped):
		// Transient skip: release the lease so another worker can re-claim later.
		_ = r.leases.Release(ctx, tx, job.ID.String(), token)

	case errors.Is(err, ErrMaxAttemptsExceeded), errors.Is(err, ErrDeadlineExceeded):
		// Permanent failure: record the terminal error on the last attempt (if
		// it hasn't already been finished), then delete the lease.
		if lastAttemptNo > 0 {
			errJSON, _ := json.Marshal(map[string]string{"message": err.Error()})
			_, _ = tx.Exec(ctx,
				`UPDATE job_attempts
				 SET error = $1, finished_at = COALESCE(finished_at, now())
				 WHERE job_id = $2 AND attempt_no = $3`,
				errJSON, job.ID, lastAttemptNo,
			)
		}
		_ = r.leases.Delete(ctx, tx, job.ID.String())

	default:
		// Unknown job type or unexpected error: release and skip.
		_ = r.leases.Release(ctx, tx, job.ID.String(), token)
	}
}
