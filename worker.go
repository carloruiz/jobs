package jobs

import (
	"context"
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

// Start launches the background claim loop and heartbeat loop. Both poll at
// Config.PollInterval and Config.HeartbeatInterval respectively. Start is
// non-blocking; call Stop to terminate both loops.
//
// TODO(PR 8): drain in-flight goroutines on shutdown.
func (r *Runtime) Start(ctx context.Context) {
	go r.claimLoop(ctx)
	go r.heartbeatLoop(ctx)
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
				go r.runWithRetry(ctx, r.db, cj.job, cj.attempt)
			}
		}
	}
}

// acquireAndLoad acquires up to batchSize available leases for this namespace
// and loads the corresponding job rows. Returns the job candidates and a map
// of job ID to lease token.
func (r *Runtime) acquireAndLoad(ctx context.Context, tx pgx.Tx) ([]scannedJobRow, map[uuid.UUID]leases.LeaseToken, error) {
	acquiredLeases, err := r.leases.AcquireMany(ctx, tx, r.namespace, r.claimBatchSize, hostname, leaseDuration)
	if err != nil {
		return nil, nil, fmt.Errorf("acquire leases: %w", err)
	}
	if len(acquiredLeases) == 0 {
		return nil, nil, nil
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
		return nil, nil, fmt.Errorf("load jobs: %w", err)
	}

	var jobCandidates []scannedJobRow
	for rows.Next() {
		sr, scanErr := scanJobRow(rows)
		if scanErr != nil {
			rows.Close()
			return nil, nil, scanErr
		}
		jobCandidates = append(jobCandidates, sr)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("rows: %w", err)
	}

	return jobCandidates, tokenByID, nil
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

	jobCandidates, tokenByID, err := r.acquireAndLoad(ctx, tx)
	if err != nil {
		return nil, err
	}
	if len(jobCandidates) == 0 {
		return nil, nil
	}

	var claimed []claimedJob
	for _, sr := range jobCandidates {
		job := sr.job
		nextAttemptNo := sr.lastAttemptNo + 1
		token := tokenByID[job.ID]

		if valErr := r.validate(&job, nextAttemptNo, sr.lastStartedAt); valErr != nil {
			if handleErr := r.handleValidationError(ctx, tx, &job, sr.lastAttemptNo, token, valErr); handleErr != nil {
				// TODO(PR 7): replace with structured logging
				fmt.Printf("handleValidationError for job %s: %v\n", job.ID, handleErr)
			}
			continue
		}

		// TODO(future): batch these inserts into a single round-trip.
		if err := insertJobAttemptRow(ctx, tx, insertAttemptParams{JobID: job.ID, AttemptNo: nextAttemptNo, Host: hostname, SHA: r.buildSHA}); err != nil {
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
	return nil
}

// handleValidationError handles a failed validate() result inside claimBatch's
// transaction. Releases or permanently fails the job depending on the error type.
// Returns an error if any DB operation fails; the caller is responsible for logging.
func (r *Runtime) handleValidationError(
	ctx context.Context, tx pgx.Tx, job *Job, lastAttemptNo int, token leases.LeaseToken, err error,
) error {
	switch {
	case errors.Is(err, ErrBackoffNotElapsed):
		// Transient skip: release the lease so another worker can re-claim later.
		return r.leases.Release(ctx, tx, job.ID.String(), token)

	case errors.Is(err, ErrMaxAttemptsExceeded), errors.Is(err, ErrDeadlineExceeded):
		// Permanent failure: append a terminal attempt row recording the error,
		// then delete the lease. The attempts table is append-only; never update.
		if insertErr := insertFailedAttemptRow(ctx, tx, insertFailedAttemptParams{
			JobID:     job.ID,
			AttemptNo: lastAttemptNo + 1,
			Host:      hostname,
			SHA:       r.buildSHA,
			ErrMsg:    err.Error(),
		}); insertErr != nil {
			return insertErr
		}
		return r.leases.Delete(ctx, tx, job.ID.String())

	default:
		// Unknown job type or unexpected error: release and skip.
		return r.leases.Release(ctx, tx, job.ID.String(), token)
	}
}
