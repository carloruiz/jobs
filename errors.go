package jobs

import "errors"

// Sentinel errors returned during claim-time validation.
var (
	// ErrMaxAttemptsExceeded is returned when a job has exceeded its configured
	// MaxAttempts and retry_until is nil or has elapsed.
	ErrMaxAttemptsExceeded = errors.New("max attempts exceeded")

	// ErrDeadlineExceeded is returned when a job's deadline has passed at
	// claim time.
	ErrDeadlineExceeded = errors.New("deadline exceeded")

	// ErrBackoffNotElapsed is returned when the elapsed time since the previous
	// attempt is less than the configured backoff delay. The lease is released
	// and the job will be re-encountered on the next poll cycle.
	ErrBackoffNotElapsed = errors.New("backoff not elapsed")

	// ErrStaleJobSkipped is returned in dev mode when a job is older than the
	// configured stale threshold.
	ErrStaleJobSkipped = errors.New("stale job skipped")
)
