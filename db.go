package jobs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// scannedJobRow holds the result of one row from the claimBatch query, bundling
// job metadata with the last attempt's sequence number and start time.
type scannedJobRow struct {
	job           Job
	lastAttemptNo int
	lastStartedAt *time.Time
}

// scanJobRow scans one row from the jobs+LATERAL job_attempts query used in
// claimBatch, including JSON unmarshaling of BackoffPolicy.
func scanJobRow(rows pgx.Rows) (scannedJobRow, error) {
	var sr scannedJobRow
	var backoffJSON []byte
	if err := rows.Scan(
		&sr.job.ID, &sr.job.Name, &sr.job.Namespace,
		&sr.job.MaxAttempts, &sr.job.RetryUntil,
		&backoffJSON, &sr.job.Deadline, &sr.job.Request, &sr.job.CreatedAt,
		&sr.lastAttemptNo, &sr.lastStartedAt,
	); err != nil {
		return scannedJobRow{}, fmt.Errorf("scan job row: %w", err)
	}
	if err := json.Unmarshal(backoffJSON, &sr.job.BackoffPolicy); err != nil {
		return scannedJobRow{}, fmt.Errorf("unmarshal backoff policy for job %s: %w", sr.job.ID, err)
	}
	return sr, nil
}

// insertAttemptRow inserts a new row into job_attempts.
func insertAttemptRow(ctx context.Context, q DB, jobID uuid.UUID, attemptNo int, host, sha string) error {
	_, err := q.Exec(ctx,
		`INSERT INTO job_attempts (job_id, attempt_no, executor_host, executor_sha) VALUES ($1, $2, $3, $4)`,
		jobID, attemptNo, host, sha,
	)
	return err
}

// insertFailedAttemptRow inserts a terminal row into job_attempts with the
// error field set and finished_at stamped to now.
func insertFailedAttemptRow(ctx context.Context, q DB, jobID uuid.UUID, attemptNo int, host, sha string, errMsg string) error {
	errJSON, _ := json.Marshal(map[string]string{"message": errMsg})
	_, err := q.Exec(ctx,
		`INSERT INTO job_attempts (job_id, attempt_no, executor_host, executor_sha, error, finished_at) VALUES ($1, $2, $3, $4, $5, now())`,
		jobID, attemptNo, host, sha, errJSON,
	)
	return err
}

// upsertJobRow inserts a new job row. Returns the inserted Job and false on
// success. Returns an empty Job and true (conflict=true) when a row with the
// same (name, idempotency_key) already exists.
func upsertJobRow(
	ctx context.Context, tx pgx.Tx,
	id uuid.UUID, key, name, namespace, buildSHA, creatorHost string,
	raw json.RawMessage, maxAttempts int, backoffJSON []byte,
) (Job, bool, error) {
	var job Job
	err := tx.QueryRow(ctx, `
		INSERT INTO jobs (id, idempotency_key, name, namespace, creator_sha, creator_host, request, max_attempts, backoff_policy)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		ON CONFLICT (name, idempotency_key) DO NOTHING
		RETURNING id, idempotency_key, name, namespace`,
		id, key, name, namespace, buildSHA, creatorHost, raw, maxAttempts, backoffJSON,
	).Scan(&job.ID, &job.IdempotencyKey, &job.Name, &job.Namespace)
	if errors.Is(err, pgx.ErrNoRows) {
		return Job{}, true, nil
	}
	if err != nil {
		return Job{}, false, err
	}
	return job, false, nil
}

// fetchJobByKey fetches the minimal job fields (id, idempotency_key, name,
// namespace) for a job identified by name and idempotency key.
func fetchJobByKey(ctx context.Context, tx pgx.Tx, name, key string) (Job, error) {
	var job Job
	err := tx.QueryRow(ctx,
		`SELECT id, idempotency_key, name, namespace FROM jobs WHERE name = $1 AND idempotency_key = $2`,
		name, key,
	).Scan(&job.ID, &job.IdempotencyKey, &job.Name, &job.Namespace)
	if err != nil {
		return Job{}, fmt.Errorf("fetch existing job: %w", err)
	}
	return job, nil
}
