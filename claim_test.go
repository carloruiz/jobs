package jobs_test

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/carloruiz/jobs"
	"github.com/carloruiz/leases/leasestest"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// --------------------------------------------------------------------------
// Test infrastructure
// --------------------------------------------------------------------------

// applyMigrationsPool applies the schema migration files against the pool
// (auto-committed). Errors for already-existing objects are silently ignored
// so that tests can be run repeatedly against the same database.
func applyMigrationsPool(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	files := []string{
		"migrations/001_create_jobs.sql",
		"migrations/002_create_job_attempts.sql",
		"migrations/003_create_jobs_overview.sql",
	}
	for _, f := range files {
		sql, err := os.ReadFile(f)
		if err != nil {
			t.Fatalf("read migration %s: %v", f, err)
		}
		if _, err := pool.Exec(context.Background(), string(sql)); err != nil {
			if !strings.Contains(err.Error(), "already exists") {
				t.Fatalf("apply migration %s: %v", f, err)
			}
		}
	}
}

// cleanNamespace deletes all test rows for the given namespace, providing
// isolation between test runs without dropping the schema.
func cleanNamespace(t *testing.T, pool *pgxpool.Pool, ns string) {
	t.Helper()
	ctx := context.Background()
	pool.Exec(ctx, `DELETE FROM job_attempts WHERE job_id IN (SELECT id FROM jobs WHERE namespace = $1)`, ns)
	pool.Exec(ctx, `DELETE FROM jobs WHERE namespace = $1`, ns)
}

// setJobDeadline updates the deadline column on an existing job row.
// Used in tests to simulate a job whose deadline has passed after initial creation.
func setJobDeadline(t *testing.T, pool *pgxpool.Pool, jobID uuid.UUID, deadline time.Time) {
	t.Helper()
	if _, err := pool.Exec(context.Background(),
		`UPDATE jobs SET deadline = $1 WHERE id = $2`, deadline, jobID,
	); err != nil {
		t.Fatalf("set job deadline: %v", err)
	}
}

// waitForAttempt polls until the given attempt has a non-null finished_at,
// or fails the test if the deadline is exceeded.
func waitForAttempt(t *testing.T, pool *pgxpool.Pool, jobID uuid.UUID, attemptNo int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	for {
		var finishedAt *time.Time
		_ = pool.QueryRow(ctx,
			`SELECT finished_at FROM job_attempts WHERE job_id = $1 AND attempt_no = $2`,
			jobID, attemptNo,
		).Scan(&finishedAt)
		if finishedAt != nil {
			return
		}
		select {
		case <-time.After(10 * time.Millisecond):
		case <-ctx.Done():
			t.Fatal("attempt did not complete within timeout")
		}
	}
}

// --------------------------------------------------------------------------
// Integration tests (require TEST_DATABASE_URL)
// --------------------------------------------------------------------------

// TestIntegration_ClaimLoop_BasicExecution verifies the full happy path:
// a job dispatched through the runtime is executed and completed.
func TestIntegration_ClaimLoop_BasicExecution(t *testing.T) {
	pool := testPool(t)
	applyMigrationsPool(t, pool)
	const ns = "claim-basic"
	cleanNamespace(t, pool, ns)
	t.Cleanup(func() { cleanNamespace(t, pool, ns) })

	ls := leasestest.New()
	rt := jobs.NewRuntime(pool, ls, jobs.Config{
		Namespace:    ns,
		PollInterval: 50 * time.Millisecond,
		BatchSize:    5,
	})

	type req struct{ V int }
	type resp struct{ V int }
	done := make(chan struct{})
	jobs.Register(rt, "identity", jobs.JobConfig{MaxAttempts: 3}, func(_ context.Context, r req) (resp, error) {
		defer close(done)
		return resp{V: r.V}, nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	rt.Start(ctx)
	defer rt.Stop()

	job, err := rt.Dispatch(ctx, pool, "identity", req{V: 42}, "identity-basic-1")
	if err != nil {
		t.Fatalf("Dispatch: %v", err)
	}

	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("handler did not complete within timeout")
	}

	// Verify the attempt row is complete.
	var finishedAt *time.Time
	var response []byte
	err = pool.QueryRow(context.Background(),
		`SELECT finished_at, response FROM job_attempts WHERE job_id = $1 AND attempt_no = 1`,
		job.ID,
	).Scan(&finishedAt, &response)
	if err != nil {
		t.Fatalf("query attempt: %v", err)
	}
	if finishedAt == nil {
		t.Error("expected finished_at to be set")
	}
	if len(response) == 0 {
		t.Error("expected non-empty response")
	}
}

// TestIntegration_ClaimLoop_FailThenRetryComplete verifies that a transient
// failure results in a local retry that eventually succeeds.
func TestIntegration_ClaimLoop_FailThenRetryComplete(t *testing.T) {
	pool := testPool(t)
	applyMigrationsPool(t, pool)
	const ns = "claim-retry"
	cleanNamespace(t, pool, ns)
	t.Cleanup(func() { cleanNamespace(t, pool, ns) })

	ls := leasestest.New()
	rt := jobs.NewRuntime(pool, ls, jobs.Config{
		Namespace:    ns,
		PollInterval: 50 * time.Millisecond,
		BatchSize:    5,
	})

	type req struct{}
	type resp struct{ Done bool }

	attempt := 0
	done := make(chan struct{})
	jobs.Register(rt, "flaky", jobs.JobConfig{
		MaxAttempts:   3,
		BackoffPolicy: jobs.BackoffPolicy{}, // zero backoff for fast tests
	}, func(_ context.Context, _ req) (resp, error) {
		attempt++
		if attempt == 1 {
			return resp{}, errors.New("transient error")
		}
		defer close(done)
		return resp{Done: true}, nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	rt.Start(ctx)
	defer rt.Stop()

	job, err := rt.Dispatch(ctx, pool, "flaky", req{}, "flaky-retry-1")
	if err != nil {
		t.Fatalf("Dispatch: %v", err)
	}

	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("job did not complete within timeout")
	}

	// Verify two attempt rows exist: first with error, second with response.
	var attemptCount int
	if err := pool.QueryRow(context.Background(),
		`SELECT COUNT(*) FROM job_attempts WHERE job_id = $1`, job.ID,
	).Scan(&attemptCount); err != nil {
		t.Fatalf("count attempts: %v", err)
	}
	if attemptCount != 2 {
		t.Errorf("expected 2 attempts, got %d", attemptCount)
	}

	var firstError []byte
	var secondResponse []byte
	pool.QueryRow(context.Background(),
		`SELECT error FROM job_attempts WHERE job_id = $1 AND attempt_no = 1`, job.ID,
	).Scan(&firstError)
	pool.QueryRow(context.Background(),
		`SELECT response FROM job_attempts WHERE job_id = $1 AND attempt_no = 2`, job.ID,
	).Scan(&secondResponse)

	if len(firstError) == 0 {
		t.Error("expected attempt 1 to have an error")
	}
	if len(secondResponse) == 0 {
		t.Error("expected attempt 2 to have a response")
	}
}

// TestIntegration_ClaimLoop_DeadlineExceeded verifies that when the claim loop
// re-acquires a lease for a job whose deadline has since elapsed, the job is
// not re-executed and a terminal attempt is recorded.
func TestIntegration_ClaimLoop_DeadlineExceeded(t *testing.T) {
	pool := testPool(t)
	applyMigrationsPool(t, pool)
	const ns = "claim-deadline"
	cleanNamespace(t, pool, ns)
	t.Cleanup(func() { cleanNamespace(t, pool, ns) })

	ls := leasestest.New()
	rt := jobs.NewRuntime(pool, ls, jobs.Config{
		Namespace:    ns,
		PollInterval: 50 * time.Millisecond,
		BatchSize:    5,
	})

	execCount := 0
	jobs.Register(rt, "deadline_job", jobs.JobConfig{MaxAttempts: 3}, func(_ context.Context, _ struct{}) (struct{}, error) {
		execCount++
		return struct{}{}, nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Dispatch creates the job and runs attempt 1 successfully.
	job, err := rt.Dispatch(ctx, pool, "deadline_job", struct{}{}, "deadline-1")
	if err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	// Wait for attempt 1 to be committed before modifying the job.
	waitForAttempt(t, pool, job.ID, 1)

	// Simulate the job's deadline having elapsed since initial creation.
	setJobDeadline(t, pool, job.ID, time.Now().Add(-1*time.Hour))

	// Simulate a lease re-appearing (e.g. crash-recovery scenario where the
	// executing worker died and the lease expired).
	if _, err := ls.Create(context.Background(), pool, ns, job.ID.String()); err != nil {
		t.Fatalf("create lease: %v", err)
	}

	// Start the claim loop; it should detect the past deadline and reject the job.
	claimCtx, claimCancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer claimCancel()
	rt.Start(claimCtx)
	<-claimCtx.Done()
	rt.Stop()

	if execCount > 1 {
		t.Error("handler should not have been re-executed for a job past its deadline")
	}

	// A terminal attempt row should have been inserted by handleValidationError.
	var errPayload []byte
	pool.QueryRow(context.Background(),
		`SELECT error FROM job_attempts WHERE job_id = $1 AND attempt_no = 2`, job.ID,
	).Scan(&errPayload)
	if len(errPayload) == 0 {
		t.Error("expected terminal attempt (attempt_no=2) to have error set")
	}
}

// TestIntegration_ClaimLoop_MaxAttemptsExceeded verifies that when the claim loop
// re-acquires a lease for a job that has already exhausted its max attempts,
// the job is not re-executed and a terminal attempt is recorded.
func TestIntegration_ClaimLoop_MaxAttemptsExceeded(t *testing.T) {
	pool := testPool(t)
	applyMigrationsPool(t, pool)
	const ns = "claim-maxattempts"
	cleanNamespace(t, pool, ns)
	t.Cleanup(func() { cleanNamespace(t, pool, ns) })

	ls := leasestest.New()
	rt := jobs.NewRuntime(pool, ls, jobs.Config{
		Namespace:    ns,
		PollInterval: 50 * time.Millisecond,
		BatchSize:    5,
	})

	execCount := 0
	jobs.Register(rt, "exhausted_job", jobs.JobConfig{MaxAttempts: 1}, func(_ context.Context, _ struct{}) (struct{}, error) {
		execCount++
		return struct{}{}, errors.New("always fails")
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Dispatch creates the job and runs attempt 1 — it fails permanently since
	// MaxAttempts=1. The lease is deleted after the permanent failure.
	job, err := rt.Dispatch(ctx, pool, "exhausted_job", struct{}{}, "exhausted-1")
	if err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	// Wait for attempt 1 to be committed (with error) before proceeding.
	waitForAttempt(t, pool, job.ID, 1)

	// Simulate a lease re-appearing (e.g. after a crash).
	if _, err := ls.Create(context.Background(), pool, ns, job.ID.String()); err != nil {
		t.Fatalf("create lease: %v", err)
	}

	// Start the claim loop; it should detect max attempts exceeded and reject the job.
	claimCtx, claimCancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer claimCancel()
	rt.Start(claimCtx)
	<-claimCtx.Done()
	rt.Stop()

	if execCount > 1 {
		t.Error("handler should not have been re-executed for an exhausted job")
	}

	// Expect attempt_no=2 to be appended as a terminal row by handleValidationError.
	var terminalError []byte
	pool.QueryRow(context.Background(),
		`SELECT error FROM job_attempts WHERE job_id = $1 AND attempt_no = 2`, job.ID,
	).Scan(&terminalError)
	if len(terminalError) == 0 {
		t.Error("expected terminal attempt (attempt_no=2) to have error set")
	}
}
