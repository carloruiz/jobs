package jobs_test

import (
	"context"
	"encoding/json"
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

// insertJob inserts a job row directly into the DB and returns its ID.
func insertJob(
	t *testing.T, pool *pgxpool.Pool,
	name, namespace string, maxAttempts int,
	backoff jobs.BackoffPolicy, deadline *time.Time,
) uuid.UUID {
	t.Helper()
	bpJSON, err := json.Marshal(backoff)
	if err != nil {
		t.Fatalf("marshal backoff: %v", err)
	}
	var id uuid.UUID
	err = pool.QueryRow(context.Background(), `
		INSERT INTO jobs (idempotency_key, name, namespace, request, max_attempts, creator_sha, creator_host, backoff_policy, deadline)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		RETURNING id`,
		uuid.NewString(), name, namespace, []byte(`{}`), maxAttempts, "", "test-host", bpJSON, deadline,
	).Scan(&id)
	if err != nil {
		t.Fatalf("insert job: %v", err)
	}
	return id
}

// insertFailedAttempt inserts a completed-but-failed attempt row.
func insertFailedAttempt(t *testing.T, pool *pgxpool.Pool, jobID uuid.UUID, attemptNo int, errMsg string) {
	t.Helper()
	errJSON, _ := json.Marshal(map[string]string{"message": errMsg})
	if _, err := pool.Exec(context.Background(), `
		INSERT INTO job_attempts (job_id, attempt_no, executor_host, executor_sha, error, finished_at)
		VALUES ($1, $2, $3, $4, $5, now())`,
		jobID, attemptNo, "test-host", "", errJSON,
	); err != nil {
		t.Fatalf("insert failed attempt: %v", err)
	}
}

// --------------------------------------------------------------------------
// Integration tests (require TEST_DATABASE_URL)
// --------------------------------------------------------------------------

// TestIntegration_ClaimLoop_BasicExecution verifies the full happy path:
// a job with an available lease is claimed, executed, and completed.
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

	// Insert a job and create an available lease for it.
	jobID := insertJob(t, pool, "identity", ns, 3, jobs.BackoffPolicy{}, nil)
	if _, err := ls.Create(context.Background(), pool, ns, jobID.String()); err != nil {
		t.Fatalf("create lease: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	rt.Start(ctx)
	defer rt.Stop()

	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("handler did not complete within timeout")
	}

	// Verify the attempt row is complete.
	var finishedAt *time.Time
	var response []byte
	err := pool.QueryRow(context.Background(),
		`SELECT finished_at, response FROM job_attempts WHERE job_id = $1 AND attempt_no = 1`,
		jobID,
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

	jobID := insertJob(t, pool, "flaky", ns, 3, jobs.BackoffPolicy{}, nil)
	if _, err := ls.Create(context.Background(), pool, ns, jobID.String()); err != nil {
		t.Fatalf("create lease: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	rt.Start(ctx)
	defer rt.Stop()

	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("job did not complete within timeout")
	}

	// Verify two attempt rows exist: first with error, second with response.
	var attemptCount int
	if err := pool.QueryRow(context.Background(),
		`SELECT COUNT(*) FROM job_attempts WHERE job_id = $1`, jobID,
	).Scan(&attemptCount); err != nil {
		t.Fatalf("count attempts: %v", err)
	}
	if attemptCount != 2 {
		t.Errorf("expected 2 attempts, got %d", attemptCount)
	}

	var firstError []byte
	var secondResponse []byte
	pool.QueryRow(context.Background(),
		`SELECT error FROM job_attempts WHERE job_id = $1 AND attempt_no = 1`, jobID,
	).Scan(&firstError)
	pool.QueryRow(context.Background(),
		`SELECT response FROM job_attempts WHERE job_id = $1 AND attempt_no = 2`, jobID,
	).Scan(&secondResponse)

	if len(firstError) == 0 {
		t.Error("expected attempt 1 to have an error")
	}
	if len(secondResponse) == 0 {
		t.Error("expected attempt 2 to have a response")
	}
}

// TestIntegration_ClaimLoop_DeadlineExceeded verifies that a job with a
// past deadline is not executed and its lease is deleted.
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

	executed := false
	jobs.Register(rt, "deadline_job", jobs.JobConfig{MaxAttempts: 3}, func(_ context.Context, _ struct{}) (struct{}, error) {
		executed = true
		return struct{}{}, nil
	})

	// Create a job with a deadline in the past.
	past := time.Now().Add(-1 * time.Hour)
	jobID := insertJob(t, pool, "deadline_job", ns, 3, jobs.BackoffPolicy{}, &past)
	if _, err := ls.Create(context.Background(), pool, ns, jobID.String()); err != nil {
		t.Fatalf("create lease: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	rt.Start(ctx)
	<-ctx.Done()
	rt.Stop()

	if executed {
		t.Error("handler should not have been called for a job past its deadline")
	}

	// A single terminal attempt row should have been inserted with the error.
	var attemptCount int
	pool.QueryRow(context.Background(),
		`SELECT COUNT(*) FROM job_attempts WHERE job_id = $1`, jobID,
	).Scan(&attemptCount)
	if attemptCount != 1 {
		t.Errorf("expected 1 terminal attempt for a deadline-exceeded job, got %d", attemptCount)
	}

	var errPayload []byte
	pool.QueryRow(context.Background(),
		`SELECT error FROM job_attempts WHERE job_id = $1 AND attempt_no = 1`, jobID,
	).Scan(&errPayload)
	if len(errPayload) == 0 {
		t.Error("expected terminal attempt to have error set")
	}
}

// TestIntegration_ClaimLoop_MaxAttemptsExceeded verifies that a job which has
// already exhausted its max attempts is not re-executed when the lease expires
// and is re-acquired.
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

	executed := false
	jobs.Register(rt, "exhausted_job", jobs.JobConfig{MaxAttempts: 1}, func(_ context.Context, _ struct{}) (struct{}, error) {
		executed = true
		return struct{}{}, nil
	})

	// Insert a job that already has 1 failed attempt (max attempts = 1).
	jobID := insertJob(t, pool, "exhausted_job", ns, 1, jobs.BackoffPolicy{}, nil)
	insertFailedAttempt(t, pool, jobID, 1, "previous failure")

	// Simulate a lease re-appearing (e.g. lease expiry after a crash).
	if _, err := ls.Create(context.Background(), pool, ns, jobID.String()); err != nil {
		t.Fatalf("create lease: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	rt.Start(ctx)
	<-ctx.Done()
	rt.Stop()

	if executed {
		t.Error("handler should not have been called for an exhausted job")
	}

	// Expect 2 attempt rows: the original failed attempt plus the terminal row
	// appended by handleValidationError when max attempts is exceeded.
	var attemptCount int
	pool.QueryRow(context.Background(),
		`SELECT COUNT(*) FROM job_attempts WHERE job_id = $1`, jobID,
	).Scan(&attemptCount)
	if attemptCount != 2 {
		t.Errorf("expected 2 attempts (original + terminal), got %d", attemptCount)
	}

	var terminalError []byte
	pool.QueryRow(context.Background(),
		`SELECT error FROM job_attempts WHERE job_id = $1 AND attempt_no = 2`, jobID,
	).Scan(&terminalError)
	if len(terminalError) == 0 {
		t.Error("expected terminal attempt (attempt_no=2) to have error set")
	}
}
