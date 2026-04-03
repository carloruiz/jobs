package jobs_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/carloruiz/jobs"
	"github.com/carloruiz/leases/leasestest"
	"github.com/google/uuid"
)

// waitForRunning polls GetJobStatus until the job is observed in "running"
// state, or fails the test if the context deadline is exceeded.
func waitForRunning(t *testing.T, rt *jobs.Runtime, jobID uuid.UUID) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	for {
		result, err := rt.GetJobStatus(ctx, jobID)
		if err == nil && result.Status == jobs.StatusRunning {
			return
		}
		select {
		case <-time.After(10 * time.Millisecond):
		case <-ctx.Done():
			t.Fatalf("timeout waiting for job %s to reach running state", jobID)
		}
	}
}

// --------------------------------------------------------------------------
// Lifecycle transition tests
// --------------------------------------------------------------------------

// TestJobStatus_Dispatch_Running verifies that Dispatch writes "running" to
// job_status in the same transaction as the attempt insert.
func TestJobStatus_Dispatch_Running(t *testing.T) {
	pool := testPool(t)
	applyMigrationsPool(t, pool)
	const ns = "status-dispatch-running"
	cleanNamespace(t, pool, ns)
	t.Cleanup(func() { cleanNamespace(t, pool, ns) })

	ls := leasestest.New()
	rt := jobs.NewRuntime(pool, ls, jobs.Config{
		Namespace:    ns,
		PollInterval: 50 * time.Millisecond,
	})

	// Block the handler so the job stays in "running" long enough to inspect.
	block := make(chan struct{})
	jobs.Register(rt, "slow", jobs.JobConfig{MaxAttempts: 1}, func(ctx context.Context, _ struct{}) (struct{}, error) {
		select {
		case <-block:
		case <-ctx.Done():
		}
		return struct{}{}, nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	job, err := rt.Dispatch(ctx, pool, "slow", struct{}{}, "dispatch-running-1")
	if err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	defer close(block)

	waitForRunning(t, rt, job.ID)
}

// TestJobStatus_Run_Completed verifies that a successfully completed job has
// "completed" status in job_status.
func TestJobStatus_Run_Completed(t *testing.T) {
	pool := testPool(t)
	applyMigrationsPool(t, pool)
	const ns = "status-run-completed"
	cleanNamespace(t, pool, ns)
	t.Cleanup(func() { cleanNamespace(t, pool, ns) })

	ls := leasestest.New()
	rt := jobs.NewRuntime(pool, ls, jobs.Config{
		Namespace:    ns,
		PollInterval: 50 * time.Millisecond,
	})

	type req struct{ V int }
	type resp struct{ V int }
	jobs.Register(rt, "echo", jobs.JobConfig{MaxAttempts: 1}, func(_ context.Context, r req) (resp, error) {
		return resp{V: r.V}, nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var dest resp
	if err := rt.Run(ctx, pool, "echo", req{V: 99}, "run-completed-1", &dest); err != nil {
		t.Fatalf("Run: %v", err)
	}

	var jobID uuid.UUID
	pool.QueryRow(context.Background(),
		`SELECT id FROM jobs WHERE namespace = $1 AND idempotency_key = $2`, ns, "run-completed-1",
	).Scan(&jobID)

	result, err := rt.GetJobStatus(context.Background(), jobID)
	if err != nil {
		t.Fatalf("GetJobStatus: %v", err)
	}
	if result.Status != jobs.StatusCompleted {
		t.Errorf("want status %q, got %q", jobs.StatusCompleted, result.Status)
	}
	if dest.V != 99 {
		t.Errorf("want response V=99, got V=%d", dest.V)
	}
}

// TestJobStatus_PermanentFailure_Failed verifies that a job that exhausts its
// max attempts is written as "failed" in job_status.
func TestJobStatus_PermanentFailure_Failed(t *testing.T) {
	pool := testPool(t)
	applyMigrationsPool(t, pool)
	const ns = "status-permanent-fail"
	cleanNamespace(t, pool, ns)
	t.Cleanup(func() { cleanNamespace(t, pool, ns) })

	ls := leasestest.New()
	rt := jobs.NewRuntime(pool, ls, jobs.Config{
		Namespace:    ns,
		PollInterval: 50 * time.Millisecond,
	})

	jobs.Register(rt, "always_fail", jobs.JobConfig{MaxAttempts: 1}, func(_ context.Context, _ struct{}) (struct{}, error) {
		return struct{}{}, errors.New("permanent error")
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	job, err := rt.Dispatch(ctx, pool, "always_fail", struct{}{}, "perm-fail-1")
	if err != nil {
		t.Fatalf("Dispatch: %v", err)
	}

	success, err := rt.WaitForCompletion(ctx, job.ID)
	if err != nil {
		t.Fatalf("WaitForCompletion: %v", err)
	}
	if success {
		t.Errorf("expected job to fail, got success")
	}
}

// TestJobStatus_ClaimLoop_Running verifies that when the claim loop picks up a
// job, it writes "running" to job_status inside the claim transaction.
func TestJobStatus_ClaimLoop_Running(t *testing.T) {
	pool := testPool(t)
	applyMigrationsPool(t, pool)
	const ns = "status-claim-running"
	cleanNamespace(t, pool, ns)
	t.Cleanup(func() { cleanNamespace(t, pool, ns) })

	ls := leasestest.New()
	rt := jobs.NewRuntime(pool, ls, jobs.Config{
		Namespace:    ns,
		PollInterval: 50 * time.Millisecond,
		BatchSize:    5,
	})

	block := make(chan struct{})
	jobs.Register(rt, "blockable", jobs.JobConfig{MaxAttempts: 1}, func(ctx context.Context, _ struct{}) (struct{}, error) {
		select {
		case <-block:
		case <-ctx.Done():
		}
		return struct{}{}, nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	rt.Start(ctx)
	defer rt.Stop()

	job, err := rt.Dispatch(ctx, pool, "blockable", struct{}{}, "claim-running-1")
	if err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	defer close(block)

	waitForRunning(t, rt, job.ID)
}

// TestJobStatus_ValidationFailure_Failed verifies that when the claim loop
// rejects a job due to max-attempts exceeded, it writes "failed" to job_status.
func TestJobStatus_ValidationFailure_Failed(t *testing.T) {
	pool := testPool(t)
	applyMigrationsPool(t, pool)
	const ns = "status-validation-fail"
	cleanNamespace(t, pool, ns)
	t.Cleanup(func() { cleanNamespace(t, pool, ns) })

	ls := leasestest.New()
	rt := jobs.NewRuntime(pool, ls, jobs.Config{
		Namespace:    ns,
		PollInterval: 50 * time.Millisecond,
		BatchSize:    5,
	})

	jobs.Register(rt, "exhausted", jobs.JobConfig{MaxAttempts: 1}, func(_ context.Context, _ struct{}) (struct{}, error) {
		return struct{}{}, errors.New("always fails")
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Dispatch: runs attempt 1, fails permanently (MaxAttempts=1).
	job, err := rt.Dispatch(ctx, pool, "exhausted", struct{}{}, "validation-fail-1")
	if err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	waitForAttempt(t, pool, job.ID, 1)

	// Simulate lease re-appearance so the claim loop picks it up again.
	if _, err := ls.Create(context.Background(), pool, ns, job.ID.String()); err != nil {
		t.Fatalf("create lease: %v", err)
	}

	claimCtx, claimCancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer claimCancel()
	rt.Start(claimCtx)
	<-claimCtx.Done()
	rt.Stop()

	// job_status should be "failed" (set by handleValidationError).
	result, err := rt.GetJobStatus(context.Background(), job.ID)
	if err != nil {
		t.Fatalf("GetJobStatus: %v", err)
	}
	if result.Status != jobs.StatusFailed {
		t.Errorf("want status %q, got %q", jobs.StatusFailed, result.Status)
	}
}

// --------------------------------------------------------------------------
// HTTP handler tests
// --------------------------------------------------------------------------

// TestAPI_HandleJobStatus_NotFound verifies the handler returns 404 for a
// job ID that has no row in job_status (job not yet claimed).
func TestAPI_HandleJobStatus_NotFound(t *testing.T) {
	pool := testPool(t)
	applyMigrationsPool(t, pool)

	ls := leasestest.New()
	rt := jobs.NewRuntime(pool, ls, jobs.Config{})
	api := jobs.NewAPI(rt)
	mux := http.NewServeMux()
	api.RegisterRoutes(mux)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/jobs/"+uuid.NewString()+"/status", nil)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Errorf("want 404, got %d", rr.Code)
	}
}

// TestAPI_HandleJobStatus_InvalidID verifies the handler returns 400 for a
// non-UUID path segment.
func TestAPI_HandleJobStatus_InvalidID(t *testing.T) {
	pool := testPool(t)
	applyMigrationsPool(t, pool)

	ls := leasestest.New()
	rt := jobs.NewRuntime(pool, ls, jobs.Config{})
	api := jobs.NewAPI(rt)
	mux := http.NewServeMux()
	api.RegisterRoutes(mux)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/jobs/not-a-uuid/status", nil)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("want 400, got %d", rr.Code)
	}
}

// TestAPI_HandleJobStatus_OK verifies the handler returns 200 with the correct
// status JSON after a job completes.
func TestAPI_HandleJobStatus_OK(t *testing.T) {
	pool := testPool(t)
	applyMigrationsPool(t, pool)
	const ns = "status-api-ok"
	cleanNamespace(t, pool, ns)
	t.Cleanup(func() { cleanNamespace(t, pool, ns) })

	ls := leasestest.New()
	rt := jobs.NewRuntime(pool, ls, jobs.Config{
		Namespace:    ns,
		PollInterval: 50 * time.Millisecond,
	})

	type req struct{}
	type resp struct{ Done bool }
	jobs.Register(rt, "simple", jobs.JobConfig{MaxAttempts: 1}, func(_ context.Context, _ req) (resp, error) {
		return resp{Done: true}, nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var dest resp
	if err := rt.Run(ctx, pool, "simple", req{}, "api-ok-1", &dest); err != nil {
		t.Fatalf("Run: %v", err)
	}

	var jobID uuid.UUID
	pool.QueryRow(context.Background(),
		`SELECT id FROM jobs WHERE namespace = $1 AND idempotency_key = $2`, ns, "api-ok-1",
	).Scan(&jobID)

	api := jobs.NewAPI(rt)
	mux := http.NewServeMux()
	api.RegisterRoutes(mux)

	req2 := httptest.NewRequest("GET", "/api/v1/jobs/"+jobID.String()+"/status", nil)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req2)

	if rr.Code != http.StatusOK {
		t.Errorf("want 200, got %d: %s", rr.Code, rr.Body.String())
	}
	if ct := rr.Header().Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Errorf("want Content-Type application/json, got %q", ct)
	}
	body := rr.Body.String()
	if !strings.Contains(body, `"completed"`) {
		t.Errorf("expected status 'completed' in response body: %s", body)
	}
}
