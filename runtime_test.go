package jobs_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/carloruiz/jobs"
	"github.com/carloruiz/leases/leasestest"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// --------------------------------------------------------------------------
// Integration tests (require TEST_DATABASE_URL)
// --------------------------------------------------------------------------

// testPool returns a *pgxpool.Pool connected to TEST_DATABASE_URL, or skips.
func testPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL not set; skipping integration test")
	}
	pool, err := pgxpool.New(context.Background(), url)
	if err != nil {
		t.Fatalf("connect to test DB: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// applyMigrations loads and executes the schema migration files inside tx.
func applyMigrations(t *testing.T, tx pgx.Tx) {
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
		if _, err := tx.Exec(context.Background(), string(sql)); err != nil {
			t.Fatalf("apply migration %s: %v", f, err)
		}
	}
}

func TestIntegration_Dispatch_NewJob(t *testing.T) {
	pool := testPool(t)
	tx, err := pool.Begin(context.Background())
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	t.Cleanup(func() { tx.Rollback(context.Background()) })
	applyMigrations(t, tx)

	ls := leasestest.New()
	rt := jobs.NewRuntime(pool, ls, jobs.Config{Namespace: "test"})

	type req struct{ N int }
	type resp struct{ N int }
	done := make(chan struct{})
	jobs.Register(rt, "echo", jobs.JobConfig{MaxAttempts: 3}, func(_ context.Context, r req) (resp, error) {
		defer close(done)
		return resp{N: r.N}, nil
	})

	job, err := rt.Dispatch(context.Background(), pool, "echo", req{N: 42}, "echo-42")
	if err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	if job.ID == (uuid.UUID{}) {
		t.Error("expected non-zero job ID")
	}
	if job.IdempotencyKey != "echo-42" {
		t.Errorf("got IdempotencyKey=%q, want %q", job.IdempotencyKey, "echo-42")
	}

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("handler goroutine did not complete in time")
	}
}

func TestIntegration_Dispatch_IdempotencyKey(t *testing.T) {
	pool := testPool(t)
	tx, err := pool.Begin(context.Background())
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	t.Cleanup(func() { tx.Rollback(context.Background()) })
	applyMigrations(t, tx)

	ls := leasestest.New()
	rt := jobs.NewRuntime(pool, ls, jobs.Config{Namespace: "test"})
	jobs.Register(rt, "noop", jobs.JobConfig{MaxAttempts: 3}, func(_ context.Context, _ struct{}) (struct{}, error) {
		return struct{}{}, nil
	})

	job1, err := rt.Dispatch(context.Background(), pool, "noop", struct{}{}, "dedup-key")
	if err != nil {
		t.Fatalf("first Dispatch: %v", err)
	}

	job2, err := rt.Dispatch(context.Background(), pool, "noop", struct{}{}, "dedup-key")
	if err != nil {
		t.Fatalf("second Dispatch: %v", err)
	}

	if job1.ID != job2.ID {
		t.Errorf("expected same ID on duplicate dispatch; got %s and %s", job1.ID, job2.ID)
	}
}

func TestIntegration_Run_NewJob(t *testing.T) {
	pool := testPool(t)
	tx, err := pool.Begin(context.Background())
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	t.Cleanup(func() { tx.Rollback(context.Background()) })
	applyMigrations(t, tx)

	ls := leasestest.New()
	rt := jobs.NewRuntime(pool, ls, jobs.Config{Namespace: "test"})

	type req struct{ A, B int }
	type resp struct{ Sum int }
	jobs.Register(rt, "add", jobs.JobConfig{MaxAttempts: 3}, func(_ context.Context, r req) (resp, error) {
		return resp{Sum: r.A + r.B}, nil
	})

	var dest resp
	err = rt.Run(context.Background(), pool, "add", req{A: 3, B: 4}, "add-3-4", &dest)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if dest.Sum != 7 {
		t.Errorf("got Sum=%d, want 7", dest.Sum)
	}
}

func TestIntegration_Run_IdempotencyKey(t *testing.T) {
	pool := testPool(t)
	tx, err := pool.Begin(context.Background())
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	t.Cleanup(func() { tx.Rollback(context.Background()) })
	applyMigrations(t, tx)

	ls := leasestest.New()
	rt := jobs.NewRuntime(pool, ls, jobs.Config{
		Namespace:    "test",
		PollInterval: 50 * time.Millisecond,
	})

	type req struct{ V int }
	type resp struct{ V int }
	jobs.Register(rt, "identity", jobs.JobConfig{MaxAttempts: 3}, func(_ context.Context, r req) (resp, error) {
		return resp{V: r.V}, nil
	})

	// First Run: executes the job and stores the result.
	var dest1 resp
	if err := rt.Run(context.Background(), pool, "identity", req{V: 99}, "identity-99", &dest1); err != nil {
		t.Fatalf("first Run: %v", err)
	}

	// Second Run: detects duplicate and polls for the stored result.
	var dest2 resp
	if err := rt.Run(context.Background(), pool, "identity", req{V: 99}, "identity-99", &dest2); err != nil {
		t.Fatalf("second Run: %v", err)
	}

	if dest1.V != 99 || dest2.V != 99 {
		t.Errorf("got V=%d and V=%d, both want 99", dest1.V, dest2.V)
	}
}
