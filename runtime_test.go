package jobs_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/carloruiz/jobs"
	"github.com/carloruiz/leases"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// --------------------------------------------------------------------------
// Fakes
// --------------------------------------------------------------------------

// fakeLeaseStore is an in-memory leases.Store for unit tests.
// It tracks held resources but does not interact with a database.
type fakeLeaseStore struct {
	mu                  sync.Mutex
	held                map[string]leases.LeaseToken
	CreateAndAcquireErr error
}

func newFakeLeaseStore() *fakeLeaseStore {
	return &fakeLeaseStore{held: make(map[string]leases.LeaseToken)}
}

func (f *fakeLeaseStore) Create(_ context.Context, _ leases.DBTX, _, resource string) (*leases.Lease, error) {
	return &leases.Lease{}, nil
}

func (f *fakeLeaseStore) CreateAndAcquire(_ context.Context, _ leases.DBTX, _, resource, _ string, _ time.Duration) (*leases.Lease, error) {
	if f.CreateAndAcquireErr != nil {
		return nil, f.CreateAndAcquireErr
	}
	token := leases.LeaseToken(uuid.NewString())
	f.mu.Lock()
	f.held[resource] = token
	f.mu.Unlock()
	return &leases.Lease{Token: token}, nil
}

func (f *fakeLeaseStore) Delete(_ context.Context, _ leases.DBTX, resource string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.held, resource)
	return nil
}

func (f *fakeLeaseStore) Acquire(_ context.Context, _ leases.DBTX, resource, _ string, _ time.Duration) (*leases.Lease, error) {
	token := leases.LeaseToken(uuid.NewString())
	f.mu.Lock()
	f.held[resource] = token
	f.mu.Unlock()
	return &leases.Lease{Token: token}, nil
}

func (f *fakeLeaseStore) Release(_ context.Context, _ leases.DBTX, resource string, _ leases.LeaseToken) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.held, resource)
	return nil
}

func (f *fakeLeaseStore) AcquireMany(_ context.Context, _ leases.DBTX, _ string, _ int, _ string, _ time.Duration) ([]leases.Lease, error) {
	return nil, nil
}

func (f *fakeLeaseStore) Heartbeat(_ context.Context, _ leases.DBTX, _ string, _ leases.LeaseToken, _ time.Duration) (*leases.Lease, error) {
	return &leases.Lease{}, nil
}

func (f *fakeLeaseStore) HeartbeatMany(_ context.Context, _ leases.DBTX, _ []leases.HeartbeatRequest, _ time.Duration) ([]leases.Lease, error) {
	return nil, nil
}

// fakeTx implements pgx.Tx for unit tests. It records Exec/QueryRow calls and
// allows callers to inject behaviour via queryRowFn.
type fakeTx struct {
	execCalls  []string
	committed  bool
	rolledBack bool
	callCount  int
	queryRowFn func(call int, sql string, args ...any) pgx.Row
}

func (t *fakeTx) Begin(_ context.Context) (pgx.Tx, error)  { return t, nil }
func (t *fakeTx) Commit(_ context.Context) error           { t.committed = true; return nil }
func (t *fakeTx) Rollback(_ context.Context) error         { t.rolledBack = true; return nil }
func (t *fakeTx) Conn() *pgx.Conn                          { return nil }
func (t *fakeTx) LargeObjects() pgx.LargeObjects           { return pgx.LargeObjects{} }
func (t *fakeTx) SendBatch(_ context.Context, _ *pgx.Batch) pgx.BatchResults { return nil }
func (t *fakeTx) CopyFrom(_ context.Context, _ pgx.Identifier, _ []string, _ pgx.CopyFromSource) (int64, error) {
	return 0, errors.New("fakeTx.CopyFrom not implemented")
}
func (t *fakeTx) Prepare(_ context.Context, _, _ string) (*pgconn.StatementDescription, error) {
	return nil, errors.New("fakeTx.Prepare not implemented")
}
func (t *fakeTx) Exec(_ context.Context, sql string, _ ...any) (pgconn.CommandTag, error) {
	t.execCalls = append(t.execCalls, sql)
	return pgconn.NewCommandTag("INSERT 1"), nil
}
func (t *fakeTx) Query(_ context.Context, _ string, _ ...any) (pgx.Rows, error) {
	return nil, errors.New("fakeTx.Query not implemented")
}
func (t *fakeTx) QueryRow(_ context.Context, sql string, args ...any) pgx.Row {
	t.callCount++
	if t.queryRowFn != nil {
		return t.queryRowFn(t.callCount, sql, args...)
	}
	return errRow{errors.New("fakeTx: no queryRowFn configured")}
}

// errRow implements pgx.Row and always returns the given error from Scan.
type errRow struct{ err error }

func (r errRow) Scan(_ ...any) error { return r.err }

// scanRow implements pgx.Row and calls fn with the scan destinations.
type scanRow struct {
	fn func(dest ...any) error
}

func (r scanRow) Scan(dest ...any) error { return r.fn(dest...) }

// fakeDB is a minimal jobs.DB backed by a single shared fakeTx.
// All calls — transactional or not — are routed through the same fakeTx so
// tests can assert on a single shared queryRowFn and callCount.
type fakeDB struct {
	tx *fakeTx
}

func (d *fakeDB) Begin(_ context.Context) (pgx.Tx, error) { return d.tx, nil }
func (d *fakeDB) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	return d.tx.Exec(ctx, sql, args...)
}
func (d *fakeDB) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return d.tx.Query(ctx, sql, args...)
}
func (d *fakeDB) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return d.tx.QueryRow(ctx, sql, args...)
}

// --------------------------------------------------------------------------
// Unit tests (no database required)
// --------------------------------------------------------------------------

func TestRun_EmptyIdempotencyKey(t *testing.T) {
	rt := jobs.NewRuntime(nil, nil, jobs.Config{})
	err := rt.Run(context.Background(), nil, "noop", struct{}{}, "", nil)
	if err == nil {
		t.Fatal("expected error for empty idempotencyKey, got nil")
	}
}

func TestDispatch_AutoGeneratesKey(t *testing.T) {
	// When idempotencyKey is "", Dispatch should auto-generate a UUID key and
	// proceed to insert without any conflict.

	tx := &fakeTx{
		queryRowFn: func(call int, sql string, args ...any) pgx.Row {
			if call == 1 {
				// INSERT ... ON CONFLICT ... RETURNING — return a successful row.
				return scanRow{fn: func(dest ...any) error {
					*dest[0].(*uuid.UUID) = uuid.New()
					*dest[1].(*string) = "auto-generated-key"
					*dest[2].(*string) = "noop"
					*dest[3].(*string) = "test"
					return nil
				}}
			}
			return errRow{fmt.Errorf("unexpected QueryRow call #%d", call)}
		},
	}

	db := &fakeDB{tx: tx}
	ls := newFakeLeaseStore()
	rt := jobs.NewRuntime(db, ls, jobs.Config{Namespace: "test"})
	jobs.Register(rt, "noop", func(_ context.Context, _ struct{}) (struct{}, error) {
		return struct{}{}, nil
	})

	job, err := rt.Dispatch(context.Background(), db, "noop", struct{}{}, "")
	if err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	if job == nil {
		t.Fatal("expected non-nil job")
	}
	if !tx.committed {
		t.Error("expected transaction to be committed")
	}
}

func TestDispatch_ExistingKey_ReturnsDuplicate(t *testing.T) {
	// When the INSERT returns pgx.ErrNoRows (conflict), Dispatch should fall
	// back to SELECT and return the existing job without re-running it.
	existingID := uuid.New()

	tx := &fakeTx{
		queryRowFn: func(call int, sql string, args ...any) pgx.Row {
			switch call {
			case 1:
				// INSERT conflicts — no row returned.
				return errRow{pgx.ErrNoRows}
			case 2:
				// SELECT existing job.
				return scanRow{fn: func(dest ...any) error {
					*dest[0].(*uuid.UUID) = existingID
					*dest[1].(*string) = "my-key"
					*dest[2].(*string) = "noop"
					*dest[3].(*string) = "test"
					return nil
				}}
			}
			return errRow{fmt.Errorf("unexpected QueryRow call #%d", call)}
		},
	}

	db := &fakeDB{tx: tx}
	rt := jobs.NewRuntime(db, newFakeLeaseStore(), jobs.Config{Namespace: "test"})

	job, err := rt.Dispatch(context.Background(), db, "noop", struct{}{}, "my-key")
	if err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	if job.ID != existingID {
		t.Errorf("got ID=%s, want %s", job.ID, existingID)
	}
	if job.IdempotencyKey != "my-key" {
		t.Errorf("got IdempotencyKey=%q, want %q", job.IdempotencyKey, "my-key")
	}
	if tx.callCount != 2 {
		t.Errorf("expected 2 QueryRow calls (INSERT + SELECT), got %d", tx.callCount)
	}
}

func TestRun_ExistingKey_PollsForResult(t *testing.T) {
	// When Run detects a conflict, it should poll jobs_overview until complete
	// and unmarshal the stored response.
	existingID := uuid.New()

	type addResp struct{ Sum int }
	respJSON, _ := json.Marshal(addResp{Sum: 7})

	// tx handles both the initial conflict path and poll queries.
	// pollForResult uses r.db (same pool), so we share the same fakeTx.
	tx := &fakeTx{
		queryRowFn: func(call int, sql string, args ...any) pgx.Row {
			switch call {
			case 1:
				// INSERT: conflict.
				return errRow{pgx.ErrNoRows}
			case 2:
				// SELECT existing job.
				return scanRow{fn: func(dest ...any) error {
					*dest[0].(*uuid.UUID) = existingID
					*dest[1].(*string) = "add"
					*dest[2].(*string) = "add"
					*dest[3].(*string) = "test"
					return nil
				}}
			case 3:
				// pollForResult: jobs_overview status.
				return scanRow{fn: func(dest ...any) error {
					*dest[0].(*string) = "complete"
					return nil
				}}
			case 4:
				// pollForResult: job_attempts response.
				return scanRow{fn: func(dest ...any) error {
					*dest[0].(*json.RawMessage) = respJSON
					return nil
				}}
			}
			return errRow{fmt.Errorf("unexpected QueryRow call #%d", call)}
		},
	}

	db := &fakeDB{tx: tx}
	rt := jobs.NewRuntime(db, newFakeLeaseStore(), jobs.Config{
		Namespace:    "test",
		PollInterval: 1 * time.Millisecond,
	})

	var dest addResp
	err := rt.Run(context.Background(), db, "add", struct{}{}, "add-key", &dest)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if dest.Sum != 7 {
		t.Errorf("got Sum=%d, want 7", dest.Sum)
	}
}

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

// applyMigrations runs the three schema migrations inside tx.
func applyMigrations(t *testing.T, tx pgx.Tx) {
	t.Helper()
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS jobs (
			id               UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
			idempotency_key  TEXT        NOT NULL,
			name             TEXT        NOT NULL,
			namespace        TEXT        NOT NULL,
			metadata         JSONB,
			request          JSONB       NOT NULL,
			max_attempts     INT         NOT NULL,
			retry_until      TIMESTAMPTZ,
			created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
			creator_sha      TEXT        NOT NULL,
			creator_host     TEXT        NOT NULL,
			backoff_policy   JSONB       NOT NULL,
			deadline         TIMESTAMPTZ,
			UNIQUE (name, idempotency_key)
		)`,
		`CREATE TABLE IF NOT EXISTS job_attempts (
			job_id           UUID        NOT NULL REFERENCES jobs(id),
			attempt_no       INT         NOT NULL,
			response         JSONB,
			error            JSONB,
			created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
			started_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
			finished_at      TIMESTAMPTZ,
			executor_host    TEXT        NOT NULL,
			executor_sha     TEXT        NOT NULL,
			PRIMARY KEY (job_id, attempt_no)
		)`,
		`CREATE OR REPLACE VIEW jobs_overview AS
		SELECT j.id AS job_id, j.name, j.namespace, j.max_attempts, j.retry_until,
		       j.deadline, j.created_at, a.attempt_no, j.request,
		       a.response, a.error, a.started_at, a.finished_at, a.executor_host,
		       CASE
		           WHEN a.attempt_no IS NULL                   THEN 'pending'
		           WHEN a.finished_at IS NULL                  THEN 'running'
		           WHEN a.error IS NOT NULL
		                AND a.attempt_no >= j.max_attempts
		                AND (j.retry_until IS NULL OR now() >= j.retry_until) THEN 'failed'
		           WHEN a.error IS NOT NULL                    THEN 'pending_retry'
		           ELSE                                             'complete'
		       END AS status
		FROM jobs j
		LEFT JOIN LATERAL (
		    SELECT * FROM job_attempts WHERE job_id = j.id
		    ORDER BY attempt_no DESC LIMIT 1
		) a ON true`,
	}
	for _, s := range stmts {
		if _, err := tx.Exec(context.Background(), s); err != nil {
			t.Fatalf("migration: %v", err)
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

	ls := newFakeLeaseStore()
	rt := jobs.NewRuntime(pool, ls, jobs.Config{Namespace: "test"})

	type req struct{ N int }
	type resp struct{ N int }
	done := make(chan struct{})
	jobs.Register(rt, "echo", func(_ context.Context, r req) (resp, error) {
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

	ls := newFakeLeaseStore()
	rt := jobs.NewRuntime(pool, ls, jobs.Config{Namespace: "test"})
	jobs.Register(rt, "noop", func(_ context.Context, _ struct{}) (struct{}, error) {
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

	ls := newFakeLeaseStore()
	rt := jobs.NewRuntime(pool, ls, jobs.Config{Namespace: "test"})

	type req struct{ A, B int }
	type resp struct{ Sum int }
	jobs.Register(rt, "add", func(_ context.Context, r req) (resp, error) {
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

	ls := newFakeLeaseStore()
	rt := jobs.NewRuntime(pool, ls, jobs.Config{
		Namespace:    "test",
		PollInterval: 50 * time.Millisecond,
	})

	type req struct{ V int }
	type resp struct{ V int }
	jobs.Register(rt, "identity", func(_ context.Context, r req) (resp, error) {
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
