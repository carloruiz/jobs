package jobs

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// --------------------------------------------------------------------------
// Mock DB for poller unit tests
// --------------------------------------------------------------------------

// mockPollerDB implements the DB interface for the statusPoller. Only Query
// is used by the poller; all other methods are no-ops.
type mockPollerDB struct {
	mu       sync.Mutex
	queryCnt int32
	// results[i] is the set of (id, status) pairs returned on the i-th Query call.
	// Once exhausted, subsequent calls return empty rows.
	results [][]pollerRow
}

type pollerRow struct {
	id     uuid.UUID
	status string
}

func (m *mockPollerDB) queryCount() int {
	return int(atomic.LoadInt32(&m.queryCnt))
}

func (m *mockPollerDB) Query(_ context.Context, _ string, _ ...any) (pgx.Rows, error) {
	idx := int(atomic.AddInt32(&m.queryCnt, 1)) - 1
	m.mu.Lock()
	var rows []pollerRow
	if idx < len(m.results) {
		rows = m.results[idx]
	}
	m.mu.Unlock()
	return &mockPollerRows{rows: rows}, nil
}

func (m *mockPollerDB) Begin(_ context.Context) (pgx.Tx, error)                          { return nil, nil }
func (m *mockPollerDB) Exec(_ context.Context, _ string, _ ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, nil
}
func (m *mockPollerDB) QueryRow(_ context.Context, _ string, _ ...any) pgx.Row { return nil }

// mockPollerRows is a minimal pgx.Rows that yields a static slice of (uuid, string) pairs.
type mockPollerRows struct {
	rows []pollerRow
	idx  int
}

func (r *mockPollerRows) Next() bool  { return r.idx < len(r.rows) }
func (r *mockPollerRows) Close()      {}
func (r *mockPollerRows) Err() error  { return nil }

func (r *mockPollerRows) Scan(dest ...any) error {
	row := r.rows[r.idx]
	r.idx++
	*(dest[0].(*uuid.UUID)) = row.id
	*(dest[1].(*string)) = row.status
	return nil
}

func (r *mockPollerRows) CommandTag() pgconn.CommandTag            { return pgconn.CommandTag{} }
func (r *mockPollerRows) FieldDescriptions() []pgconn.FieldDescription { return nil }
func (r *mockPollerRows) Values() ([]any, error)                   { return nil, nil }
func (r *mockPollerRows) RawValues() [][]byte                      { return nil }
func (r *mockPollerRows) Conn() *pgx.Conn                          { return nil }

// --------------------------------------------------------------------------
// Tests
// --------------------------------------------------------------------------

// TestStatusPoller_NoSubscriberTick verifies that when there are no active
// subscriptions, tick() returns immediately without issuing any DB query.
func TestStatusPoller_NoSubscriberTick(t *testing.T) {
	db := &mockPollerDB{}
	p := newStatusPoller(db, "ns", 50*time.Millisecond)

	p.tick()

	if got := db.queryCount(); got != 0 {
		t.Errorf("expected 0 DB queries with no subscribers, got %d", got)
	}
}

// TestStatusPoller_ConcurrentPollers verifies that N concurrent subscribers
// for N distinct job IDs result in exactly 1 DB query per tick.
func TestStatusPoller_ConcurrentPollers(t *testing.T) {
	const N = 5
	ids := make([]uuid.UUID, N)
	rows := make([]pollerRow, N)
	for i := range ids {
		ids[i] = uuid.New()
		rows[i] = pollerRow{id: ids[i], status: StatusCompleted}
	}

	db := &mockPollerDB{
		results: [][]pollerRow{rows}, // one query returns all N rows
	}
	p := newStatusPoller(db, "ns", 50*time.Millisecond)

	// Subscribe to all N job IDs before ticking.
	channels := make([]<-chan pollerResult, N)
	cancels := make([]func(), N)
	for i, id := range ids {
		ch, cancel := p.Subscribe(id)
		channels[i] = ch
		cancels[i] = cancel
	}
	defer func() {
		for _, c := range cancels {
			c()
		}
	}()

	p.tick()

	if got := db.queryCount(); got != 1 {
		t.Errorf("expected exactly 1 DB query for %d subscribers, got %d", N, got)
	}

	// All N subscribers should receive their result.
	for i, ch := range channels {
		select {
		case result := <-ch:
			if result.status != StatusCompleted {
				t.Errorf("subscriber %d: got status %q, want %q", i, result.status, StatusCompleted)
			}
		default:
			t.Errorf("subscriber %d did not receive a result after tick", i)
		}
	}
}

// TestStatusPoller_TerminalStateFanOut verifies that when multiple subscribers
// are waiting on the same job ID, all of them receive the terminal result.
func TestStatusPoller_TerminalStateFanOut(t *testing.T) {
	jobID := uuid.New()
	db := &mockPollerDB{
		results: [][]pollerRow{
			{{id: jobID, status: StatusCompleted}},
		},
	}
	p := newStatusPoller(db, "ns", 50*time.Millisecond)

	const subscribers = 3
	channels := make([]<-chan pollerResult, subscribers)
	cancels := make([]func(), subscribers)
	for i := range channels {
		ch, cancel := p.Subscribe(jobID)
		channels[i] = ch
		cancels[i] = cancel
	}
	defer func() {
		for _, c := range cancels {
			c()
		}
	}()

	p.tick()

	for i, ch := range channels {
		select {
		case result := <-ch:
			if result.status != StatusCompleted {
				t.Errorf("subscriber %d: got status %q, want %q", i, result.status, StatusCompleted)
			}
		default:
			t.Errorf("subscriber %d did not receive a result after tick", i)
		}
	}
}

// TestStatusPoller_FailedJobFanOut verifies that a failed job fans out
// the "failed" status to all subscribers.
func TestStatusPoller_FailedJobFanOut(t *testing.T) {
	jobID := uuid.New()
	db := &mockPollerDB{
		results: [][]pollerRow{
			{{id: jobID, status: StatusFailed}},
		},
	}
	p := newStatusPoller(db, "ns", 50*time.Millisecond)

	ch1, cancel1 := p.Subscribe(jobID)
	ch2, cancel2 := p.Subscribe(jobID)
	defer cancel1()
	defer cancel2()

	p.tick()

	for i, ch := range []<-chan pollerResult{ch1, ch2} {
		select {
		case result := <-ch:
			if result.status != StatusFailed {
				t.Errorf("subscriber %d: got status %q, want %q", i, result.status, StatusFailed)
			}
		default:
			t.Errorf("subscriber %d did not receive a result after tick", i)
		}
	}
}

// TestStatusPoller_CancelRemovesSubscription verifies that calling cancel()
// removes the subscriber so it no longer receives results on subsequent ticks.
func TestStatusPoller_CancelRemovesSubscription(t *testing.T) {
	jobID := uuid.New()
	db := &mockPollerDB{
		results: [][]pollerRow{
			{{id: jobID, status: StatusCompleted}},
		},
	}
	p := newStatusPoller(db, "ns", 50*time.Millisecond)

	ch, cancel := p.Subscribe(jobID)
	cancel() // unsubscribe before tick

	p.tick()

	select {
	case result := <-ch:
		t.Errorf("cancelled subscriber received unexpected result: %v", result)
	default:
		// expected: no result delivered
	}
}

// TestStatusPoller_BackgroundLoop verifies that the background goroutine
// (started via Subscribe) delivers results without manually calling tick().
func TestStatusPoller_BackgroundLoop(t *testing.T) {
	jobID := uuid.New()
	db := &mockPollerDB{
		results: [][]pollerRow{
			{}, // first tick: job not yet terminal
			{{id: jobID, status: StatusCompleted}}, // second tick: terminal
		},
	}
	p := newStatusPoller(db, "ns", 30*time.Millisecond)
	defer p.Stop()

	ch, cancel := p.Subscribe(jobID)
	defer cancel()

	select {
	case result := <-ch:
		if result.status != StatusCompleted {
			t.Errorf("got status %q, want %q", result.status, StatusCompleted)
		}
	case <-time.After(2 * time.Second):
		t.Error("timed out waiting for poller to deliver terminal result")
	}
}
