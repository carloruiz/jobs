package jobs

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/carloruiz/leases"
	"github.com/carloruiz/leases/leasestest"
	"github.com/google/uuid"
)

// --------------------------------------------------------------------------
// Test helpers
// --------------------------------------------------------------------------

// heartbeatSpy wraps leasestest.Store and intercepts HeartbeatMany calls so
// that tests can observe call counts and inject errors.
type heartbeatSpy struct {
	leases.Store

	mu    sync.Mutex
	calls int
	err   error // returned by every HeartbeatMany call when non-nil
}

func newHeartbeatSpy() *heartbeatSpy {
	return &heartbeatSpy{Store: leasestest.New()}
}

func (s *heartbeatSpy) HeartbeatMany(
	ctx context.Context,
	db leases.DBTX,
	items []leases.HeartbeatRequest,
	duration time.Duration,
) ([]leases.Lease, error) {
	s.mu.Lock()
	s.calls++
	err := s.err
	s.mu.Unlock()
	return nil, err
}

func (s *heartbeatSpy) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

func (s *heartbeatSpy) setErr(err error) {
	s.mu.Lock()
	s.err = err
	s.mu.Unlock()
}

// isStopped reports whether the runtime's stop channel has been closed.
func isStopped(r *Runtime) bool {
	select {
	case <-r.stopCh:
		return true
	default:
		return false
	}
}

// activeJobCount returns the number of entries in the activeJobs map.
func activeJobCount(r *Runtime) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.activeJobs)
}

// waitFor polls cond until it returns true or the deadline elapses.
func waitFor(t *testing.T, deadline time.Duration, cond func() bool) bool {
	t.Helper()
	end := time.Now().Add(deadline)
	for time.Now().Before(end) {
		if cond() {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return false
}

// --------------------------------------------------------------------------
// Tests
// --------------------------------------------------------------------------

// TestHeartbeatLoop_Ticks verifies that HeartbeatMany is called periodically
// when at least one job is registered in activeJobs.
func TestHeartbeatLoop_Ticks(t *testing.T) {
	spy := newHeartbeatSpy()
	rt := NewRuntime(nil, spy, Config{
		Namespace:         "hb-ticks",
		PollInterval:      50 * time.Millisecond,
		HeartbeatInterval: 50 * time.Millisecond,
	})

	// Register a synthetic active job directly (no DB or Dispatch needed).
	jobID := uuid.New()
	token := leases.LeaseToken(uuid.NewString())
	rt.registerActiveJob(jobID, func() {}, token)
	defer rt.deregisterActiveJob(jobID)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	rt.Start(ctx)
	defer rt.Stop()

	// HeartbeatMany should be called at least once within the timeout.
	if !waitFor(t, 2*time.Second, func() bool { return spy.callCount() >= 1 }) {
		t.Errorf("expected HeartbeatMany to be called at least once, got %d calls", spy.callCount())
	}
}

// TestHeartbeatLoop_NoCallsWhenIdle verifies that HeartbeatMany is NOT called
// when there are no active jobs.
func TestHeartbeatLoop_NoCallsWhenIdle(t *testing.T) {
	spy := newHeartbeatSpy()
	rt := NewRuntime(nil, spy, Config{
		Namespace:         "hb-idle",
		PollInterval:      50 * time.Millisecond,
		HeartbeatInterval: 50 * time.Millisecond,
	})

	// No active jobs registered.
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	rt.Start(ctx)
	<-ctx.Done()
	rt.Stop()

	if spy.callCount() > 0 {
		t.Errorf("expected no HeartbeatMany calls with no active jobs, got %d", spy.callCount())
	}
}

// TestHeartbeatLoop_SelfTermination verifies that after maxHeartbeatFailures
// consecutive HeartbeatMany errors, the runtime calls Stop() (self-terminates).
func TestHeartbeatLoop_SelfTermination(t *testing.T) {
	spy := newHeartbeatSpy()
	spy.setErr(errors.New("db unavailable"))

	rt := NewRuntime(nil, spy, Config{
		Namespace:         "hb-terminate",
		PollInterval:      50 * time.Millisecond,
		HeartbeatInterval: 50 * time.Millisecond,
	})

	jobID := uuid.New()
	token := leases.LeaseToken(uuid.NewString())
	rt.registerActiveJob(jobID, func() {}, token)
	defer rt.deregisterActiveJob(jobID)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	rt.Start(ctx)

	// After maxHeartbeatFailures (3) ticks the runtime should call Stop().
	if !waitFor(t, 2*time.Second, func() bool { return isStopped(rt) }) {
		t.Errorf("runtime did not self-terminate after consecutive heartbeat failures (got %d HeartbeatMany calls)", spy.callCount())
	}
}

// TestHeartbeatLoop_FailureCounterResets verifies that a successful heartbeat
// resets the consecutive failure counter so that the runtime does not terminate
// after non-consecutive errors.
func TestHeartbeatLoop_FailureCounterResets(t *testing.T) {
	spy := newHeartbeatSpy()

	rt := NewRuntime(nil, spy, Config{
		Namespace:         "hb-reset",
		PollInterval:      50 * time.Millisecond,
		HeartbeatInterval: 50 * time.Millisecond,
	})

	jobID := uuid.New()
	token := leases.LeaseToken(uuid.NewString())
	rt.registerActiveJob(jobID, func() {}, token)
	defer rt.deregisterActiveJob(jobID)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	rt.Start(ctx)
	defer rt.Stop()

	// Wait for at least one successful heartbeat.
	if !waitFor(t, 1*time.Second, func() bool { return spy.callCount() >= 1 }) {
		t.Fatal("no heartbeat calls observed before injecting errors")
	}

	// Inject two failures (one less than maxHeartbeatFailures=3).
	spy.setErr(errors.New("transient error"))
	time.Sleep(120 * time.Millisecond) // allow up to 2 ticks

	// Clear the error — next heartbeat succeeds and counter should reset.
	spy.setErr(nil)
	time.Sleep(60 * time.Millisecond)

	// Runtime should NOT have self-terminated.
	if isStopped(rt) {
		t.Error("runtime self-terminated despite non-consecutive heartbeat failures")
	}
}

// TestRegisterDeregisterActiveJob verifies that the activeJobs map is correctly
// maintained by registerActiveJob and deregisterActiveJob.
func TestRegisterDeregisterActiveJob(t *testing.T) {
	spy := newHeartbeatSpy()
	rt := NewRuntime(nil, spy, Config{Namespace: "hb-register"})

	if got := activeJobCount(rt); got != 0 {
		t.Fatalf("expected 0 active jobs, got %d", got)
	}

	id1 := uuid.New()
	id2 := uuid.New()
	tok := leases.LeaseToken(uuid.NewString())

	rt.registerActiveJob(id1, func() {}, tok)
	rt.registerActiveJob(id2, func() {}, tok)

	if got := activeJobCount(rt); got != 2 {
		t.Fatalf("expected 2 active jobs after registration, got %d", got)
	}

	rt.deregisterActiveJob(id1)

	if got := activeJobCount(rt); got != 1 {
		t.Fatalf("expected 1 active job after deregistration, got %d", got)
	}

	rt.deregisterActiveJob(id2)

	if got := activeJobCount(rt); got != 0 {
		t.Fatalf("expected 0 active jobs after full deregistration, got %d", got)
	}
}
