package jobs

import (
	"context"
	"sync"
	"time"

	"github.com/carloruiz/leases"
	"github.com/google/uuid"
)

// Config holds startup configuration for a System.
type Config struct {
	Namespace    string
	BatchSize    int
	PollInterval time.Duration
}

// statusResult is the outcome delivered to a statusPoller subscriber.
type statusResult struct {
	Response []byte
	Err      error
}

// statusPoller batches concurrent poll requests into a single DB round-trip per tick.
type statusPoller struct {
	mu       sync.Mutex
	subs     map[uuid.UUID][]chan statusResult
	db       DBTX
	interval time.Duration
}

// System is the top-level struct that handles both dispatching and executing
// jobs. Namespace is a system-wide configuration set at startup.
type System struct {
	namespace string
	db        DBTX
	leases    leases.Store
	registry  map[string]Handler
	poller    *statusPoller
}

// NewSystem constructs a System with the given database, lease store, and config.
func NewSystem(db DBTX, ls leases.Store, cfg Config) *System {
	return &System{
		namespace: cfg.Namespace,
		db:        db,
		leases:    ls,
		registry:  make(map[string]Handler),
		poller: &statusPoller{
			subs:     make(map[uuid.UUID][]chan statusResult),
			db:       db,
			interval: cfg.PollInterval,
		},
	}
}

// Register registers a typed handler for the given job name. Go infers the
// type parameters from fn, so callers never write explicit type annotations:
//
//	jobs.Register(sys, "send_email", sendEmailHandler)
func Register[Req, Resp any](s *System, name string, fn func(ctx context.Context, req Req) (Resp, error)) {
	s.registry[name] = JobFn[Req, Resp](fn)
}
