package jobs

import (
	"context"
	"time"

	"github.com/carloruiz/leases"
)

// Config holds startup configuration for a Runtime.
type Config struct {
	Namespace    string
	PollInterval time.Duration
}

// Runtime is the top-level struct that handles both dispatching and executing
// jobs. Namespace is a system-wide configuration set at startup.
type Runtime struct {
	namespace string
	db        DB
	leases    leases.Store
	registry  map[string]Handler
}

// NewRuntime constructs a Runtime with the given database, lease store, and config.
func NewRuntime(db DB, ls leases.Store, cfg Config) *Runtime {
	return &Runtime{
		namespace: cfg.Namespace,
		db:        db,
		leases:    ls,
		registry:  make(map[string]Handler),
	}
}

// Lookup returns the Handler registered for the given job name.
func (r *Runtime) Lookup(name string) (Handler, bool) {
	h, ok := r.registry[name]
	return h, ok
}

// Register registers a typed handler for the given job name. Go infers the
// type parameters from fn, so callers never write explicit type annotations:
//
//	jobs.Register(rt, "send_email", sendEmailHandler)
func Register[Req, Resp any](r *Runtime, name string, fn func(ctx context.Context, req Req) (Resp, error)) {
	r.registry[name] = JobFn[Req, Resp](fn)
}
