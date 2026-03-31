// Package leases defines the distributed lease interface used by the job system
// to prevent double-claiming of jobs. The concrete implementation is provided
// separately; this package contains only the interface and associated types.
package leases

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// DBTX is satisfied by *pgxpool.Pool, *pgx.Conn, and pgx.Tx.
type DBTX interface {
	Begin(ctx context.Context) (pgx.Tx, error)
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// LeaseToken is an opaque value that identifies a specific acquisition of a
// lease. It must be presented when releasing or heartbeating a lease to prevent
// stale owners from interfering with a new holder.
type LeaseToken string

// Lease represents an acquired lease on a resource.
type Lease struct {
	// Resource is the identifier of the leased resource (typically a job ID).
	Resource string
	// Token is the opaque acquisition token; required for Release and Heartbeat.
	Token LeaseToken
	// ExpiresAt is when the lease expires if not heartbeated.
	ExpiresAt time.Time
}

// HeartbeatRequest is passed to HeartbeatMany to identify a lease to renew.
type HeartbeatRequest struct {
	Resource string
	Token    LeaseToken
}

// Store is the interface for distributed lease operations. All methods that
// take a DBTX will participate in the caller's transaction when one is
// provided, enabling atomic lease + data operations.
//
// TODO: provide a concrete pgx-backed implementation.
type Store interface {
	// Create inserts an unlocked (unclaimed) lease row. Used when a job is
	// dispatched asynchronously and will be claimed by the poll loop.
	Create(ctx context.Context, db DBTX, group, resource string) (*Lease, error)

	// CreateAndAcquire atomically creates and acquires a lease. Used during
	// Dispatch/Run for local-first execution.
	CreateAndAcquire(ctx context.Context, db DBTX, group, resource, owner string, duration time.Duration) (*Lease, error)

	// Delete removes the lease row entirely. Called on job completion,
	// permanent failure, or cancellation.
	Delete(ctx context.Context, db DBTX, resource string) error

	// Acquire claims an existing unlocked lease. Returns an error if the lease
	// is already held.
	Acquire(ctx context.Context, db DBTX, resource string, owner string, duration time.Duration) (*Lease, error)

	// Release returns a held lease to the unclaimed pool so another worker can
	// acquire it. Only called during graceful termination.
	Release(ctx context.Context, db DBTX, resource string, token LeaseToken) error

	// AcquireMany claims up to limit available leases from the given group.
	// Used by the claim loop.
	AcquireMany(ctx context.Context, db DBTX, group string, limit int, owner string, duration time.Duration) ([]Lease, error)

	// Heartbeat renews a single lease, extending its expiry.
	Heartbeat(ctx context.Context, db DBTX, resource string, token LeaseToken, duration time.Duration) (*Lease, error)

	// HeartbeatMany renews multiple leases in a single round-trip.
	HeartbeatMany(ctx context.Context, db DBTX, items []HeartbeatRequest, duration time.Duration) ([]Lease, error)
}
