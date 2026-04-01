package jobs

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"time"

	"github.com/carloruiz/leases"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// DB is satisfied by *pgxpool.Pool, *pgx.Conn, and pgx.Tx, allowing job
// system operations to run against either a connection pool or an existing
// transaction.
type DB interface {
	Begin(ctx context.Context) (pgx.Tx, error)
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// Handler is the internal interface for job execution. All job types must
// implement this interface. Use JobFn to adapt a typed function.
type Handler interface {
	Handle(ctx context.Context, req json.RawMessage) (json.RawMessage, error)
}

// JobFn adapts a typed function to the internal json.RawMessage boundary.
// Req and Resp must be JSON-serializable; the invariant is enforced at
// unmarshal (request) and marshal (response) time.
type JobFn[Req, Resp any] func(ctx context.Context, req Req) (Resp, error)

// Handle implements Handler. It unmarshals the raw request into Req, calls the
// underlying function, and marshals the result back to JSON.
func (f JobFn[Req, Resp]) Handle(ctx context.Context, raw json.RawMessage) (json.RawMessage, error) {
	var req Req
	if err := json.Unmarshal(raw, &req); err != nil {
		return nil, fmt.Errorf("unmarshal request: %w", err)
	}
	resp, err := f(ctx, req)
	if err != nil {
		return nil, err
	}
	return json.Marshal(resp)
}

// BackoffPolicy defines the exponential back-off schedule for a job type.
// Duration computes the delay before attempt n (0-based) using:
//
//	min(InitialInterval * Multiplier^n, MaxInterval)
type BackoffPolicy struct {
	InitialInterval time.Duration `json:"initial_interval"`
	Multiplier      float64       `json:"multiplier"`
	MaxInterval     time.Duration `json:"max_interval"`
}

// Duration returns the delay that should elapse before attempt n (0-based).
// The result is capped to MaxInterval when MaxInterval is non-zero.
func (b BackoffPolicy) Duration(attempt int) time.Duration {
	d := float64(b.InitialInterval) * math.Pow(b.Multiplier, float64(attempt))
	if b.MaxInterval > 0 && time.Duration(d) >= b.MaxInterval {
		return b.MaxInterval
	}
	return time.Duration(d)
}

// Job represents a single unit of work. Rows are immutable after insertion.
type Job struct {
	ID             uuid.UUID
	IdempotencyKey string
	Name           string
	Namespace      string
	Metadata       json.RawMessage
	Request        json.RawMessage
	MaxAttempts    int
	RetryUntil     *time.Time
	CreatedAt      time.Time
	CreatorSHA     string
	CreatorHost    string
	BackoffPolicy  BackoffPolicy
	Deadline       *time.Time
}

// Attempt represents a single execution of a job. Each row in job_attempts
// maps to one Attempt.
type Attempt struct {
	JobID        uuid.UUID
	AttemptNo    int
	Request      json.RawMessage
	Response     json.RawMessage
	Error        json.RawMessage
	CreatedAt    time.Time
	StartedAt    time.Time
	FinishedAt   *time.Time
	ExecutorHost string
	ExecutorSHA  string
	LeaseToken   leases.LeaseToken
}
