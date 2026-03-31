package jobs

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// DBTX is satisfied by *pgxpool.Pool, *pgx.Conn, and pgx.Tx, allowing job
// system operations to run against either a connection pool or an existing
// transaction.
type DBTX interface {
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

// BackoffPolicy defines the per-attempt delay schedule for a job type.
// DelaySeconds is a list of wait durations indexed by attempt number (0-based).
// If the attempt index exceeds the list length, the last value is used.
type BackoffPolicy struct {
	DelaySeconds []int `json:"delay_seconds"`
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
	BackoffPolicy  json.RawMessage
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
	LeaseToken   string
}
