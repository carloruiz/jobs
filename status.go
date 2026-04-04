package jobs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// Job status constants written to the job_status table.
const (
	StatusRunning   = "running"
	StatusCompleted = "completed"
	StatusFailed    = "failed"
)

// upsertJobStatus writes or updates the job_status row within the given DB
// handle (pool, conn, or tx). Called at every lifecycle event. The call must
// occur inside the same transaction as the triggering event so that status and
// execution state remain consistent.
func upsertJobStatus(ctx context.Context, q DB, namespace string, jobID uuid.UUID, status string) error {
	_, err := q.Exec(ctx,
		`INSERT INTO job_status (namespace, job_id, status, updated_at)
		 VALUES ($1, $2, $3, now())
		 ON CONFLICT (namespace, job_id) DO UPDATE SET status = $3, updated_at = now()`,
		namespace, jobID, status,
	)
	return err
}

// JobStatusResult holds the current status of a job, returned by GetJobStatus
// and by the HTTP status endpoint.
type JobStatusResult struct {
	JobID     uuid.UUID `json:"job_id"`
	Status    string    `json:"status"`
	UpdatedAt time.Time `json:"updated_at"`
}

// GetJobStatus fetches the current status of a job from the job_status table.
// Returns nil, pgx.ErrNoRows if the job has no status row yet (not yet claimed).
func (r *Runtime) GetJobStatus(ctx context.Context, jobID uuid.UUID) (*JobStatusResult, error) {
	var result JobStatusResult
	result.JobID = jobID
	err := r.db.QueryRow(ctx,
		`SELECT status, updated_at FROM job_status WHERE namespace = $1 AND job_id = $2`,
		r.namespace, jobID,
	).Scan(&result.Status, &result.UpdatedAt)
	if err != nil {
		return nil, err
	}
	return &result, nil
}

// WaitForCompletion blocks until the job reaches a terminal state (completed
// or failed) or the context is cancelled. Uses the shared statusPoller so
// that N concurrent callers issue only 1 DB query per poll interval.
// Returns (true, nil) on success, (false, nil) on permanent failure, and
// (false, err) if the context expires or a DB error occurs.
func (r *Runtime) WaitForCompletion(ctx context.Context, jobID uuid.UUID) (bool, error) {
	ch, cancel := r.poller.Subscribe(jobID)
	defer cancel()
	select {
	case <-ctx.Done():
		return false, ctx.Err()
	case result := <-ch:
		if result.err != nil {
			return false, fmt.Errorf("poll status: %w", result.err)
		}
		return result.status == StatusCompleted, nil
	}
}

// API exposes the HTTP management endpoints for the job system.
// Additional endpoints (cancel, retry, full detail) are added in PR 7.
//
// TODO(PR 7): add cancel, retry, and full job-detail endpoints.
type API struct {
	rt *Runtime
}

// NewAPI constructs an API backed by the given Runtime.
func NewAPI(rt *Runtime) *API {
	return &API{rt: rt}
}

// RegisterRoutes registers all management API routes on mux.
func (api *API) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /api/v1/jobs/{id}/status", api.handleJobStatus)
}

// handleJobStatus serves GET /api/v1/jobs/{id}/status.
// Returns the current status via a primary-key scan on job_status (O(1)).
// Returns 404 when the job has not yet been claimed (no row in job_status).
func (api *API) handleJobStatus(w http.ResponseWriter, r *http.Request) {
	idStr := r.PathValue("id")
	id, err := uuid.Parse(idStr)
	if err != nil {
		http.Error(w, "invalid job id", http.StatusBadRequest)
		return
	}

	result, err := api.rt.GetJobStatus(r.Context(), id)
	if errors.Is(err, pgx.ErrNoRows) {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
}
