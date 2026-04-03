package jobs

import (
	"context"
	"encoding/json"
	"errors"
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
func upsertJobStatus(ctx context.Context, q DB, jobID uuid.UUID, status string) error {
	_, err := q.Exec(ctx,
		`INSERT INTO job_status (job_id, status, updated_at)
		 VALUES ($1, $2, now())
		 ON CONFLICT (job_id) DO UPDATE SET status = $2, updated_at = now()`,
		jobID, status,
	)
	return err
}

// jobStatusResponse is the JSON body returned by GET /api/v1/jobs/:id/status.
type jobStatusResponse struct {
	JobID     uuid.UUID `json:"job_id"`
	Status    string    `json:"status"`
	UpdatedAt time.Time `json:"updated_at"`
}

// API exposes the HTTP management endpoints for the job system.
// Additional endpoints (cancel, retry, full detail) are added in PR 7.
//
// TODO(PR 7): add cancel, retry, and full job-detail endpoints.
type API struct {
	db DB
}

// NewAPI constructs an API backed by the given DB handle.
func NewAPI(db DB) *API {
	return &API{db: db}
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

	var resp jobStatusResponse
	resp.JobID = id
	err = api.db.QueryRow(r.Context(),
		`SELECT status, updated_at FROM job_status WHERE job_id = $1`, id,
	).Scan(&resp.Status, &resp.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}
