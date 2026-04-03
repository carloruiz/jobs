package jobs

import (
	"context"
	"time"

	"github.com/carloruiz/leases"
	"github.com/google/uuid"
)

const maxHeartbeatFailures = 3

// heartbeatLoop periodically calls HeartbeatMany for all active leases.
// If maxHeartbeatFailures consecutive errors occur, the worker self-terminates
// by calling Stop() to signal the claim loop to exit.
//
// TODO(PR 8): replace Stop() with a graceful Shutdown() that drains in-flight jobs.
func (r *Runtime) heartbeatLoop(ctx context.Context) {
	ticker := time.NewTicker(r.heartbeatInterval)
	defer ticker.Stop()

	consecutiveFailures := 0

	for {
		select {
		case <-ctx.Done():
			return
		case <-r.stopCh:
			return
		case <-ticker.C:
			reqs := r.activeLeasesAsHeartbeatRequests()
			if len(reqs) == 0 {
				consecutiveFailures = 0
				continue
			}
			_, err := r.leases.HeartbeatMany(ctx, r.db, reqs, leaseDuration)
			if err != nil {
				consecutiveFailures++
				if consecutiveFailures >= maxHeartbeatFailures {
					// Too many consecutive DB errors: self-terminate so that lease
					// expiry reclaims all active jobs on other workers.
					r.Stop()
					return
				}
				continue
			}
			consecutiveFailures = 0
		}
	}
}

// activeLeasesAsHeartbeatRequests returns a HeartbeatRequest for every active job.
// Returns nil if no jobs are currently active.
func (r *Runtime) activeLeasesAsHeartbeatRequests() []leases.HeartbeatRequest {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.activeJobs) == 0 {
		return nil
	}
	reqs := make([]leases.HeartbeatRequest, 0, len(r.activeJobs))
	for resource, aj := range r.activeJobs {
		reqs = append(reqs, leases.HeartbeatRequest{Resource: resource, Token: aj.token})
	}
	return reqs
}

// registerActiveJob adds the job to the activeJobs map for heartbeating.
func (r *Runtime) registerActiveJob(id uuid.UUID, cancel context.CancelFunc, token leases.LeaseToken) {
	r.mu.Lock()
	r.activeJobs[id.String()] = &activeJob{cancel: cancel, token: token}
	r.mu.Unlock()
}

// deregisterActiveJob removes the job from the activeJobs map.
func (r *Runtime) deregisterActiveJob(id uuid.UUID) {
	r.mu.Lock()
	delete(r.activeJobs, id.String())
	r.mu.Unlock()
}
