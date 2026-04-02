// export_test.go exposes internal Runtime helpers for use by external test packages.
package jobs

import (
	"context"

	"github.com/carloruiz/leases"
	"github.com/google/uuid"
)

// ExportRegisterActiveJob adds a job to the runtime's activeJobs map.
// Used by tests to simulate an active job without going through Dispatch.
func ExportRegisterActiveJob(r *Runtime, id uuid.UUID, cancel context.CancelFunc, token leases.LeaseToken) {
	r.registerActiveJob(id, cancel, token)
}

// ExportDeregisterActiveJob removes a job from the runtime's activeJobs map.
func ExportDeregisterActiveJob(r *Runtime, id uuid.UUID) {
	r.deregisterActiveJob(id)
}

// ExportIsStopped reports whether the runtime's stop channel has been closed.
func ExportIsStopped(r *Runtime) bool {
	select {
	case <-r.stopCh:
		return true
	default:
		return false
	}
}

// ExportActiveJobCount returns the number of entries in the activeJobs map.
func ExportActiveJobCount(r *Runtime) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.activeJobs)
}
