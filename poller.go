package jobs

import (
	"context"
	"sync"
	"time"

	"github.com/google/uuid"
)

// pollerResult is delivered to a subscriber when its job reaches a terminal
// state. err is non-nil only when the poller encounters a DB error that
// prevents it from fanning out a status result.
type pollerResult struct {
	status string // StatusCompleted or StatusFailed
	err    error
}

// pollerSub is a single subscriber waiting on a specific job ID.
type pollerSub struct {
	ch chan pollerResult
}

// statusPoller runs a single background goroutine that, on each tick, issues
// one WHERE job_id = ANY($1) query covering all active subscriptions. This
// replaces per-caller ticker loops and caps DB query count at 1 per poll
// interval regardless of how many concurrent Run() callers are waiting.
//
// The goroutine starts lazily on the first Subscribe() call and stops when
// Stop() is called.
type statusPoller struct {
	db        DB
	namespace string
	interval  time.Duration

	mu   sync.Mutex
	subs map[uuid.UUID][]*pollerSub

	startOnce sync.Once
	stopCh    chan struct{}
}

func newStatusPoller(db DB, namespace string, interval time.Duration) *statusPoller {
	return &statusPoller{
		db:        db,
		namespace: namespace,
		interval:  interval,
		subs:      make(map[uuid.UUID][]*pollerSub),
		stopCh:    make(chan struct{}),
	}
}

// Subscribe registers jobID for polling and returns:
//   - ch: a buffered channel that receives exactly one pollerResult when the
//     job reaches a terminal state (completed or failed).
//   - cancel: a function to deregister the subscription; always defer it.
//
// Subscribe starts the background goroutine on the first call.
func (p *statusPoller) Subscribe(jobID uuid.UUID) (<-chan pollerResult, func()) {
	sub := &pollerSub{ch: make(chan pollerResult, 1)}

	p.mu.Lock()
	p.subs[jobID] = append(p.subs[jobID], sub)
	p.mu.Unlock()

	// Start the background goroutine exactly once.
	p.startOnce.Do(func() { go p.run() })

	cancel := func() {
		p.mu.Lock()
		defer p.mu.Unlock()
		subs := p.subs[jobID]
		for i, s := range subs {
			if s == sub {
				last := len(subs) - 1
				subs[i] = subs[last]
				p.subs[jobID] = subs[:last]
				if len(p.subs[jobID]) == 0 {
					delete(p.subs, jobID)
				}
				return
			}
		}
	}
	return sub.ch, cancel
}

// Stop signals the poller goroutine to exit. Safe to call multiple times and
// before the goroutine has started.
func (p *statusPoller) Stop() {
	select {
	case <-p.stopCh:
	default:
		close(p.stopCh)
	}
}

func (p *statusPoller) run() {
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()
	for {
		select {
		case <-p.stopCh:
			return
		case <-ticker.C:
			p.tick()
		}
	}
}

// tick performs a single batched poll: one DB query for all subscribed job
// IDs, then fans out terminal results to the waiting subscribers.
func (p *statusPoller) tick() {
	p.mu.Lock()
	if len(p.subs) == 0 {
		p.mu.Unlock()
		return
	}
	jobIDs := make([]uuid.UUID, 0, len(p.subs))
	for id := range p.subs {
		jobIDs = append(jobIDs, id)
	}
	p.mu.Unlock()

	rows, err := p.db.Query(context.Background(),
		`SELECT job_id, status FROM job_status
		 WHERE namespace = $1 AND job_id = ANY($2)
		   AND status IN ('completed', 'failed')`,
		p.namespace, jobIDs,
	)
	if err != nil {
		// Transient DB error — skip this tick; subscribers keep waiting.
		return
	}
	defer rows.Close()

	terminalByID := make(map[uuid.UUID]string)
	for rows.Next() {
		var id uuid.UUID
		var status string
		if scanErr := rows.Scan(&id, &status); scanErr != nil {
			continue
		}
		terminalByID[id] = status
	}
	if rows.Err() != nil {
		return
	}

	if len(terminalByID) == 0 {
		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	for id, status := range terminalByID {
		result := pollerResult{status: status}
		for _, sub := range p.subs[id] {
			select {
			case sub.ch <- result:
			default:
			}
		}
		delete(p.subs, id)
	}
}
