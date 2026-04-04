package jobs

import (
	"context"
	"sync"
	"time"

	"github.com/google/uuid"
)

// runWithLock acquires mu, runs fn, then releases mu via defer. The error
// returned by fn is passed through to the caller. Use this helper wherever a
// lock must be held for the duration of a critical section.
func runWithLock(mu sync.Locker, fn func() error) error {
	mu.Lock()
	defer mu.Unlock()
	return fn()
}

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
//   - ch: a receive-only buffered channel that delivers exactly one pollerResult
//     when the job reaches a terminal state (completed or failed). The channel
//     is owned by the poller — callers must never close it. Because it is typed
//     as <-chan pollerResult (receive-only), the compiler enforces this: only
//     the statusPoller, which holds the underlying bidirectional channel, is
//     able to close or send on it.
//   - cancel: a function to deregister the subscription; always defer it.
//
// Subscribe starts the background goroutine on the first call.
func (p *statusPoller) Subscribe(jobID uuid.UUID) (<-chan pollerResult, func()) {
	sub := &pollerSub{ch: make(chan pollerResult, 1)}

	_ = runWithLock(&p.mu, func() error {
		p.subs[jobID] = append(p.subs[jobID], sub)
		return nil
	})

	// Start the background goroutine exactly once.
	p.startOnce.Do(func() { go p.run() })

	cancel := func() {
		_ = runWithLock(&p.mu, func() error {
			subs := p.subs[jobID]
			for i, s := range subs {
				if s == sub {
					last := len(subs) - 1
					subs[i] = subs[last]
					p.subs[jobID] = subs[:last]
					if len(p.subs[jobID]) == 0 {
						delete(p.subs, jobID)
					}
					// The channel is not closed here: the subscriber selects
					// on ctx.Done() alongside ch, so it will never block
					// indefinitely. The channel is buffered (size 1) and, once
					// removed from the map, will be garbage-collected when the
					// subscriber's goroutine exits and drops its reference.
					return nil
				}
			}
			// Sub not found: tick() already fanned out and removed it.
			return nil
		})
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
	var jobIDs []uuid.UUID
	_ = runWithLock(&p.mu, func() error {
		if len(p.subs) == 0 {
			return nil
		}
		jobIDs = make([]uuid.UUID, 0, len(p.subs))
		for id := range p.subs {
			jobIDs = append(jobIDs, id)
		}
		return nil
	})
	if len(jobIDs) == 0 {
		return
	}

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

	_ = runWithLock(&p.mu, func() error {
		for id, status := range terminalByID {
			result := pollerResult{status: status}
			for _, sub := range p.subs[id] {
				// Send the result then close so the subscriber's channel
				// read always unblocks. The send is non-blocking only as a
				// safety net; in normal flow the buffered channel is empty.
				select {
				case sub.ch <- result:
					close(sub.ch)
				default:
				}
			}
			delete(p.subs, id)
		}
		return nil
	})
}
