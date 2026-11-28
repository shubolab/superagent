// Package schedule owns admission capacity for a local runner.
package schedule

import (
	"container/list"
	"context"
	"errors"
	"sync"
	"time"

	"github.com/shubolab/superagent/internal/contract"
)

type Session struct {
	Subject contract.SubjectID
	ID      string
}

type Scheduler struct {
	cfg      contract.SessionSchedulingConfig
	mu       sync.Mutex
	queue    list.List
	attempts map[Session]*attempt
	active   map[Session]*Permit

	now   func() time.Time
	timer func(time.Duration) (<-chan time.Time, func())
}

type attempt struct {
	session Session
	id      contract.RunID
	digest  string
	ctx     context.Context
	expires time.Time
	element *list.Element

	ready  chan struct{}
	done   chan struct{}
	permit *Permit
	err    error
}

func New(cfg contract.SessionSchedulingConfig) (*Scheduler, error) {
	if cfg.MaxActiveRuns <= 0 || cfg.MaxQueuedRuns < 0 || cfg.MaxQueueWait < 0 ||
		(cfg.MaxQueuedRuns > 0 && cfg.MaxQueueWait == 0) {
		return nil, contract.ErrInvalidConfiguration
	}
	return &Scheduler{
		cfg: cfg, attempts: make(map[Session]*attempt), active: make(map[Session]*Permit),
		now: time.Now, timer: func(d time.Duration) (<-chan time.Time, func()) {
			timer := time.NewTimer(d)
			return timer.C, func() { timer.Stop() }
		},
	}, nil
}

// Admit runs one start or recovery for a logical request. Concurrent retries share its reply.
func (s *Scheduler) Admit(ctx context.Context, session Session, id contract.RunID, digest string, work func(context.Context) error) error {
	a, owner, err := s.submit(ctx, session, id, digest)
	if err != nil {
		return err
	}
	if !owner {
		select {
		case <-a.done:
			return a.err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if !a.expires.IsZero() {
		s.wait(a)
	}
	if a.permit == nil {
		return a.err
	}
	err = work(context.WithValue(ctx, permitKey{}, a.permit))
	s.mu.Lock()
	a.err = err
	delete(s.attempts, session)
	close(a.done)
	if a.permit.phase == reserved {
		s.releaseLocked(a.permit)
	}
	s.mu.Unlock()
	return err
}

func (s *Scheduler) submit(ctx context.Context, session Session, id contract.RunID, digest string) (*attempt, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	s.expireLocked(now)
	if a := s.attempts[session]; a != nil {
		return a, false, match(a.id, a.digest, id, digest)
	}
	p := s.active[session]
	if p != nil {
		if err := match(p.id, p.digest, id, digest); err != nil {
			return nil, false, err
		}
	}
	a := &attempt{session: session, id: id, digest: digest, ctx: ctx, ready: make(chan struct{}), done: make(chan struct{})}
	if p != nil || len(s.active) < s.cfg.MaxActiveRuns {
		if p == nil {
			p = s.acquireLocked(a)
		}
		a.permit = p
	} else {
		if s.queue.Len() >= s.cfg.MaxQueuedRuns {
			return nil, false, contract.ErrScheduleQueueFull
		}
		a.expires = now.Add(s.cfg.MaxQueueWait)
		a.element = s.queue.PushBack(a)
	}
	s.attempts[session] = a
	return a, true, nil
}

func match(existingID contract.RunID, existingDigest string, id contract.RunID, digest string) error {
	if existingID != id {
		return contract.ErrSessionBusy
	}
	if existingDigest != digest {
		return contract.ErrStartConflict
	}
	return nil
}

func (s *Scheduler) wait(a *attempt) {
	select {
	case <-a.ready:
		return
	default:
	}
	timer, stop := s.timer(max(0, a.expires.Sub(s.now())))
	defer stop()
	select {
	case <-a.ready:
		return
	case <-a.ctx.Done():
	case <-timer:
	}
	s.mu.Lock()
	if a.element != nil {
		err := waitError(a, s.now())
		if err == nil {
			err = contract.ErrScheduleQueueTimeout
		}
		s.removeLocked(a, err)
		s.mu.Unlock()
		return
	}
	s.mu.Unlock()
	<-a.ready
}

func waitError(a *attempt, now time.Time) error {
	deadline, hasDeadline := a.ctx.Deadline()
	err := a.ctx.Err()
	if hasDeadline && !deadline.After(a.expires) && !now.Before(deadline) && !errors.Is(err, context.Canceled) {
		return context.DeadlineExceeded
	}
	if !now.Before(a.expires) {
		return contract.ErrScheduleQueueTimeout
	}
	return err
}

func (s *Scheduler) expireLocked(now time.Time) {
	for e := s.queue.Front(); e != nil; {
		a := e.Value.(*attempt)
		e = e.Next()
		if err := waitError(a, now); err != nil {
			s.removeLocked(a, err)
		}
	}
}

func (s *Scheduler) removeLocked(a *attempt, err error) {
	s.queue.Remove(a.element)
	a.element = nil
	a.err = err
	delete(s.attempts, a.session)
	close(a.ready)
	close(a.done)
}

func (s *Scheduler) acquireLocked(a *attempt) *Permit {
	p := &Permit{scheduler: s, session: a.session, id: a.id, digest: a.digest, resolved: make(chan struct{})}
	s.active[a.session] = p
	return p
}

func (s *Scheduler) releaseLocked(p *Permit) {
	if s.active[p.session] != p {
		return
	}
	delete(s.active, p.session)
	p.resolveLocked()
	p.phase = released
	now := s.now()
	for e := s.queue.Front(); e != nil; e = s.queue.Front() {
		a := e.Value.(*attempt)
		if err := waitError(a, now); err != nil {
			s.removeLocked(a, err)
			continue
		}
		s.queue.Remove(e)
		a.element = nil
		a.permit = s.acquireLocked(a)
		close(a.ready)
		break
	}
}

// Counts reports the local runner's current occupancy.
func (s *Scheduler) Counts() (active, queued int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.active), s.queue.Len()
}
