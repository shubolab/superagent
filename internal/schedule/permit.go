package schedule

import (
	"context"

	"github.com/shubolab/superagent/internal/contract"
)

type permitKey struct{}
type phase uint8

const (
	reserved phase = iota
	uncertain
	running
	released
)

// Permit belongs to exactly one session and Run inside its originating scheduler.
type Permit struct {
	scheduler *Scheduler
	session   Session
	id        contract.RunID
	digest    string
	phase     phase
	resolved  chan struct{}
}

func (s *Scheduler) Permit(ctx context.Context, subject contract.SubjectID, id contract.RunID) *Permit {
	p, _ := ctx.Value(permitKey{}).(*Permit)
	if p == nil || p.scheduler != s || p.session.Subject != subject || p.id != id {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.active[p.session] != p {
		return nil
	}
	return p
}

// Attach transfers release responsibility to the runner's terminal transition.
func (p *Permit) Attach() {
	s := p.scheduler
	s.mu.Lock()
	defer s.mu.Unlock()
	p.resolveLocked()
	p.phase = running
}

// MarkUncertain keeps the slot until the runner can verify the original admission.
func (p *Permit) MarkUncertain() <-chan struct{} {
	s := p.scheduler
	s.mu.Lock()
	defer s.mu.Unlock()
	p.phase = uncertain
	return p.resolved
}

// ConfirmStopped settles uncertain admission when the runner can prove it is idle.
func (p *Permit) ConfirmStopped() bool {
	s := p.scheduler
	s.mu.Lock()
	defer s.mu.Unlock()
	if p.phase != uncertain {
		return true
	}
	if a := s.attempts[p.session]; a != nil && a.permit == p {
		return false
	}
	s.releaseLocked(p)
	return true
}

func (p *Permit) Release() {
	s := p.scheduler
	s.mu.Lock()
	defer s.mu.Unlock()
	s.releaseLocked(p)
}

func (p *Permit) resolveLocked() {
	if p.phase == reserved || p.phase == uncertain {
		close(p.resolved)
	}
}
