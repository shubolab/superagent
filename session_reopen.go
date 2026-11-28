package superagent

import (
	"context"
	"errors"
	"os"

	"github.com/shubolab/superagent/internal/mesh"
	"github.com/shubolab/superagent/internal/schedule"
	"github.com/shubolab/superagent/internal/session"
)

type PendingRecoveryState string

const (
	RecoveryIdle       PendingRecoveryState = "idle"
	RecoverySave       PendingRecoveryState = "save"
	RecoveryContinue   PendingRecoveryState = "continue"
	RecoveryUnadmitted PendingRecoveryState = "unadmitted"
	RecoveryUnknown    PendingRecoveryState = "unknown"
)

// ErrPendingResultReady means the original Run can be saved without resuming it.
var ErrPendingResultReady = errors.New("pending Run result is ready to save")

type PendingRecovery struct {
	RunID RunID
	State PendingRecoveryState
	Cause error
	Draft string // Populated only after successfully discarding an unadmitted reservation.
}

type pendingEvidence struct {
	PendingRecovery
	result *Result
}

// InspectPending classifies retained evidence without writing records or dispatching work.
func (s *Session) InspectPending(ctx context.Context) (PendingRecovery, error) {
	runner, err := s.runner(ctx)
	if err != nil {
		return PendingRecovery{}, err
	}
	idx, err := s.store.Load()
	if err != nil {
		return PendingRecovery{}, err
	}
	if idx.Pending == nil {
		return PendingRecovery{State: RecoveryIdle}, nil
	}
	return s.inspectPending(ctx, runner, idx, idx.Pending.Request.ID).PendingRecovery, nil
}

func (s *Session) inspectPending(ctx context.Context, runner mesh.SessionRunner, idx session.Index, id RunID) pendingEvidence {
	p := pendingEvidence{PendingRecovery: PendingRecovery{RunID: id, State: RecoveryUnknown}}
	if _, ok := idx.Receipt(id); ok {
		p.State = RecoverySave
		return p
	}
	pending, err := idx.Awaiting(id)
	if err != nil {
		p.Cause = err
		return p
	}
	if _, err := s.store.ReadSealed(idx, id); err == nil {
		p.State = RecoverySave
		return p
	} else if !errors.Is(err, os.ErrNotExist) {
		p.Cause = err
		return p
	}
	binding, err := runner.InspectSessionBinding(ctx)
	if err != nil {
		p.Cause = err
		return p
	}
	if binding != pending.Binding {
		p.Cause = ErrRecoveryBinding
		return p
	}
	if _, err := s.frozenRequest(runner, idx, pending, pending.Request); err != nil {
		p.Cause = err
		return p
	}
	checkpoint, err := runner.SessionCheckpoint(s.scope.Subject, id, pending.RequestDigest)
	switch {
	case err != nil:
		p.Cause = err
	case checkpoint.Result != nil && !session.ValidResult(pending, *checkpoint.Result):
		p.Cause = ErrSessionContent
	case checkpoint.Result != nil:
		p.State, p.result = RecoverySave, checkpoint.Result
	case checkpoint.Exists:
		p.State = RecoveryContinue
	case !pending.Attempted:
		p.State = RecoveryUnadmitted
	default:
		p.Cause = ErrRunNotFound
	}
	return p
}

// CommitPendingResult only publishes a retained terminal result. It never resumes a Run.
func (s *Session) CommitPendingResult(ctx context.Context, id RunID) (SessionCommit, error) {
	runner, err := s.runner(ctx)
	if err != nil {
		return SessionCommit{}, err
	}
	if !s.store.Owns(id) {
		return SessionCommit{}, ErrSessionOwnership
	}
	if receipt, ready, err := s.publish(ctx, id, nil); ready || err != nil {
		return receipt, err
	}
	idx, err := s.store.Load()
	if err != nil {
		return SessionCommit{}, err
	}
	p := s.inspectPending(ctx, runner, idx, id)
	if p.State != RecoverySave {
		return SessionCommit{}, errors.Join(ErrSessionResultUnavailable, p.Cause)
	}
	receipt, ready, err := s.publish(ctx, id, p.result)
	if !ready && err == nil {
		err = ErrSessionResultUnavailable
	}
	return receipt, err
}

// DiscardUnadmitted clears only the exact reservation proven never to have attempted admission.
func (s *Session) DiscardUnadmitted(ctx context.Context, id RunID) (PendingRecovery, error) {
	runner, err := s.runner(ctx)
	if err != nil {
		return PendingRecovery{}, err
	}
	admission, err := s.store.AdmissionLock(ctx, id)
	if err != nil {
		return PendingRecovery{}, err
	}
	defer admission.Close()
	lock, err := s.store.Lock(ctx)
	if err != nil {
		return PendingRecovery{}, err
	}
	defer lock.Close()
	idx, err := s.store.Load()
	if err != nil {
		return PendingRecovery{}, err
	}
	if idx.Pending == nil {
		return PendingRecovery{State: RecoveryIdle}, nil
	}
	p := s.inspectPending(ctx, runner, idx, idx.Pending.Request.ID).PendingRecovery
	if p.RunID != id || p.State != RecoveryUnadmitted {
		return p, nil
	}
	if err := ctx.Err(); err != nil {
		return p, err
	}
	input := idx.Pending.Request.Input.Text
	idx.Pending = nil
	if err := s.store.Save(idx); err != nil {
		return p, err
	}
	p.Draft = input
	return p, nil
}

// ResumeAdmittedWithProgress continues an already admitted Run using its original checkpoint.
// An expired or missing checkpoint cannot turn this operation into a fresh Start.
func (s *Session) ResumeAdmittedWithProgress(ctx context.Context, id RunID) <-chan StartUpdate {
	return startUpdates(id, func(report func(schedule.AdmissionPhase)) (*Run, error) {
		return s.resumeAdmitted(ctx, id, report)
	})
}

func (s *Session) resumeAdmitted(ctx context.Context, id RunID, report func(schedule.AdmissionPhase)) (*Run, error) {
	runner, err := s.runner(ctx)
	if err != nil {
		return nil, err
	}
	if !s.store.Owns(id) {
		return nil, ErrSessionOwnership
	}
	idx, err := s.store.Load()
	if err != nil {
		return nil, err
	}
	if err := continuationError(s.inspectPending(ctx, runner, idx, id)); err != nil {
		return nil, err
	}
	resume := func(ctx context.Context) (*Run, error) {
		lock, err := s.store.AdmissionLock(ctx, id)
		if err != nil {
			return nil, err
		}
		defer lock.Close()
		idx, err := s.store.Load()
		if err != nil {
			return nil, err
		}
		if err := continuationError(s.inspectPending(ctx, runner, idx, id)); err != nil {
			return nil, err
		}
		if _, err := runner.RecoverSession(ctx, s.scope.Subject, id, idx.Pending.RequestDigest); err != nil {
			return nil, sessionResultError(err)
		}
		return s.client.Run(id), nil
	}
	if runner.SessionScheduler() == nil {
		return resume(ctx)
	}
	_, digest, err := freezeScheduledRequest(idx.Pending.Request)
	if err != nil {
		return nil, err
	}
	return s.admitScheduled(ctx, runner, id, digest, report, resume)
}

func continuationError(p pendingEvidence) error {
	switch p.State {
	case RecoveryContinue:
		return nil
	case RecoverySave:
		return ErrPendingResultReady
	default:
		return errors.Join(ErrSessionResultUnavailable, p.Cause)
	}
}
