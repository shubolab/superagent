package sdk

import (
	"context"
	"errors"

	"github.com/shubolab/superagent/internal/contract"
	"github.com/shubolab/superagent/internal/session"
	"github.com/shubolab/superagent/mesh"
	"github.com/shubolab/superagent/schedule"
	api "github.com/shubolab/superagent/session"
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
	RunID contract.RunID
	State PendingRecoveryState
	Cause error
	Draft string // Populated only after successfully discarding an unadmitted reservation.
}

type pendingEvidence struct {
	PendingRecovery
	result *contract.Result
}

// InspectPending classifies retained evidence without writing records or dispatching work.
func (s *Session) InspectPending(ctx context.Context) (PendingRecovery, error) {
	runner, err := s.runner(ctx)
	if err != nil {
		return PendingRecovery{}, err
	}
	idx, err := s.store.LoadState(ctx)
	if err != nil {
		return PendingRecovery{}, err
	}
	if idx.Pending == nil {
		return PendingRecovery{State: RecoveryIdle}, nil
	}
	return s.inspectPending(ctx, runner, idx, idx.Pending.Request.ID).PendingRecovery, nil
}

func (s *Session) inspectPending(ctx context.Context, runner mesh.SessionRunner, idx api.State, id contract.RunID) pendingEvidence {
	p := pendingEvidence{PendingRecovery: PendingRecovery{RunID: id, State: RecoveryUnknown}}
	record, err := s.store.LookupCommit(ctx, id)
	if err != nil {
		p.Cause = err
		return p
	}
	if record.Receipt != nil {
		p.State = RecoverySave
		return p
	}
	pending, err := awaiting(idx, id)
	if err != nil {
		p.Cause = err
		return p
	}
	if record.Sealed != nil {
		if err := verifySealed(pending, *record.Sealed); err != nil {
			p.Cause = err
		} else {
			p.State = RecoverySave
		}
		return p
	}
	binding, err := runner.InspectSessionBinding(ctx)
	if err != nil {
		p.Cause = err
		return p
	}
	if binding != pending.Binding {
		p.Cause = contract.ErrRecoveryBinding
		return p
	}
	if _, err := s.frozenRequest(ctx, runner, pending, pending.Request); err != nil {
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
		p.Cause = contract.ErrRunNotFound
	}
	return p
}

// CommitPendingResult only publishes a retained terminal result. It never resumes a Run.
func (s *Session) CommitPendingResult(ctx context.Context, id contract.RunID) (SessionCommit, error) {
	runner, err := s.runner(ctx)
	if err != nil {
		return SessionCommit{}, err
	}
	if !s.owns(id) {
		return SessionCommit{}, ErrSessionOwnership
	}
	if receipt, ready, err := s.publish(ctx, id, nil); ready || err != nil {
		return receipt, err
	}
	idx, err := s.store.LoadState(ctx)
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
func (s *Session) DiscardUnadmitted(ctx context.Context, id contract.RunID) (PendingRecovery, error) {
	runner, err := s.runner(ctx)
	if err != nil {
		return PendingRecovery{}, err
	}
	var p PendingRecovery
	err = s.withAdmission(ctx, id, func(ctx context.Context) error {
		idx, err := s.store.LoadState(ctx)
		if err != nil {
			return err
		}
		if idx.Pending == nil {
			p.State = RecoveryIdle
			return nil
		}
		p = s.inspectPending(ctx, runner, idx, idx.Pending.Request.ID).PendingRecovery
		if p.RunID != id || p.State != RecoveryUnadmitted {
			return nil
		}
		if err := s.store.ClearReservation(ctx, idx.Pending.Key()); err != nil {
			return err
		}
		p.Draft = idx.Pending.Request.Input.Text
		return nil
	})
	return p, err
}

// ResumeAdmittedWithProgress continues an already admitted Run using its original checkpoint.
func (s *Session) ResumeAdmittedWithProgress(ctx context.Context, id contract.RunID) <-chan StartUpdate {
	return ResumeAdmittedWithProgress(s, ctx, id, startUpdate)
}

// ResumeAdmittedWithProgress adapts recovery updates to the caller's handle type.
func ResumeAdmittedWithProgress[T any](s *Session, ctx context.Context, id contract.RunID, update func(contract.RunID, StartPhase, *Run, error) T) <-chan T {
	return startUpdates(id, update, func(report func(schedule.AdmissionPhase)) (*Run, error) {
		return s.resumeAdmitted(ctx, id, report)
	})
}

func (s *Session) resumeAdmitted(ctx context.Context, id contract.RunID, report func(schedule.AdmissionPhase)) (*Run, error) {
	runner, err := s.runner(ctx)
	if err != nil {
		return nil, err
	}
	if !s.owns(id) {
		return nil, ErrSessionOwnership
	}
	idx, err := s.store.LoadState(ctx)
	if err != nil {
		return nil, err
	}
	if err := continuationError(s.inspectPending(ctx, runner, idx, id)); err != nil {
		return nil, err
	}
	resume := func(ctx context.Context) (*Run, error) {
		err := s.withAdmission(ctx, id, func(ctx context.Context) error {
			idx, err := s.store.LoadState(ctx)
			if err != nil {
				return err
			}
			if err := continuationError(s.inspectPending(ctx, runner, idx, id)); err != nil {
				return err
			}
			_, err = runner.RecoverSession(ctx, s.scope.Subject, id, idx.Pending.RequestDigest)
			if err != nil {
				return sessionResultError(err)
			}
			return nil
		})
		if err != nil {
			return nil, err
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
