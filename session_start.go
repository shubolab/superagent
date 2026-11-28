package superagent

import (
	"context"
	"errors"
	"fmt"
	"time"
	"unicode/utf8"

	"github.com/shubolab/superagent/internal/mesh"
	"github.com/shubolab/superagent/internal/session"
)

// Start reserves this session's next Run and uses its committed history.
// The caller supplies a NewRunID and the current input. The session owns History and Recoverable.
func (s *Session) Start(ctx context.Context, req StartRequest) (*Run, error) {
	runner, err := s.runner(ctx)
	if err != nil {
		return nil, err
	}
	if !validSessionStart(req) {
		return nil, ErrInvalidRequest
	}
	req.Recoverable = true
	lock, err := s.store.AdmissionLock(ctx, req.ID)
	if err != nil {
		return nil, err
	}
	defer lock.Close()
	if err := s.reserve(ctx, runner, req); err != nil {
		return nil, err
	}
	return s.resume(ctx, runner, req.ID)
}

// Recover reconciles the original pending Run against its retained local authority.
func (s *Session) Recover(ctx context.Context, id RunID) (*Run, error) {
	runner, err := s.runner(ctx)
	if err != nil {
		return nil, err
	}
	return s.recoverPending(ctx, runner, id)
}

func (s *Session) recoverPending(ctx context.Context, runner mesh.SessionRunner, id RunID) (*Run, error) {
	lock, err := s.store.AdmissionLock(ctx, id)
	if err != nil {
		return nil, err
	}
	defer lock.Close()
	return s.resume(ctx, runner, id)
}

func (s *Session) reserve(ctx context.Context, runner mesh.SessionRunner, req StartRequest) error {
	lock, err := s.store.Lock(ctx)
	if err != nil {
		return err
	}
	defer lock.Close()
	idx, err := s.store.Load()
	if err != nil {
		return err
	}
	if _, committed := idx.Receipt(req.ID); committed {
		return ErrStartConflict
	}
	if idx.Pending != nil {
		if idx.Pending.Request.ID != req.ID {
			return ErrSessionBusy
		}
		pending, err := idx.Awaiting(req.ID)
		if err != nil {
			return err
		}
		_, err = s.frozenRequest(runner, idx, pending, req)
		return err
	}
	first, history, err := s.store.Window(idx)
	if err != nil {
		return err
	}
	binding, err := runner.SessionBinding(ctx)
	if err != nil {
		return err
	}
	pending := session.Pending{Version: idx.Version, FirstVersion: first, Binding: binding, Request: req}
	req.History = history
	pending.RequestDigest, err = runner.SessionRequestDigest(req)
	if err != nil {
		return err
	}
	if err := s.store.CheckCapacity(idx, pending); err != nil {
		return err
	}
	idx.Pending = &pending
	return s.store.Save(idx)
}

func (s *Session) resume(ctx context.Context, runner mesh.SessionRunner, id RunID) (*Run, error) {
	idx, err := s.store.Load()
	if err != nil {
		return nil, err
	}
	pending, err := idx.Awaiting(id)
	if err != nil {
		return nil, err
	}
	binding, err := runner.SessionBinding(ctx)
	if err != nil {
		return nil, sessionResultError(err)
	}
	if binding != pending.Binding {
		return nil, sessionResultError(ErrRecoveryBinding)
	}
	req, err := s.frozenRequest(runner, idx, pending, pending.Request)
	if err != nil {
		return nil, sessionResultError(err)
	}
	admitted, err := runner.SessionCheckpoint(s.scope.Subject, id, pending.RequestDigest)
	if err != nil {
		return nil, sessionResultError(err)
	}
	if admitted {
		if _, err := runner.Recover(ctx, s.scope.Subject, id); err != nil {
			return nil, sessionResultError(err)
		}
		return s.client.Run(id), nil
	}
	if pending.Attempted {
		return nil, sessionResultError(ErrRunNotFound)
	}
	if err := s.setAttempted(ctx, id, true); err != nil {
		return nil, err
	}
	_, err = runner.Start(ctx, s.scope.Subject, req)
	if err == nil {
		return s.client.Run(id), nil
	}
	if errors.Is(err, ErrStartOutcomeUnknown) || errors.Is(err, ErrStartConflict) || errors.Is(err, ErrRecoveryBusy) {
		return nil, err
	}
	// The local runner returned a definite rejection. Complete the reservation cleanup even if the
	// caller cancelled, while keeping a bounded time to acquire and write the index.
	cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Second)
	defer cancel()
	admitted, probeErr := runner.SessionCheckpoint(s.scope.Subject, id, pending.RequestDigest)
	if probeErr != nil || admitted {
		return nil, errors.Join(err, ErrStartOutcomeUnknown, probeErr)
	}
	return nil, errors.Join(err, s.setAttempted(cleanup, id, false))
}

func (s *Session) setAttempted(ctx context.Context, id RunID, attempted bool) error {
	lock, err := s.store.Lock(ctx)
	if err != nil {
		return err
	}
	defer lock.Close()
	idx, err := s.store.Load()
	if err != nil {
		return err
	}
	if _, err := idx.Awaiting(id); err != nil {
		return err
	}
	if attempted {
		idx.Pending.Attempted = true
	} else {
		idx.Pending = nil
	}
	return s.store.Save(idx)
}

func (s *Session) frozenRequest(runner mesh.SessionRunner, idx session.Index, pending session.Pending, req StartRequest) (StartRequest, error) {
	history, err := s.store.FrozenWindow(idx, pending.FirstVersion)
	if err != nil {
		return StartRequest{}, err
	}
	req.History = history
	digest, err := runner.SessionRequestDigest(req)
	if err != nil || digest != pending.RequestDigest {
		return StartRequest{}, ErrStartConflict
	}
	return req, nil
}

func validSessionStart(req StartRequest) bool {
	if len(req.History) > 0 || len(req.PinnedHistory) > 0 || !utf8.ValidString(req.Input.Text) ||
		!utf8.ValidString(req.Instruction) || !utf8.ValidString(string(req.Model)) {
		return false
	}
	for _, tool := range req.Tools {
		if !utf8.ValidString(string(tool)) {
			return false
		}
	}
	return req.Environment == nil || (utf8.ValidString(string(req.Environment.Ref)) && utf8.ValidString(string(req.Environment.Workspace)))
}

func sessionResultError(err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return fmt.Errorf("%w: %w", ErrSessionResultUnavailable, err)
}
