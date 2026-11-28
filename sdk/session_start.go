package sdk

import (
	"context"
	"errors"
	"fmt"
	"time"
	"unicode/utf8"

	"github.com/shubolab/superagent/internal/contract"
	"github.com/shubolab/superagent/internal/durable"
	"github.com/shubolab/superagent/mesh"
	"github.com/shubolab/superagent/schedule"
	api "github.com/shubolab/superagent/session"
)

// Start reserves this session's next Run and uses its committed history.
// The caller supplies a NewRunID and the current input. The session owns History and Recoverable.
func (s *Session) Start(ctx context.Context, req contract.StartRequest) (*Run, error) {
	return s.startSession(ctx, req, "", nil)
}

func (s *Session) startSession(ctx context.Context, req contract.StartRequest, digest string, report func(schedule.AdmissionPhase)) (*Run, error) {
	runner, err := s.runner(ctx)
	if err != nil {
		return nil, err
	}
	if !validSessionStart(req) {
		return nil, contract.ErrInvalidRequest
	}
	req.Recoverable = true
	if runner.SessionScheduler() != nil {
		return s.startScheduled(ctx, runner, req, digest, report)
	}
	return s.start(ctx, runner, req)
}

func (s *Session) start(ctx context.Context, runner mesh.SessionRunner, req contract.StartRequest) (*Run, error) {
	var run *Run
	err := s.withAdmission(ctx, req.ID, func(ctx context.Context) error {
		created, err := s.reserve(ctx, runner, req)
		if err != nil {
			return err
		}
		run, err = s.resume(ctx, runner, req.ID)
		if created && !errors.Is(err, contract.ErrStartOutcomeUnknown) && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
			cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Second)
			defer cancel()
			if cleanupErr := s.clearUnattempted(cleanup, req.ID); cleanupErr != nil {
				err = errors.Join(contract.ErrStartOutcomeUnknown, err, cleanupErr)
			}
		}
		return err
	})
	return run, err
}

// Recover reconciles the original pending Run against its retained local authority.
func (s *Session) Recover(ctx context.Context, id contract.RunID) (*Run, error) {
	runner, err := s.runner(ctx)
	if err != nil {
		return nil, err
	}
	return s.recoverPending(ctx, runner, id)
}

func (s *Session) recoverPending(ctx context.Context, runner mesh.SessionRunner, id contract.RunID) (*Run, error) {
	if runner.SessionScheduler() != nil {
		return s.recoverScheduled(ctx, runner, id)
	}
	return s.recoverRun(ctx, runner, id)
}

func (s *Session) recoverRun(ctx context.Context, runner mesh.SessionRunner, id contract.RunID) (*Run, error) {
	var run *Run
	err := s.withAdmission(ctx, id, func(ctx context.Context) error {
		var err error
		run, err = s.resume(ctx, runner, id)
		return err
	})
	return run, err
}

func (s *Session) withAdmission(ctx context.Context, id contract.RunID, work func(context.Context) error) error {
	if !s.owns(id) {
		return ErrSessionOwnership
	}
	return s.store.WithAdmission(ctx, id, work)
}

func (s *Session) reserve(ctx context.Context, runner mesh.SessionRunner, req contract.StartRequest) (bool, error) {
	idx, err := s.store.LoadState(ctx)
	if err != nil {
		return false, err
	}
	record, err := s.store.LookupCommit(ctx, req.ID)
	if err != nil {
		return false, err
	}
	if record.Receipt != nil {
		return false, contract.ErrStartConflict
	}
	if idx.Pending != nil {
		if idx.Pending.Request.ID != req.ID {
			return false, ErrSessionBusy
		}
		pending, err := awaiting(idx, req.ID)
		if err != nil {
			return false, err
		}
		_, err = s.frozenRequest(ctx, runner, pending, req)
		return false, err
	}
	first, history, err := s.window(ctx, idx.Version, 1, s.limits.MaxHistoryBytes, false)
	if err != nil {
		return false, err
	}
	binding, err := runner.SessionBinding(ctx)
	if err != nil {
		return false, err
	}
	pending := api.Reservation{Version: idx.Version, FirstVersion: first, Binding: binding, Request: req}
	req.History = history
	pending.RequestDigest, err = runner.SessionRequestDigest(req)
	if err != nil {
		return false, err
	}
	if err := s.store.Reserve(ctx, pending); err != nil {
		return false, err
	}
	return true, nil
}

func (s *Session) clearUnattempted(ctx context.Context, id contract.RunID) error {
	idx, err := s.store.LoadState(ctx)
	if err != nil || idx.Pending == nil || idx.Pending.Request.ID != id || idx.Pending.Attempted {
		return err
	}
	return s.store.ClearReservation(ctx, idx.Pending.Key())
}

func (s *Session) resume(ctx context.Context, runner mesh.SessionRunner, id contract.RunID) (*Run, error) {
	idx, err := s.store.LoadState(ctx)
	if err != nil {
		return nil, err
	}
	pending, err := awaiting(idx, id)
	if err != nil {
		return nil, err
	}
	req, err := s.checkedPendingRequest(ctx, runner, pending)
	if err != nil {
		return nil, err
	}
	status, err := runner.SessionCheckpoint(s.scope.Subject, id, pending.RequestDigest)
	if err != nil {
		return nil, sessionResultError(err)
	}
	if status.Exists {
		if _, err := runner.Recover(ctx, s.scope.Subject, id); err != nil {
			return nil, sessionResultError(err)
		}
		return s.client.Run(id), nil
	}
	if pending.Attempted {
		return nil, sessionResultError(contract.ErrRunNotFound)
	}
	if err := s.store.MarkAttempted(ctx, pending.Key()); err != nil {
		return nil, err
	}
	_, err = runner.Start(ctx, s.scope.Subject, req)
	if err == nil {
		return s.client.Run(id), nil
	}
	if errors.Is(err, contract.ErrStartOutcomeUnknown) || errors.Is(err, contract.ErrStartConflict) || errors.Is(err, contract.ErrRecoveryBusy) {
		return nil, err
	}
	// The local runner returned a definite rejection. Complete the reservation cleanup even if the
	// caller cancelled, while keeping a bounded time to acquire and write the index.
	cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Second)
	defer cancel()
	status, probeErr := runner.SessionCheckpoint(s.scope.Subject, id, pending.RequestDigest)
	if probeErr != nil || status.Exists {
		return nil, errors.Join(err, contract.ErrStartOutcomeUnknown, probeErr)
	}
	pending.Attempted = true
	if cleanupErr := s.store.ClearReservation(cleanup, pending.Key()); cleanupErr != nil {
		return nil, errors.Join(err, contract.ErrStartOutcomeUnknown, cleanupErr)
	}
	return nil, err
}

func (s *Session) checkedPendingRequest(ctx context.Context, runner mesh.SessionRunner, pending api.Reservation) (contract.StartRequest, error) {
	binding, err := runner.SessionBinding(ctx)
	if err != nil {
		return contract.StartRequest{}, sessionResultError(err)
	}
	if binding != pending.Binding {
		return contract.StartRequest{}, sessionResultError(contract.ErrRecoveryBinding)
	}
	req, err := s.frozenRequest(ctx, runner, pending, pending.Request)
	if err != nil {
		return contract.StartRequest{}, sessionResultError(err)
	}
	return req, nil
}

func (s *Session) frozenRequest(ctx context.Context, runner mesh.SessionRunner, pending api.Reservation, req contract.StartRequest) (contract.StartRequest, error) {
	_, history, err := s.window(ctx, pending.Version, pending.FirstVersion, durable.MaxRecordBytes, true)
	if err != nil {
		return contract.StartRequest{}, err
	}
	req.History = history
	digest, err := runner.SessionRequestDigest(req)
	if err != nil || digest != pending.RequestDigest {
		return contract.StartRequest{}, contract.ErrStartConflict
	}
	return req, nil
}

func validSessionStart(req contract.StartRequest) bool {
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
