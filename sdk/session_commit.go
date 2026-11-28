package sdk

import (
	"context"

	"github.com/shubolab/superagent/internal/contract"
	"github.com/shubolab/superagent/internal/session"
	"github.com/shubolab/superagent/mesh"
	api "github.com/shubolab/superagent/session"
)

// CommitRun publishes the original terminal Wait delta once and returns its durable receipt.
// A completed receipt or sealed delta remains usable after the runner's result retention expires.
func (s *Session) CommitRun(ctx context.Context, id contract.RunID) (SessionCommit, error) {
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
	result, err := s.resultForCommit(ctx, runner, id)
	if err == nil {
		receipt, _, err := s.publish(ctx, id, &result)
		return receipt, err
	}
	if ctx.Err() == nil {
		// Another committer may have already sealed or published while this caller checked the Run.
		if receipt, ready, recheckErr := s.publish(ctx, id, nil); ready || recheckErr != nil {
			return receipt, recheckErr
		}
	}
	return SessionCommit{}, err
}

func (s *Session) resultForCommit(ctx context.Context, runner mesh.SessionRunner, id contract.RunID) (contract.Result, error) {
	if runner.SessionScheduler() != nil {
		pending, checkpoint, err := s.scheduledCheckpoint(ctx, runner, id)
		if err != nil {
			return contract.Result{}, err
		}
		if checkpoint.Result != nil {
			if _, err := s.checkedPendingRequest(ctx, runner, pending); err != nil {
				return contract.Result{}, err
			}
			return *checkpoint.Result, nil
		}
		if _, err := s.admitRecovery(ctx, runner, pending); err != nil {
			return contract.Result{}, err
		}
	} else if _, err := s.recoverRun(ctx, runner, id); err != nil {
		return contract.Result{}, err
	}
	result, err := runner.Wait(ctx, s.scope.Subject, id)
	if err != nil {
		return contract.Result{}, sessionResultError(err)
	}
	return result, nil
}

func (s *Session) publish(ctx context.Context, id contract.RunID, result *contract.Result) (SessionCommit, bool, error) {
	var digest string
	if result != nil {
		var err error
		digest, err = api.ResultDigest(result.Outcome, result.Messages)
		if err != nil || result.RunID != id {
			return SessionCommit{}, false, ErrSessionContent
		}
	}
	state, err := s.store.LoadState(ctx)
	if err != nil {
		return SessionCommit{}, false, err
	}
	record, err := s.store.LookupCommit(ctx, id)
	if err != nil {
		return SessionCommit{}, false, err
	}
	var expected api.Receipt
	if record.Receipt != nil {
		expected = *record.Receipt
	} else {
		pending, err := awaiting(state, id)
		if err != nil {
			return SessionCommit{}, false, err
		}
		switch {
		case record.Sealed != nil:
			if err := verifySealed(pending, *record.Sealed); err != nil {
				return SessionCommit{}, false, err
			}
			expected = record.Sealed.Receipt
		case result == nil:
			return SessionCommit{}, false, nil
		default:
			if !session.ValidResult(pending, *result) {
				return SessionCommit{}, false, ErrSessionContent
			}
			expected = api.Receipt{RunID: id, Version: pending.Version + 1, Digest: digest}
			turn := api.Turn{Receipt: expected, RequestDigest: pending.RequestDigest, Outcome: result.Outcome, Messages: result.Messages}
			if err := s.store.Seal(ctx, turn); err != nil {
				return SessionCommit{}, false, err
			}
		}
	}
	if expected.RunID != id || expected.Version == 0 || expected.Digest == "" {
		return SessionCommit{}, false, ErrSessionStorage
	}
	if result != nil && digest != expected.Digest {
		return SessionCommit{}, false, ErrSessionContent
	}
	receipt, err := s.store.Publish(ctx, id, expected.Digest)
	if err != nil {
		return SessionCommit{}, false, err
	}
	if receipt != expected {
		return SessionCommit{}, false, ErrSessionStorage
	}
	return SessionCommit(receipt), true, nil
}

func verifySealed(pending api.Reservation, turn api.Turn) error {
	digest, err := api.ResultDigest(turn.Outcome, turn.Messages)
	if err != nil || turn.Digest != digest || turn.Version != pending.Version+1 || turn.RequestDigest != pending.RequestDigest ||
		!session.ValidResult(pending, contract.Result{RunID: turn.RunID, Outcome: turn.Outcome, Messages: turn.Messages}) {
		return ErrSessionStorage
	}
	return nil
}
