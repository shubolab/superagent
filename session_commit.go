package superagent

import (
	"context"
	"errors"
	"os"
)

// CommitRun publishes the original terminal Wait delta once and returns its durable receipt.
// A completed receipt or sealed delta remains usable after the runner's result retention expires.
func (s *Session) CommitRun(ctx context.Context, id RunID) (SessionCommit, error) {
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
	_, err = s.recoverPending(ctx, runner, id)
	if err == nil {
		var result Result
		result, err = runner.Wait(ctx, s.scope.Subject, id)
		if err == nil {
			receipt, _, err := s.publish(ctx, id, &result)
			return receipt, err
		}
		err = sessionResultError(err)
	}
	if ctx.Err() == nil {
		// Another committer may have already sealed or published while this caller checked the Run.
		if receipt, ready, recheckErr := s.publish(ctx, id, nil); ready || recheckErr != nil {
			return receipt, recheckErr
		}
	}
	return SessionCommit{}, err
}

func (s *Session) publish(ctx context.Context, id RunID, result *Result) (SessionCommit, bool, error) {
	lock, err := s.store.Lock(ctx)
	if err != nil {
		return SessionCommit{}, false, err
	}
	defer lock.Close()
	idx, err := s.store.Load()
	if err != nil {
		return SessionCommit{}, false, err
	}
	turn, committed, err := s.store.PrepareCommit(idx, id, result)
	if result == nil && errors.Is(err, os.ErrNotExist) {
		return SessionCommit{}, false, nil
	}
	if err != nil {
		return SessionCommit{}, false, err
	}
	if committed {
		return turn.Receipt, true, nil
	}
	receipt, err := s.store.Publish(idx, turn)
	return receipt, err == nil, err
}
