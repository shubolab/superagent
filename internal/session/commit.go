package session

import (
	"errors"
	"os"

	"github.com/shubolab/superagent/internal/contract"
	"github.com/shubolab/superagent/internal/run"
)

// PrepareCommit checks the published receipt or seals the original result. Callers hold the index lock.
func (s *Store) PrepareCommit(idx Index, id contract.RunID, result *contract.Result) (Turn, bool, error) {
	if receipt, ok := idx.Receipt(id); ok {
		if err := matchResult(receipt, result); err != nil {
			return Turn{}, false, err
		}
		return Turn{Receipt: receipt}, true, s.Sync()
	}
	pending, err := idx.Awaiting(id)
	if err != nil {
		return Turn{}, false, err
	}
	turn, err := s.readTurn(id)
	if err == nil {
		if turn.Version != idx.Version+1 || turn.RequestDigest != pending.RequestDigest || !validTurn(pending, turn.Outcome, turn.Messages) {
			return Turn{}, false, ErrStorage
		}
		return turn, false, matchResult(turn.Receipt, result)
	}
	if !errors.Is(err, os.ErrNotExist) || result == nil {
		return Turn{}, false, err
	}
	if result.RunID != id || !validTurn(pending, result.Outcome, result.Messages) {
		return Turn{}, false, ErrContent
	}
	digest, err := resultDigest(result.Outcome, result.Messages)
	if err != nil {
		return Turn{}, false, ErrContent
	}
	turn = Turn{
		Receipt: Receipt{RunID: id, Version: idx.Version + 1, Digest: digest},
		Scope:   s.scope, RequestDigest: pending.RequestDigest, Outcome: result.Outcome, Messages: result.Messages,
	}
	return turn, false, s.write(s.turnPath(id), turn)
}

// Publish advances the visible index to include an already sealed turn.
func (s *Store) Publish(idx Index, turn Turn) (Receipt, error) {
	pending, err := idx.Awaiting(turn.RunID)
	if err != nil {
		return Receipt{}, err
	}
	if turn.Version != idx.Version+1 {
		return Receipt{}, ErrVersion
	}
	if turn.Scope != s.scope || turn.RequestDigest != pending.RequestDigest {
		return Receipt{}, ErrContent
	}
	// A restart may have followed an uncertain seal. Confirm the existing directory entry first.
	if err := s.Sync(); err != nil {
		return Receipt{}, err
	}
	idx.Version = turn.Version
	idx.Receipts = append(idx.Receipts, turn.Receipt)
	idx.Pending = nil
	if err := s.Save(idx); err != nil {
		return Receipt{}, err
	}
	return turn.Receipt, nil
}

func matchResult(receipt Receipt, result *contract.Result) error {
	if result == nil {
		return nil
	}
	digest, err := resultDigest(result.Outcome, result.Messages)
	if err != nil || result.RunID != receipt.RunID || digest != receipt.Digest {
		return ErrContent
	}
	return nil
}

func validTurn(pending Pending, outcome contract.RunOutcome, messages []contract.Message) bool {
	switch outcome {
	case contract.Succeeded, contract.Cancelled, contract.TimedOut, contract.BudgetExceeded, contract.Failed, contract.ToolOutcomeUnknown:
	default:
		return false
	}
	if len(messages) == 0 || messages[0].User == nil || *messages[0].User != pending.Request.Input || !run.ValidHistory(messages) {
		return false
	}
	for i, message := range messages[1:] {
		if message.User != nil || (message.Tool != nil && message.Tool.Result.RunID != pending.Request.ID) {
			return false
		}
		if message.Assistant != nil && len(message.Assistant.ToolCalls) == 0 {
			return outcome == contract.Succeeded && i == len(messages)-2
		}
	}
	return outcome != contract.Succeeded
}
