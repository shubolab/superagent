package session

import (
	"encoding/json"
	"slices"

	"github.com/shubolab/superagent/internal/contract"
	"github.com/shubolab/superagent/internal/durable"
)

// Window selects the longest complete suffix within the configured input limit.
func (s *Store) Window(idx Index) (uint64, []contract.Message, error) {
	return s.window(idx, 1, s.cfg.MaxHistoryBytes, false)
}

// FrozenWindow reconstructs the range selected at reservation, even if the host changes its window limit.
func (s *Store) FrozenWindow(idx Index, first uint64) ([]contract.Message, error) {
	_, messages, err := s.window(idx, first, durable.MaxRecordBytes, true)
	return messages, err
}

func (s *Store) window(idx Index, first, limit uint64, exact bool) (uint64, []contract.Message, error) {
	if first == 0 || first > idx.Version+1 {
		return 0, nil, ErrVersion
	}
	remaining := limit - 2 // JSON array brackets.
	var turns [][]contract.Message
	for version := idx.Version; version >= first; version-- {
		turn, err := s.ReadTurn(idx, version)
		if err != nil {
			return 0, nil, err
		}
		raw, err := json.Marshal(turn.Messages)
		if err != nil || len(turn.Messages) == 0 {
			return 0, nil, ErrStorage
		}
		size := uint64(len(raw) - 2)
		if len(turns) > 0 {
			size++ // Separator between adjacent turns.
		}
		if size > remaining {
			if exact || len(turns) == 0 {
				return 0, nil, ErrHistoryLimit
			}
			break
		}
		remaining -= size
		turns = append(turns, turn.Messages)
	}
	slices.Reverse(turns)
	return idx.Version + 1 - uint64(len(turns)), slices.Concat(turns...), nil
}
