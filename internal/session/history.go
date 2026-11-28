package session

import (
	"encoding/json"
	"slices"

	"github.com/shubolab/superagent/internal/contract"
)

// Window selects the longest complete suffix within the configured input limit.
func (s *Store) Window(idx Index) (uint64, []contract.Message, error) {
	return ReadWindow(idx.Version, 1, s.cfg.MaxHistoryBytes, false, func(version uint64) ([]contract.Message, error) {
		turn, err := s.ReadTurn(idx, version)
		return turn.Messages, err
	})
}

func ReadWindow(version, first, limit uint64, exact bool, read func(uint64) ([]contract.Message, error)) (uint64, []contract.Message, error) {
	if first == 0 || first > version+1 {
		return 0, nil, ErrVersion
	}
	remaining := limit - 2 // JSON array brackets.
	var turns [][]contract.Message
	for current := version; current >= first; current-- {
		messages, err := read(current)
		if err != nil {
			return 0, nil, err
		}
		raw, err := json.Marshal(messages)
		if err != nil || len(messages) == 0 {
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
		turns = append(turns, messages)
	}
	slices.Reverse(turns)
	return version + 1 - uint64(len(turns)), slices.Concat(turns...), nil
}
