package session

import (
	"encoding/json"
	"errors"
	"io"
	"math"
	"os"

	"github.com/shubolab/superagent/internal/durable"
)

const maxHistoryOffset = int64(math.MaxInt64)

func (s *Store) historyPath() string { return s.indexPath + ".jsonl" }

func validHistoryOffsets(idx Index) bool {
	if idx.Format != indexFormat || uint64(len(idx.HistoryEnds)) != idx.Version {
		return false
	}
	var start int64
	for _, end := range idx.HistoryEnds {
		if end <= start || end-start > durable.MaxRecordBytes+1 {
			return false
		}
		start = end
	}
	return true
}

func historyEnd(idx Index) int64 {
	if len(idx.HistoryEnds) == 0 {
		return 0
	}
	return idx.HistoryEnds[len(idx.HistoryEnds)-1]
}

// The index publishes the synced prefix. A crash before index publication can
// leave extra bytes, which are invisible and removable under the index lock.
func (s *Store) checkHistory(idx Index) error {
	file, err := os.OpenFile(s.historyPath(), os.O_RDWR, 0600)
	if err != nil {
		return storageError(err)
	}
	defer file.Close()
	return storageError(trimHistory(file, historyEnd(idx)))
}

func trimHistory(file *os.File, end int64) error {
	info, err := file.Stat()
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Size() < end {
		return ErrStorage
	}
	if info.Size() == end {
		return nil
	}
	if err := file.Truncate(end); err != nil {
		return err
	}
	return file.Sync()
}

func (s *Store) initializeHistory() error {
	path := s.historyPath()
	info, err := os.Lstat(path)
	switch {
	case errors.Is(err, os.ErrNotExist):
		_, err = durable.WriteBytes(path, nil)
		return storageError(err)
	case err != nil:
		return storageError(err)
	case !info.Mode().IsRegular() || info.Size() != 0:
		return ErrStorage
	default:
		// An interrupted creation may already have written an empty file.
		return s.Sync()
	}
}

func historyLine(turn Turn) ([]byte, error) {
	data, err := json.Marshal(turn)
	if err != nil {
		return nil, err
	}
	if len(data) > durable.MaxRecordBytes {
		return nil, ErrCapacity
	}
	return append(data, '\n'), nil
}

func (s *Store) appendHistory(idx Index, turn Turn) (int64, error) {
	line, err := historyLine(turn)
	if err != nil {
		return 0, err
	}
	file, err := os.OpenFile(s.historyPath(), os.O_RDWR, 0600)
	if err != nil {
		return 0, storageError(err)
	}
	defer file.Close()
	start := historyEnd(idx)
	if err := trimHistory(file, start); err != nil {
		return 0, storageError(err)
	}
	if _, err := file.WriteAt(line, start); err != nil {
		return 0, storageError(err)
	}
	return start + int64(len(line)), storageError(file.Sync())
}

func (s *Store) readHistoryTurn(idx Index, version uint64) (Turn, error) {
	end := idx.HistoryEnds[version-1]
	var start int64
	if version > 1 {
		start = idx.HistoryEnds[version-2]
	}
	file, err := os.Open(s.historyPath())
	if err != nil {
		return Turn{}, storageError(err)
	}
	defer file.Close()
	line, err := io.ReadAll(io.NewSectionReader(file, start, end-start))
	if err != nil || len(line) == 0 || int64(len(line)) != end-start || line[len(line)-1] != '\n' {
		return Turn{}, ErrStorage
	}
	var turn Turn
	if err := json.Unmarshal(line, &turn); err != nil {
		return Turn{}, storageError(err)
	}
	return s.verifyTurn(idx.Receipts[version-1].RunID, turn)
}
