package contextview

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"sync"

	"github.com/shubolab/superagent/internal/contract"
)

// Store holds immutable source data for one Run. Pending candidates are private.
type Store struct {
	mu        sync.RWMutex
	refs      map[int]contract.ContextSourceRef
	entries   map[contract.ContextSourceRef]source
	pageBytes uint64
}

type source struct {
	raw       string
	published bool
}

func NewStore(pageBytes uint64) *Store {
	return &Store{refs: make(map[int]contract.ContextSourceRef), entries: make(map[contract.ContextSourceRef]source), pageBytes: pageBytes}
}

func (s *Store) Register(b Block) (contract.ContextSourceRef, string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if ref, ok := s.refs[b.Start]; ok {
		return ref, s.entries[ref].raw
	}
	ref := contract.ContextSourceRef(rand.Text())
	for {
		if _, exists := s.entries[ref]; !exists {
			break
		}
		ref = contract.ContextSourceRef(rand.Text())
	}
	raw := encodeSource(b.Messages)
	s.refs[b.Start] = ref
	s.entries[ref] = source{raw: raw}
	return ref, raw
}

func encodeSource(messages []contract.Message) string {
	if len(messages) == 1 {
		if a := messages[0].Assistant; a != nil {
			return a.Text
		}
		if u := messages[0].User; u != nil {
			return u.Text
		}
	}
	raw, _ := json.Marshal(messages)
	return string(raw)
}

func (s *Store) Publish(refs []contract.ContextSourceRef) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, ref := range refs {
		if entry, ok := s.entries[ref]; ok {
			entry.published = true
			s.entries[ref] = entry
		}
	}
}

func (s *Store) ReadSource(ctx context.Context, ref contract.ContextSourceRef, offset, maxBytes uint64) (contract.ContextSourcePage, error) {
	if err := ctx.Err(); err != nil {
		return contract.ContextSourcePage{}, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	entry, ok := s.entries[ref]
	if !ok || !entry.published || maxBytes == 0 || offset > uint64(len(entry.raw)) {
		return contract.ContextSourcePage{}, contract.ErrContextSourceUnavailable
	}
	end := offset + min(uint64(len(entry.raw))-offset, maxBytes, s.pageBytes)
	return contract.ContextSourcePage{Content: []byte(entry.raw[offset:end]), NextOffset: end, EOF: end == uint64(len(entry.raw))}, nil
}
