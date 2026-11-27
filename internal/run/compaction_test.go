package run

import (
	"errors"
	"testing"
	"time"

	"github.com/shubolab/superagent/internal/contextview"
	"github.com/shubolab/superagent/internal/contract"
)

func TestStaleCompactionViewCannotPublish(t *testing.T) {
	s := newState(contract.StartRequest{ID: "run"}, limits{}, time.Now().Add(time.Minute), 1024)
	defer s.cancel()
	s.compaction = &compactionState{sources: contextview.NewStore(3)}
	source, _ := s.compaction.sources.Register(contextview.Block{Start: 0, Messages: s.messages})
	s.messages = append(s.messages, contract.Message{Assistant: &contract.AssistantMessage{Text: "new"}})
	old := checkedView{view: contextview.View{
		Summary: []contract.ContextSummaryEntry{{Section: "decisions", Text: "stale", Sources: []contract.ContextSourceRef{source}}},
	}}
	if rejected, ok := (&Runner{}).acceptView(s, 1, old); ok || !errors.Is(rejected.err, errMessageVersionChanged) {
		t.Fatalf("stale candidate accepted: %v, %v", ok, rejected.err)
	}
	if _, err := s.compaction.sources.ReadSource(t.Context(), source, 0, 3); !errors.Is(err, contract.ErrContextSourceUnavailable) {
		t.Fatalf("stale source published: %v", err)
	}
}
