package contextview

import (
	"testing"

	"github.com/shubolab/superagent/internal/contract"
)

func TestThresholdsForDocumentedWindow(t *testing.T) {
	trigger, target := Thresholds(116000, contract.ContextCompactionPolicy{TriggerRatio: .9, TargetRatio: .7})
	if trigger != 104400 || target != 81200 {
		t.Fatalf("trigger %d, target %d", trigger, target)
	}
}

func TestProtectedToolBlockAndPendingSource(t *testing.T) {
	messages := []contract.Message{
		{User: &contract.UserMessage{Text: "original"}},
		{Assistant: &contract.AssistantMessage{ToolCalls: []contract.ModelToolCall{{CallID: "pair"}}}},
		{Tool: &contract.ToolMessage{Result: contract.ToolResult{CallID: "pair"}}},
		{User: &contract.UserMessage{Text: "latest"}},
	}
	blocks := Blocks(messages, []int{2})
	if len(blocks) != 3 || !blocks[0].Protected || !blocks[1].Protected || !blocks[2].Protected {
		t.Fatalf("failed to protect pinned tool block and users")
	}
	repo := NewStore(3)
	ref, raw := repo.Register(blocks[0])
	if raw != "original" {
		t.Fatal("source differs from original text")
	}
	if _, err := repo.ReadSource(t.Context(), ref, 0, 3); err != contract.ErrContextSourceUnavailable {
		t.Fatalf("pending source returned %v", err)
	}
	again, _ := repo.Register(blocks[0])
	if ref != again {
		t.Fatalf("unstable refs %q and %q", ref, again)
	}
}
