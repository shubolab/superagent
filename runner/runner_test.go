package runner

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/shubolab/superagent/internal/contract"
)

func ptr[T any](v T) *T { return &v }

func TestRunnerProtectsAdmissionSnapshot(t *testing.T) {
	budget := contract.Budget{
		Timeout: ptr(5 * time.Second), MaxModelRequests: ptr(uint64(1)),
		MaxModelInputBytes: ptr(uint64(1024)), MaxOutputBytes: ptr(uint64(100)),
	}
	type opened struct {
		ctx context.Context
		req contract.ModelRequest
	}
	call := make(chan opened, 1)
	runner, err := New(contract.RunnerConfig{
		AllowedModels: map[contract.SubjectID][]contract.ModelRef{"alice": {"m"}},
		DefaultBudget: budget, BudgetCaps: budget, ObserveBufferBytes: 1024, ResultRetention: time.Minute,
	}, map[contract.ModelRef]struct{}{"m": {}}, func(ctx context.Context, _ contract.SubjectID, req contract.ModelRequest) (contract.ModelStream, error) {
		call <- opened{ctx, req}
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	initial := &contract.UserMessage{Text: "initial"}
	req := contract.StartRequest{ID: "id", Model: "m", History: []contract.Message{{User: initial}}, Input: contract.UserMessage{Text: "new"}}
	startCtx, cancel := context.WithCancel(t.Context())
	defer cancel()
	if _, err := runner.Start(startCtx, "alice", req); err != nil {
		t.Fatal(err)
	}
	cancel()
	initial.Text = "changed"
	testCtx, stop := context.WithTimeout(t.Context(), time.Second)
	defer stop()
	select {
	case generated := <-call:
		if generated.ctx == startCtx || generated.req.Messages[0].User.Text != "initial" ||
			generated.req.Messages[1].User.Text != "new" {
			t.Fatalf("model got the wrong context or snapshot: %+v", generated.req)
		}
		generated.req.Messages[0].User.Text = "provider edit"
	case <-testCtx.Done():
		t.Fatal("model was not called")
	}
	initial.Text = "initial"
	if _, err := runner.Start(t.Context(), "alice", req); err != nil {
		t.Fatalf("provider mutation changed retry identity: %v", err)
	}
}

func TestInputMeasurementMatchesCompactJSON(t *testing.T) {
	req := contract.ModelRequest{
		Instruction: "plain\n<",
		Messages: []contract.Message{
			{User: &contract.UserMessage{Text: "中文\""}},
			{Assistant: &contract.AssistantMessage{Text: "yes"}},
		},
	}
	data, err := json.Marshal(struct {
		Instruction string
		Messages    []contract.Message
	}{req.Instruction, req.Messages})
	if err != nil {
		t.Fatal(err)
	}
	n := uint64(len(data))
	if !inputFits(req, n) || inputFits(req, n-1) || inputFits(req, 1) {
		t.Fatalf("limits should use the exact encoded input size, %d bytes", n)
	}
}

func TestFinishedRunExpires(t *testing.T) {
	key := runKey{subject: "owner", id: "id"}
	run := newState(contract.StartRequest{ID: key.id}, limits{}, time.Now().Add(time.Minute), 1024)
	run.finishLocked(contract.Succeeded, &contract.AssistantMessage{Text: "done"}, nil, nil)
	run.finishedAt = time.Now().Add(-2 * time.Minute)
	runner := &Runner{retention: time.Minute, runs: map[runKey]*state{key: run}}
	if _, err := runner.Wait(t.Context(), key.subject, key.id); !errors.Is(err, contract.ErrRunNotFound) {
		t.Fatalf("expired result lookup = %v", err)
	}
	if len(runner.runs) != 0 {
		t.Fatal("expired result was retained in the registry")
	}
}
