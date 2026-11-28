package sdk

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/shubolab/superagent/internal/contract"
	"github.com/shubolab/superagent/internal/durable"
	"github.com/shubolab/superagent/internal/session"
)

type sessionTestSubject struct{}

func sessionTestClient(t *testing.T, root, binding string, retention time.Duration, model contract.ModelProvider, tool contract.Tool) *Client {
	t.Helper()
	m, err := NewMesh(sessionTestMeshConfig(t, root, binding, retention, model, tool))
	if err != nil {
		t.Fatal(err)
	}
	client, err := NewClient(m, "main")
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func sessionTestMeshConfig(t *testing.T, root, binding string, retention time.Duration, model contract.ModelProvider, tool contract.Tool) MeshConfig {
	t.Helper()
	if !durable.Supported {
		t.Skip("local session storage requires process locks")
	}
	budget := contract.Budget{
		Timeout: address(12 * time.Second), MaxModelRequests: address(uint64(5)),
		MaxModelInputBytes: address(uint64(32 << 10)), MaxOutputBytes: address(uint64(24 << 10)),
		MaxToolRequests: address(uint64(4)),
	}
	cfg := contract.RunnerConfig{
		AllowedModels: map[contract.SubjectID][]contract.ModelRef{"owner": {"chat"}, "other": {"chat"}},
		DefaultBudget: budget, BudgetCaps: budget, ObserveBufferBytes: 16 << 10, ResultRetention: retention,
		Recovery: &contract.RecoveryConfig{Directory: filepath.Join(root, "runs"), BindingVersion: binding, ReconcileTimeout: 100 * time.Millisecond},
	}
	var tools map[contract.ToolRef]contract.LocalToolBinding
	if tool != nil {
		receiptTool, err := NewLocalReceiptTool(filepath.Join(root, "receipts"), tool)
		if err != nil {
			t.Fatal(err)
		}
		cfg.AllowedTools = map[contract.SubjectID]map[contract.ToolRef]contract.ToolPermission{"owner": {"tool": {}}}
		tools = map[contract.ToolRef]contract.LocalToolBinding{"tool": {
			Spec: contract.ToolSpec{InputSchema: json.RawMessage(`{}`), Limits: contract.ToolLimits{MaxResultBytes: 1024}},
			Tool: receiptTool,
		}}
	}
	return MeshConfig{
		LocalRunners: map[contract.RunnerRef]contract.RunnerConfig{"main": cfg},
		LocalModels:  map[contract.ModelRef]contract.ModelProvider{"chat": model},
		LocalTools:   tools,
		ResolveCaller: func(ctx context.Context) (contract.SubjectID, error) {
			if subject, ok := ctx.Value(sessionTestSubject{}).(contract.SubjectID); ok {
				return subject, nil
			}
			return "owner", nil
		},
	}
}

func sessionTestContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func openTestSession(t *testing.T, ctx context.Context, client *Client, root string, id SessionID) *Session {
	t.Helper()
	s, err := client.OpenSession(ctx, id, SessionConfig{Directory: filepath.Join(root, "sessions"), MaxTurns: 12, MaxHistoryBytes: 8192})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func sessionRequest(s *Session, input string) contract.StartRequest {
	return contract.StartRequest{ID: s.NewRunID(), Model: "chat", Input: contract.UserMessage{Text: input}}
}

func startSessionRun(t *testing.T, ctx context.Context, s *Session, req contract.StartRequest) *Run {
	t.Helper()
	run, err := s.Start(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if run.ID() != req.ID {
		t.Fatalf("Run ID %q, want %q", run.ID(), req.ID)
	}
	return run
}

func waitSessionRun(t *testing.T, ctx context.Context, run *Run, outcome contract.RunOutcome) contract.Result {
	t.Helper()
	result, err := run.Wait(ctx)
	if err != nil || result.Outcome != outcome {
		t.Fatalf("Wait outcome = %s, stop %+v, want %s, error = %v", result.Outcome, result.Stop, outcome, err)
	}
	return result
}

func commitSessionRun(t *testing.T, ctx context.Context, s *Session, id contract.RunID, version uint64) SessionCommit {
	t.Helper()
	receipt, err := s.CommitRun(ctx, id)
	if err != nil || receipt.RunID != id || receipt.Version != version || receipt.Digest == "" {
		t.Fatalf("commit = %+v, want version %d and Run %s (%v)", receipt, version, id, err)
	}
	return receipt
}

func assertSessionVersion(t *testing.T, ctx context.Context, s *Session, version uint64, messages int) SessionSnapshot {
	t.Helper()
	snapshot, err := s.Snapshot(ctx)
	if err != nil || snapshot.Version != version || len(snapshot.Messages) != messages {
		t.Fatalf("snapshot version %d with %d messages, want %d with %d (%v)",
			snapshot.Version, len(snapshot.Messages), version, messages, err)
	}
	return snapshot
}

func TestSessionAdmission(t *testing.T) {
	root, ctx := t.TempDir(), sessionTestContext(t)
	requests := make(chan contract.ModelRequest, 4)
	model := deliveryModel(func(_ context.Context, req contract.ModelRequest) (contract.ModelStream, error) {
		requests <- req
		return &completionStream{&contract.ModelReply{Finish: contract.ModelFinishStop, Text: "reply"}}, nil
	})
	client := sessionTestClient(t, root, "v1", time.Minute, model, nil)
	s := openTestSession(t, ctx, client, root, "admission")
	invalid := sessionRequest(s, "first")
	invalid.Model = "missing"
	if _, err := s.Start(ctx, invalid); !errors.Is(err, contract.ErrBindingNotFound) {
		t.Fatalf("definite rejection: %v", err)
	}
	if id, err := s.PendingRun(ctx); err != nil || id != "" {
		t.Fatalf("reservation after definite rejection: %q, %v", id, err)
	}

	req := sessionRequest(s, "same input")
	original := startSessionRun(t, ctx, s, req)
	waitSessionRun(t, ctx, original, contract.Succeeded)
	assertSessionVersion(t, ctx, s, 0, 0)
	if _, err := s.Start(ctx, sessionRequest(s, "new")); !errors.Is(err, ErrSessionBusy) {
		t.Fatalf("expected busy after Wait: %v", err)
	}
	startSessionRun(t, ctx, s, req)
	changed := req
	changed.Input.Text = "changed"
	if _, err := s.Start(ctx, changed); !errors.Is(err, contract.ErrStartConflict) {
		t.Fatalf("mismatched retry: %v", err)
	}
	commitSessionRun(t, ctx, s, req.ID, 1)
	followup := sessionRequest(s, "same input")
	waitSessionRun(t, ctx, startSessionRun(t, ctx, s, followup), contract.Succeeded)
	prior, current := <-requests, <-requests
	if prior.RunID != req.ID || len(prior.Messages) != 1 || prior.Messages[0].User == nil || prior.Messages[0].User.Text != "same input" {
		t.Fatalf("first input: %+v", prior)
	}
	if current.RunID != followup.ID || len(current.Messages) != 3 || current.Messages[0].User == nil ||
		current.Messages[0].User.Text != "same input" || current.Messages[1].Assistant == nil ||
		current.Messages[1].Assistant.Text != "reply" || current.Messages[2].User == nil || current.Messages[2].User.Text != "same input" {
		t.Fatalf("history or repeated text was altered: %+v", current)
	}
	select {
	case request := <-requests:
		t.Fatalf("unexpected model dispatch: %+v", request)
	default:
	}
}

func fileSessionStore(s *Session) *session.Store { return s.store.(session.Backend).Store }
