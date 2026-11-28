package sdk

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/shubolab/superagent/internal/contract"
)

type stagedSessionStream struct {
	ctx    context.Context
	events []contract.ModelEvent
	gate   <-chan struct{}
	offset int
	closed chan struct{}
	once   sync.Once
}

func (s *stagedSessionStream) Recv() (contract.ModelEvent, error) {
	if s.offset >= len(s.events) {
		return contract.ModelEvent{}, io.EOF
	}
	if s.offset == 1 && s.gate != nil {
		select {
		case <-s.ctx.Done():
			return contract.ModelEvent{}, s.ctx.Err()
		case <-s.closed:
			return contract.ModelEvent{}, contract.ErrStreamClosed
		case <-s.gate:
		}
	}
	event := s.events[s.offset]
	s.offset++
	return event, nil
}

func (s *stagedSessionStream) Close() error {
	s.once.Do(func() { close(s.closed) })
	return nil
}

func textSessionStream(ctx context.Context, first, rest string, gate <-chan struct{}) contract.ModelStream {
	return &stagedSessionStream{
		ctx: ctx, gate: gate, closed: make(chan struct{}),
		events: []contract.ModelEvent{
			{Kind: contract.ModelTextDelta, TextDelta: first},
			{Kind: contract.ModelTextDelta, TextDelta: rest},
			{Kind: contract.ModelCompleted, Reply: &contract.ModelReply{Finish: contract.ModelFinishStop, Text: first + rest}},
		},
	}
}

func sessionToolReply(ids ...contract.CallID) contract.ModelStream {
	calls := make([]contract.ModelToolCall, 0, len(ids))
	for _, id := range ids {
		calls = append(calls, contract.ModelToolCall{CallID: id, Tool: "tool", Arguments: json.RawMessage(`{}`)})
	}
	return &completionStream{&contract.ModelReply{Finish: contract.ModelFinishToolCalls, Text: "tool round", ToolCalls: calls}}
}

func knownSessionTool(req contract.ToolRequest) contract.ToolResult {
	return contract.ToolResult{RunID: req.RunID, CallID: req.CallID, State: contract.ToolExecutionKnown,
		Completion: &contract.ToolCompletion{Outcome: contract.ToolSucceeded, Content: string(req.CallID)}}
}

func recvSessionEvent(t *testing.T, events contract.EventStream, id contract.RunID, kind contract.EventKind) contract.Event {
	t.Helper()
	for {
		event, err := events.Recv()
		if err != nil {
			t.Fatalf("expected event %s for %s: %v", kind, id, err)
		}
		if event.RunID != id {
			t.Fatalf("foreign event: %+v", event)
		}
		if event.Kind == kind {
			return event
		}
	}
}

func observeSessionRun(t *testing.T, ctx context.Context, run *Run) contract.EventStream {
	t.Helper()
	stream, err := run.Observe(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = stream.Close() })
	return stream
}

func TestStageCSessionC4IndependentRuns(t *testing.T) {
	root, ctx := t.TempDir(), sessionTestContext(t)
	held, cancelled, secondDelta := make(chan struct{}), make(chan struct{}), make(chan struct{})
	allowReceipt := make(chan struct{})
	releaseTool := sync.OnceFunc(func() { close(allowReceipt) })
	defer releaseTool()
	requests := make(chan contract.ModelRequest, 12)
	var toolCalls atomic.Int32
	tool := deliveryTool(func(ctx context.Context, req contract.ToolRequest) (contract.ToolResult, error) {
		toolCalls.Add(1)
		if req.CallID == "ready" {
			return knownSessionTool(req), nil
		}
		close(held)
		<-ctx.Done()
		close(cancelled)
		<-allowReceipt
		return contract.ToolResult{RunID: req.RunID, CallID: req.CallID, State: contract.ToolExecutionNotExecuted,
			Reason: &contract.ToolReason{Code: "cancelled_during_wait"}}, nil
	})
	model := deliveryModel(func(ctx context.Context, req contract.ModelRequest) (contract.ModelStream, error) {
		requests <- req
		var input string
		for _, msg := range slices.Backward(req.Messages) {
			if msg.User != nil {
				input = msg.User.Text
				break
			}
		}
		switch input {
		case "A":
			if req.Step == 1 {
				return sessionToolReply("ready"), nil
			}
			return sessionToolReply("hold"), nil
		case "B1":
			return textSessionStream(ctx, "B1", " finished", nil), nil
		case "B2":
			return textSessionStream(ctx, "B2", " finished", secondDelta), nil
		default:
			return nil, errors.New("unexpected model invocation")
		}
	})
	client := sessionTestClient(t, root, "v1", time.Minute, model, tool)
	a, b := openTestSession(t, ctx, client, root, "A"), openTestSession(t, ctx, client, root, "B")
	aRequest := sessionRequest(a, "A")
	aRequest.Tools = []contract.ToolRef{"tool"}
	aRun := startSessionRun(t, ctx, a, aRequest)
	aEvents := observeSessionRun(t, ctx, aRun)
	select {
	case <-held:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	for {
		event := recvSessionEvent(t, aEvents, aRun.ID(), contract.EventToolDispatched)
		if event.Tool != nil && event.Tool.CallID == "hold" {
			break
		}
	}
	if err := aEvents.Close(); err != nil {
		t.Fatal(err)
	}
	waitCtx, cancel := context.WithTimeout(ctx, 15*time.Millisecond)
	_, waitErr := a.CommitRun(waitCtx, aRun.ID())
	cancel()
	if !errors.Is(waitErr, context.DeadlineExceeded) {
		t.Fatalf("commit wait while tool holds: %v", waitErr)
	}
	assertSessionVersion(t, ctx, a, 0, 0)
	if _, err := a.Start(ctx, sessionRequest(a, "busy")); !errors.Is(err, ErrSessionBusy) {
		t.Fatalf("second A start while tool holds: %v", err)
	}
	type commitResult struct {
		receipt SessionCommit
		err     error
	}
	aCommit := make(chan commitResult, 1)
	go func() {
		receipt, err := a.CommitRun(ctx, aRun.ID())
		aCommit <- commitResult{receipt, err}
	}()

	b1 := startSessionRun(t, ctx, b, sessionRequest(b, "B1"))
	b1Events := observeSessionRun(t, ctx, b1)
	if first := recvSessionEvent(t, b1Events, b1.ID(), contract.EventTextDelta); first.TextDelta != "B1" {
		t.Fatalf("first B stream: %+v", first)
	}
	if end := recvSessionEvent(t, b1Events, b1.ID(), contract.EventRunFinished); end.Outcome != contract.Succeeded {
		t.Fatalf("first B terminal: %+v", end)
	}
	commitSessionRun(t, ctx, b, b1.ID(), 1)
	select {
	case <-cancelled:
		t.Fatal("A tool stopped before B committed")
	default:
	}
	assertSessionVersion(t, ctx, a, 0, 0)

	b2 := startSessionRun(t, ctx, b, sessionRequest(b, "B2"))
	b2Events := observeSessionRun(t, ctx, b2)
	if first := recvSessionEvent(t, b2Events, b2.ID(), contract.EventTextDelta); first.TextDelta != "B2" {
		t.Fatalf("second B stream: %+v", first)
	}
	if err := aRun.Cancel(ctx); err != nil {
		t.Fatal(err)
	}
	afterCancelCtx, cancelWait := context.WithTimeout(ctx, 5*time.Millisecond)
	_, afterCancelErr := aRun.Wait(afterCancelCtx)
	cancelWait()
	if !errors.Is(afterCancelErr, context.DeadlineExceeded) {
		t.Fatalf("cancel request completed the Run before the tool returned: %v", afterCancelErr)
	}
	assertSessionVersion(t, ctx, a, 0, 0)
	releaseTool()
	close(secondDelta)
	if after := recvSessionEvent(t, b2Events, b2.ID(), contract.EventTextDelta); after.TextDelta != " finished" {
		t.Fatalf("B stream after cancelling A: %+v", after)
	}
	if end := recvSessionEvent(t, b2Events, b2.ID(), contract.EventRunFinished); end.Outcome != contract.Succeeded {
		t.Fatalf("second B terminal: %+v", end)
	}
	commitSessionRun(t, ctx, b, b2.ID(), 2)

	result := waitSessionRun(t, ctx, aRun, contract.Cancelled)
	if committed := <-aCommit; committed.err != nil || committed.receipt.RunID != aRun.ID() || committed.receipt.Version != 1 {
		t.Fatalf("A commit: %+v", committed)
	}
	afterA := assertSessionVersion(t, ctx, a, 1, 3)
	afterB := assertSessionVersion(t, ctx, b, 2, 4)
	if len(result.Messages) != 3 || afterA.Messages[0].User == nil || afterA.Messages[0].User.Text != "A" ||
		afterA.Messages[2].Tool == nil || afterA.Messages[2].Tool.Result.RunID != aRun.ID() ||
		afterA.Messages[2].Tool.Result.CallID != "ready" || afterB.Messages[3].Assistant == nil ||
		afterB.Messages[3].Assistant.Text != "B2 finished" || toolCalls.Load() != 2 {
		t.Fatalf("isolated histories and tool calls: A %+v, B %+v, calls %d", afterA, afterB, toolCalls.Load())
	}
	if len(requests) != 4 {
		t.Fatalf("model dispatch count = %d, want 4", len(requests))
	}
	var seenB2 bool
	for range 4 {
		req := <-requests
		if req.RunID != b2.ID() {
			continue
		}
		seenB2 = true
		m := req.Messages
		if len(m) != 3 || m[0].User == nil || m[0].User.Text != "B1" || m[1].Assistant == nil ||
			m[1].Assistant.Text != "B1 finished" || m[2].User == nil || m[2].User.Text != "B2" {
			t.Fatalf("second B Run saw the wrong history: %+v", req)
		}
	}
	if !seenB2 {
		t.Fatal("B2's model input was not captured")
	}
}

func TestStageCSessionTerminalDeltas(t *testing.T) {
	for _, outcome := range []contract.RunOutcome{contract.Failed, contract.ToolOutcomeUnknown} {
		t.Run(string(outcome), func(t *testing.T) {
			root, ctx := t.TempDir(), sessionTestContext(t)
			model := deliveryModel(func(ctx context.Context, req contract.ModelRequest) (contract.ModelStream, error) {
				if req.Step == 1 {
					return sessionToolReply("committed"), nil
				}
				if outcome == contract.Failed {
					return &stagedSessionStream{ctx: ctx, closed: make(chan struct{}),
						events: []contract.ModelEvent{{Kind: contract.ModelTextDelta, TextDelta: "draft"}}}, nil
				}
				return sessionToolReply("uncommitted", "unknown"), nil
			})
			tool := deliveryTool(func(_ context.Context, req contract.ToolRequest) (contract.ToolResult, error) {
				if req.CallID == "unknown" {
					return contract.ToolResult{}, errors.New("no trustworthy receipt")
				}
				return knownSessionTool(req), nil
			})
			client := sessionTestClient(t, root, "v1", time.Minute, model, tool)
			s := openTestSession(t, ctx, client, root, "terminal")
			req := sessionRequest(s, "input")
			req.Tools = []contract.ToolRef{"tool"}
			handle := startSessionRun(t, ctx, s, req)
			result := waitSessionRun(t, ctx, handle, outcome)
			commitSessionRun(t, ctx, s, handle.ID(), 1)
			turn, err := s.ReadTurn(ctx, 1)
			if err != nil || turn.Outcome != outcome || len(turn.Messages) != 3 || len(result.Messages) != 3 ||
				turn.Messages[0].User == nil || turn.Messages[0].User.Text != "input" || turn.Messages[1].Assistant == nil ||
				strings.Contains(turn.Messages[1].Assistant.Text, "draft") || turn.Messages[2].Tool == nil ||
				turn.Messages[2].Tool.Result.CallID != "committed" || turn.Messages[2].Tool.Result.RunID != handle.ID() {
				t.Fatalf("persisted delta = %+v (%v), Wait = %+v", turn, err, result)
			}
		})
	}
}
