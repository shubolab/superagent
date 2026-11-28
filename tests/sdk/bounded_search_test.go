//go:build darwin || linux

package superagent_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/shubolab/superagent"
)

type toolFunc func(context.Context, superagent.ToolRequest) (superagent.ToolResult, error)

func (f toolFunc) Invoke(ctx context.Context, req superagent.ToolRequest) (superagent.ToolResult, error) {
	return f(ctx, req)
}

var _ superagent.Tool = toolFunc(nil)

type environmentProbe struct {
	superagent.EnvironmentProvider
	allocates, grants, opens, revokes, releases atomic.Int32
	scanned                                     atomic.Uint64
	onGrant                                     func(context.Context)
	releaseErr                                  error
}

func (e *environmentProbe) Allocate(ctx context.Context, scope superagent.RunScope) (superagent.EnvironmentHandle, error) {
	e.allocates.Add(1)
	return e.EnvironmentProvider.Allocate(ctx, scope)
}

func (e *environmentProbe) Grant(ctx context.Context, scope superagent.CallScope, prefix string) (superagent.EnvironmentGrantRef, error) {
	e.grants.Add(1)
	ref, err := e.EnvironmentProvider.Grant(ctx, scope, prefix)
	if e.onGrant != nil {
		e.onGrant(ctx)
	}
	return ref, err
}

func (e *environmentProbe) OpenRead(ctx context.Context, scope superagent.CallScope, ref superagent.EnvironmentGrantRef, path string) (io.ReadCloser, error) {
	e.opens.Add(1)
	return e.EnvironmentProvider.OpenRead(ctx, scope, ref, path)
}

func (e *environmentProbe) Revoke(ctx context.Context, scope superagent.CallScope, ref superagent.EnvironmentGrantRef) (uint64, error) {
	e.revokes.Add(1)
	scanned, err := e.EnvironmentProvider.Revoke(ctx, scope, ref)
	e.scanned.Add(scanned)
	return scanned, err
}

func (e *environmentProbe) Release(ctx context.Context, scope superagent.RunScope, handle superagent.EnvironmentHandle) error {
	e.releases.Add(1)
	return errors.Join(e.EnvironmentProvider.Release(ctx, scope, handle), e.releaseErr)
}

type searchFixture struct {
	cfg    superagent.MeshConfig
	env    *environmentProbe
	work   superagent.EnvironmentRequirement
	calls  chan modelCall
	first  *controlledStream
	second *controlledStream
	hits   atomic.Int32
}

func newSearchFixture(t *testing.T) *searchFixture {
	t.Helper()
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, "docs"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "docs", "health.txt"), []byte("status=ready\n"), 0600); err != nil {
		t.Fatal(err)
	}
	env, err := superagent.NewLocalReadEnvironment(map[superagent.WorkspaceRef]superagent.WorkspaceConfig{"workspace": {Root: dir, ReadPrefixes: []string{"docs"}}})
	if err != nil {
		t.Fatal(err)
	}
	f := &searchFixture{
		env: &environmentProbe{EnvironmentProvider: env}, work: superagent.EnvironmentRequirement{Ref: "local", Workspace: "workspace"},
		calls: make(chan modelCall, 4), first: scripted(), second: scripted(),
	}
	cfg := hostConfig(modelFunc(func(ctx context.Context, req superagent.ModelRequest) (superagent.ModelStream, error) {
		f.calls <- modelCall{ctx: ctx, req: req}
		if req.Step == 1 {
			return f.first, nil
		}
		return f.second, nil
	}))
	runner := cfg.LocalRunners["main"]
	runner.AllowedTools = map[superagent.SubjectID]map[superagent.ToolRef]superagent.ToolPermission{
		"alice": {"search": {Environment: f.work, ReadPrefix: "docs"}},
	}
	runner.DefaultBudget.MaxModelRequests = pointer(uint64(2))
	runner.DefaultBudget.MaxOutputBytes = pointer(uint64(4096))
	runner.DefaultBudget.MaxToolRequests = pointer(uint64(1))
	runner.DefaultBudget.MaxToolScanBytes = pointer(uint64(32))
	runner.BudgetCaps = runner.DefaultBudget
	runner.BudgetCaps.MaxToolRequests = pointer(uint64(4))
	cfg.LocalRunners["main"] = runner
	binding := superagent.NewBoundedFileSearch()
	original := binding.Tool
	binding.Tool = toolFunc(func(ctx context.Context, req superagent.ToolRequest) (superagent.ToolResult, error) {
		f.hits.Add(1)
		return original.Invoke(ctx, req)
	})
	cfg.LocalTools = map[superagent.ToolRef]superagent.LocalToolBinding{"search": binding}
	cfg.LocalEnvironments = map[superagent.EnvironmentRef]superagent.EnvironmentProvider{"local": f.env}
	f.cfg = cfg
	return f
}

func (f *searchFixture) request() superagent.StartRequest {
	req := request("search-run")
	req.Instruction = "answer with evidence"
	req.Input.Text = "查一下服务状态"
	req.Tools = []superagent.ToolRef{"search"}
	req.Environment = pointer(f.work)
	return req
}

func searchCall(id superagent.CallID, path string) superagent.ModelToolCall {
	args, _ := json.Marshal(map[string]string{"path": path, "query": "status"})
	return superagent.ModelToolCall{CallID: id, Tool: "search", Arguments: args}
}

func searchCompleted(text string, calls ...superagent.ModelToolCall) superagent.ModelEvent {
	return superagent.ModelEvent{Kind: superagent.ModelCompleted, Reply: &superagent.ModelReply{
		Finish: superagent.ModelFinishToolCalls, Text: text, ToolCalls: calls,
	}}
}

func allRunEvents(t *testing.T, stream superagent.EventStream) []superagent.Event {
	t.Helper()
	defer stream.Close()
	var events []superagent.Event
	for {
		event, err := stream.Recv()
		if err == io.EOF {
			return events
		}
		if err != nil {
			t.Fatal(err)
		}
		events = append(events, event)
	}
}

func observeRun(t *testing.T, ctx context.Context, run *superagent.Run) superagent.EventStream {
	t.Helper()
	stream, err := run.Observe(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return stream
}

func TestBoundedSearchPublicFlow(t *testing.T) {
	f := newSearchFixture(t)
	ctx := hostContext(t, "alice")
	req := f.request()
	answer := "服务状态为 ready。"
	// Get the exact successful usage, and exercise that same boundary on a separate determination.
	for _, exact := range []bool{false, true} {
		if exact {
			f = newSearchFixture(t)
		}
		run := start(t, ctx, newClient(t, f.cfg), req)
		stream := observeRun(t, ctx, run)
		first := recv(t, ctx, f.calls).req
		if first.Step != 1 || len(first.Messages) != 1 || first.Messages[0].User.Text != req.Input.Text ||
			first.Instruction != req.Instruction || len(first.Tools) != 1 || first.Tools[0].Ref != "search" {
			t.Fatalf("first request = %+v", first)
		}
		f.first.events <- delta("查找")
		for _, want := range []superagent.EventKind{superagent.EventRunStarted, superagent.EventTextDelta} {
			event, err := stream.Recv()
			if err != nil || event.Kind != want {
				t.Fatalf("before complete: %+v, %v", event, err)
			}
		}
		if f.hits.Load() != 0 || f.env.opens.Load() != 0 {
			t.Fatal("execution before ModelCompleted")
		}
		call := searchCall("c1", "docs/health.txt")
		f.first.events <- searchCompleted("查找", call)
		second := recv(t, ctx, f.calls).req
		if second.Step != 2 || len(second.Messages) != 3 || second.Messages[1].Assistant.ToolCalls[0].CallID != "c1" ||
			second.Instruction != req.Instruction || !reflect.DeepEqual(first.Tools, second.Tools) {
			t.Fatalf("second request = %+v", second)
		}
		receipt := second.Messages[2].Tool.Result
		var report superagent.SearchReport
		if receipt.RunID != req.ID || receipt.CallID != "c1" || receipt.State != superagent.ToolExecutionKnown ||
			receipt.Completion.Outcome != superagent.ToolSucceeded || *receipt.Usage.ScannedBytes != 13 ||
			json.Unmarshal([]byte(receipt.Completion.Content), &report) != nil || report.Coverage != "complete" ||
			report.ScannedBytes != 13 || !reflect.DeepEqual(report.Matches, []superagent.SearchMatch{{Line: 1, Preview: "status=ready"}}) {
			t.Fatalf("receipt = %+v, report = %+v", receipt, report)
		}
		encoded, _ := json.Marshal(receipt)
		consumed := uint64(len("查找") + len(call.CallID) + len(call.Tool) + len(call.Arguments) + len(encoded))
		if first.MaxOutputBytes-second.MaxOutputBytes != consumed {
			t.Fatalf("deduplicated model output and full receipt cost = %d, want %d", first.MaxOutputBytes-second.MaxOutputBytes, consumed)
		}
		f.second.events <- delta(answer)
		f.second.events <- completed(answer)
		result := wait(t, ctx, run, superagent.Succeeded)
		if result.Answer.Text != answer || len(result.Messages) != 4 || !reflect.DeepEqual(result.Messages[:3], second.Messages) || result.Cleanup != superagent.CleanupReleased {
			t.Fatalf("result = %+v", result)
		}
		if repeated := wait(t, ctx, run, superagent.Succeeded); !reflect.DeepEqual(repeated, result) {
			t.Fatal("repeat Wait changed result")
		}
		wantEvents := []superagent.EventKind{
			superagent.EventMessageCompleted, superagent.EventToolDispatched, superagent.EventToolCompleted,
			superagent.EventTextDelta, superagent.EventMessageCompleted, superagent.EventRunFinished,
		}
		events := allRunEvents(t, stream)
		if len(events) != len(wantEvents) || f.hits.Load() != 1 || f.env.allocates.Load() != 1 || f.env.grants.Load() != 1 ||
			f.env.opens.Load() != 1 || f.env.revokes.Load() != 1 || f.env.releases.Load() != 1 || f.env.scanned.Load() != 13 || len(f.calls) != 0 {
			t.Fatalf("events or component counts incorrect: %+v", events)
		}
		for i, event := range events {
			if event.RunID != req.ID || event.Seq != uint64(i+3) || event.Kind != wantEvents[i] {
				t.Fatalf("event[%d] = %+v", i, event)
			}
		}
		if events[2].Tool.CallID != "c1" || events[2].Tool.Status != superagent.ToolStatusSucceeded || events[5].Outcome != result.Outcome {
			t.Fatalf("unexpected completion events: %+v", events)
		}
		req.Budget.MaxOutputBytes = pointer(consumed + uint64(len(answer)))
	}
}

func TestBoundedSearchAdmissionAndIncompleteReply(t *testing.T) {
	t.Run("admission", func(t *testing.T) {
		f := newSearchFixture(t)
		client := newClient(t, f.cfg)
		for _, tt := range []struct {
			subject superagent.SubjectID
			change  func(*superagent.StartRequest)
			err     error
		}{
			{"bob", func(*superagent.StartRequest) {}, superagent.ErrAccessDenied},
			{"alice", func(r *superagent.StartRequest) { r.Tools[0] = "missing" }, superagent.ErrBindingNotFound},
			{"alice", func(r *superagent.StartRequest) { r.Environment.Workspace = "other" }, superagent.ErrAccessDenied},
		} {
			req := f.request()
			_, err := client.Start(hostContext(t, tt.subject), reqAfter(req, tt.change))
			if !errors.Is(err, tt.err) {
				t.Fatalf("Start error %v, want %v", err, tt.err)
			}
		}
		if len(f.calls) != 0 || f.hits.Load() != 0 || f.env.allocates.Load() != 0 {
			t.Fatal("invalid Start ran a component")
		}
	})
	good := searchCall("c1", "docs/health.txt")
	for name, calls := range map[string][]superagent.ModelToolCall{
		"early EOF": nil, "repeated ID": {good, good}, "missing ID": {searchCall("", "docs/health.txt")},
		"not object":   {good, {CallID: "c2", Tool: "search", Arguments: json.RawMessage(`[]`)}},
		"invalid JSON": {{CallID: "c1", Tool: "search", Arguments: json.RawMessage(`{"path":`)}},
	} {
		t.Run(name, func(t *testing.T) {
			f := newSearchFixture(t)
			ctx := hostContext(t, "alice")
			run := start(t, ctx, newClient(t, f.cfg), f.request())
			f.first.events <- delta("working")
			if calls == nil {
				close(f.first.events)
			} else {
				f.first.events <- searchCompleted("working", calls...)
			}
			result := wait(t, ctx, run, superagent.Failed)
			if len(result.Messages) != 1 || f.hits.Load() != 0 || f.env.grants.Load() != 0 || len(f.calls) != 1 {
				t.Fatalf("invalid stream caused dispatch or commit: %+v", result)
			}
		})
	}
}

func reqAfter(req superagent.StartRequest, change func(*superagent.StartRequest)) superagent.StartRequest {
	change(&req)
	return req
}

func TestBoundedSearchRefusesAndAccounts(t *testing.T) {
	for _, tc := range []struct {
		name    string
		path    string
		status  superagent.ToolStatus
		invokes int32
	}{
		{"outside", "secret.txt", superagent.ToolStatusRejected, 0},
		{"inside but missing", "docs/missing", superagent.ToolStatusFailed, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newSearchFixture(t)
			ctx := hostContext(t, "alice")
			run := start(t, ctx, newClient(t, f.cfg), f.request())
			f.first.events <- searchCompleted("", searchCall("c1", tc.path))
			first := recv(t, ctx, f.calls).req
			second := recv(t, ctx, f.calls).req
			receipt := second.Messages[2].Tool.Result
			if receipt.State != superagent.ToolExecutionNotExecuted || receipt.CallID != "c1" || receipt.Reason == nil ||
				*receipt.Usage.ScannedBytes != 0 || f.hits.Load() != tc.invokes || f.env.scanned.Load() != 0 {
				t.Fatalf("unexpected refusal: %+v", receipt)
			}
			encoded, _ := json.Marshal(receipt)
			call := searchCall("c1", tc.path)
			if got := first.MaxOutputBytes - second.MaxOutputBytes; got != uint64(len(encoded)+len(call.CallID)+len(call.Tool)+len(call.Arguments)) {
				t.Fatalf("refusal output cost = %d", got)
			}
			f.second.events <- searchCompleted("", searchCall("c2", "docs/health.txt"))
			result := wait(t, ctx, run, superagent.BudgetExceeded)
			if result.Stop.Code != superagent.StopToolRequestLimit || len(result.Messages) != 3 || f.hits.Load() != tc.invokes {
				t.Fatalf("refused attempt did not consume a slot: %+v", result)
			}
			events := allRunEvents(t, observeRun(t, ctx, run))
			for _, event := range events {
				if event.Kind == superagent.EventToolCompleted && event.Tool.CallID == "c1" && event.Tool.Status == tc.status {
					return
				}
			}
			t.Fatalf("no matching completion: %+v", events)
		})
	}
}

func TestBoundedSearchBudgetGates(t *testing.T) {
	for _, tc := range []struct {
		name     string
		budget   superagent.Budget
		stop     superagent.StopCode
		invokes  int32
		messages int
	}{
		{"tool attempts", superagent.Budget{MaxToolRequests: pointer(uint64(0))}, superagent.StopToolRequestLimit, 0, 1},
		{"minimum receipt", superagent.Budget{MaxOutputBytes: pointer(uint64(95))}, superagent.StopToolReceiptLimit, 0, 1},
		{"scan exhausted", superagent.Budget{MaxToolScanBytes: pointer(uint64(0))}, superagent.StopToolScanLimit, 0, 1},
		{"next model attempt", superagent.Budget{MaxModelRequests: pointer(uint64(1))}, superagent.StopModelRequestLimit, 1, 3},
		{"next model input", superagent.Budget{MaxModelInputBytes: pointer(uint64(800))}, superagent.StopModelInputLimit, 1, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newSearchFixture(t)
			ctx := hostContext(t, "alice")
			req := f.request()
			req.Budget = tc.budget
			run := start(t, ctx, newClient(t, f.cfg), req)
			f.first.events <- searchCompleted("", searchCall("c1", "docs/health.txt"))
			result := wait(t, ctx, run, superagent.BudgetExceeded)
			if result.Stop == nil || result.Stop.Code != tc.stop || f.hits.Load() != tc.invokes ||
				len(result.Messages) != tc.messages || len(f.calls) != 1 {
				t.Fatalf("budget result = %+v, stop = %+v, invokes = %d, model calls = %d", result, result.Stop, f.hits.Load(), len(f.calls))
			}
		})
	}
}

func TestBoundedSearchSerialBatch(t *testing.T) {
	f := newSearchFixture(t)
	ctx := hostContext(t, "alice")
	entered := make(chan superagent.ToolRequest, 2)
	release := make(chan struct{})
	binding := f.cfg.LocalTools["search"]
	original := binding.Tool
	binding.Tool = toolFunc(func(ctx context.Context, req superagent.ToolRequest) (superagent.ToolResult, error) {
		entered <- req
		if req.CallID == "c1" {
			select {
			case <-ctx.Done():
				return superagent.ToolResult{}, ctx.Err()
			case <-release:
			}
		}
		return original.Invoke(ctx, req)
	})
	f.cfg.LocalTools["search"] = binding
	req := f.request()
	req.Budget.MaxToolRequests = pointer(uint64(2))
	run := start(t, ctx, newClient(t, f.cfg), req)
	f.first.events <- searchCompleted("", searchCall("c1", "docs/health.txt"), searchCall("c2", "docs/health.txt"))
	_ = recv(t, ctx, f.calls)
	if first := recv(t, ctx, entered); first.CallID != "c1" || first.Limits.MaxScanBytes != 32 || len(entered) != 0 || len(f.calls) != 0 || f.env.grants.Load() != 1 {
		t.Fatalf("serial first call = %+v", first)
	}
	close(release)
	if second := recv(t, ctx, entered); second.CallID != "c2" || second.Limits.MaxScanBytes != 19 || second.Limits.MaxResultBytes >= 4096 {
		t.Fatalf("second call before actual settlement: %+v", second)
	}
	second := recv(t, ctx, f.calls).req
	if len(second.Messages) != 4 || second.Messages[2].Tool.Result.CallID != "c1" || second.Messages[3].Tool.Result.CallID != "c2" {
		t.Fatalf("batch order = %+v", second.Messages)
	}
	f.second.events <- completed("verified")
	result := wait(t, ctx, run, superagent.Succeeded)
	if f.env.scanned.Load() != 26 || len(result.Messages) != 5 {
		t.Fatalf("serial result = %+v, meter = %d", result, f.env.scanned.Load())
	}
}

func TestBoundedSearchStopsAtDispatchBoundary(t *testing.T) {
	t.Run("cancel before dispatch", func(t *testing.T) {
		f := newSearchFixture(t)
		ctx := hostContext(t, "alice")
		grant := make(chan struct{})
		f.env.onGrant = func(ctx context.Context) { close(grant); <-ctx.Done() }
		run := start(t, ctx, newClient(t, f.cfg), f.request())
		f.first.events <- searchCompleted("", searchCall("c1", "docs/health.txt"))
		recv(t, ctx, grant)
		if err := run.Cancel(ctx); err != nil {
			t.Fatal(err)
		}
		result := wait(t, ctx, run, superagent.Cancelled)
		if f.env.revokes.Load() != 1 || f.env.releases.Load() != 1 ||
			f.hits.Load() != 0 || f.env.opens.Load() != 0 || len(result.Messages) != 1 || result.Cleanup != superagent.CleanupReleased {
			t.Fatalf("pre-dispatch cancellation: %+v", result)
		}
	})
	for _, tc := range []struct {
		name                                      string
		waitForDeadline, loseReceipt, failCleanup bool
		outcome                                   superagent.RunOutcome
	}{
		{"known cancel", false, false, false, superagent.Cancelled},
		{"known timeout", true, false, false, superagent.TimedOut},
		{"unknown cancel", false, true, false, superagent.ToolOutcomeUnknown},
		{"unknown cleanup pending", false, true, true, superagent.ToolOutcomeUnknown},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newSearchFixture(t)
			ctx := hostContext(t, "alice")
			started := make(chan struct{})
			unblock := make(chan struct{})
			defer close(unblock)
			if tc.failCleanup {
				f.env.releaseErr = errors.New("cleanup acknowledgement lost")
			}
			binding := f.cfg.LocalTools["search"]
			binding.Tool = toolFunc(func(ctx context.Context, req superagent.ToolRequest) (superagent.ToolResult, error) {
				f.hits.Add(1)
				close(started)
				<-ctx.Done()
				if tc.loseReceipt {
					<-unblock
				}
				return superagent.ToolResult{
					RunID: req.RunID, CallID: req.CallID, State: superagent.ToolExecutionKnown,
					Completion: &superagent.ToolCompletion{Outcome: superagent.ToolCancelled},
					Reason:     &superagent.ToolReason{Code: "cancelled"},
				}, nil
			})
			f.cfg.LocalTools["search"] = binding
			req := f.request()
			if tc.waitForDeadline {
				req.Budget.Timeout = pointer(500 * time.Millisecond)
			}
			req.Budget.MaxToolRequests = pointer(uint64(2))
			run := start(t, ctx, newClient(t, f.cfg), req)
			f.first.events <- searchCompleted("", searchCall("c1", "docs/health.txt"), searchCall("c2", "docs/health.txt"))
			recv(t, ctx, started)
			if !tc.waitForDeadline {
				if err := run.Cancel(ctx); err != nil {
					t.Fatal(err)
				}
			}
			result := wait(t, ctx, run, tc.outcome)
			cleanup := superagent.CleanupReleased
			if tc.failCleanup {
				cleanup = superagent.CleanupPending
			}
			if result.Stop.CallID != "c1" || f.hits.Load() != 1 || f.env.revokes.Load() != 1 || f.env.releases.Load() != 1 ||
				len(f.calls) != 1 || len(result.Messages) != 1 || result.Cleanup != cleanup {
				t.Fatalf("in-flight cancellation: %+v, stop = %+v", result, result.Stop)
			}
		})
	}
}
