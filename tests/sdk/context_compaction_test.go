package superagent_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/shubolab/superagent"
)

const sourceRef superagent.ToolRef = "original_context"

type compactFixture struct {
	modelFunc
	calls   atomic.Int32
	measure func(superagent.ModelRequest) uint64
}

func (*compactFixture) ContextLimits() superagent.ModelContextLimits {
	return superagent.ModelContextLimits{WindowTokens: 220}
}

func compactMeasure(req superagent.ModelRequest) uint64 {
	if isSummary(req) {
		var sources []struct{ Ref, Original string }
		_ = json.Unmarshal([]byte(req.Messages[0].User.Text), &sources)
		return 20 + 8*uint64(len(sources))
	}
	tokens := uint64(10 + 4*len(req.Context.Summary))
	for _, message := range req.Messages {
		switch {
		case message.User != nil:
			tokens += 5
		case message.Assistant != nil:
			if strings.HasPrefix(message.Assistant.Text, "evidenc") {
				tokens += 100
			}
		case message.Tool != nil && message.Tool.Result.Completion != nil:
			content := message.Tool.Result.Completion.Content
			switch {
			case strings.Contains(content, "medium"):
				tokens += 75
			case strings.HasPrefix(content, "large"):
				tokens += 140
			case content == "smallish":
				tokens += 58
			default:
				tokens += 4
			}
		}
	}
	return tokens
}

func (m *compactFixture) MeasureInput(_ context.Context, req superagent.ModelRequest) (superagent.InputTokenMeasurement, error) {
	bytes, err := encodeMeterFixture(req)
	tokens := compactMeasure(req)
	if m.measure != nil {
		tokens = m.measure(req)
	}
	return superagent.InputTokenMeasurement{
		Tokens: tokens, Kind: superagent.MeasurementExact, ProfileVersion: testProfile,
		InputDigest: superagent.InputDigest(string(req.Model), testProfile, bytes),
	}, err
}

func (m *compactFixture) Generate(ctx context.Context, req superagent.ModelRequest) (superagent.ModelStream, error) {
	bytes, err := encodeMeterFixture(req)
	if err != nil {
		return nil, err
	}
	if err := superagent.VerifyInputDigest(req.Context, string(req.Model), testProfile, bytes); err != nil {
		return nil, err
	}
	m.calls.Add(1)
	return m.modelFunc(ctx, req)
}

func isSummary(req superagent.ModelRequest) bool {
	return strings.HasPrefix(req.Instruction, "Summarize the supplied original history blocks")
}

func compactConfig(m superagent.ModelProvider) superagent.MeshConfig {
	cfg := hostConfig(m)
	rc := cfg.LocalRunners["main"]
	rc.DefaultBudget.MaxModelRequests = pointer(uint64(8))
	rc.DefaultBudget.MaxToolRequests = pointer(uint64(3))
	rc.DefaultBudget.MaxOutputBytes = pointer(uint64(4096))
	rc.BudgetCaps = rc.DefaultBudget
	rc.ContextPolicies = map[superagent.ModelRef]superagent.ModelContextPolicy{
		"chat": {MaxOutputTokens: 20, Compaction: &superagent.ContextCompactionPolicy{
			TriggerRatio: .75, TargetRatio: .60, SummaryMaxTokens: 16, SummaryMaxBytes: 256, MinSavedTokens: 10,
			MaxCompactionsPerRun: 3, SourcePageBytes: 3, SourceTool: sourceRef,
		}},
	}
	rc.AllowedTools = map[superagent.SubjectID]map[superagent.ToolRef]superagent.ToolPermission{
		"alice": {sourceRef: {}, "data": {}},
	}
	cfg.LocalRunners["main"] = rc
	cfg.LocalTools = map[superagent.ToolRef]superagent.LocalToolBinding{
		sourceRef: superagent.NewContextSourceTool(),
		"data": {Spec: superagent.ToolSpec{InputSchema: json.RawMessage(`{}`), Limits: superagent.ToolLimits{MaxResultBytes: 2048}},
			Tool: compactTool(func(_ context.Context, req superagent.ToolRequest) (superagent.ToolResult, error) {
				return dataReceipt(req.RunID, req.CallID, "medium"), nil
			})},
	}
	return cfg
}

type compactTool func(context.Context, superagent.ToolRequest) (superagent.ToolResult, error)

func (f compactTool) Invoke(ctx context.Context, req superagent.ToolRequest) (superagent.ToolResult, error) {
	return f(ctx, req)
}

func dataReceipt(id superagent.RunID, call superagent.CallID, content string) superagent.ToolResult {
	return superagent.ToolResult{RunID: id, CallID: call, State: superagent.ToolExecutionKnown,
		Completion: &superagent.ToolCompletion{Outcome: superagent.ToolSucceeded, Content: content}}
}

func dataCall(id superagent.CallID) superagent.ModelEvent {
	return searchOrCompactionCall("data", id, `{}`)
}

func searchOrCompactionCall(tool superagent.ToolRef, id superagent.CallID, args string) superagent.ModelEvent {
	return superagent.ModelEvent{Kind: superagent.ModelCompleted, Reply: &superagent.ModelReply{
		Finish: superagent.ModelFinishToolCalls, ToolCalls: []superagent.ModelToolCall{{Tool: tool, CallID: id, Arguments: json.RawMessage(args)}},
	}}
}

func compactRequest() superagent.StartRequest {
	req := request("compact")
	req.Instruction = "Only use authorized tools."
	req.Tools = []superagent.ToolRef{sourceRef, "data"}
	req.History = []superagent.Message{
		{User: &superagent.UserMessage{Text: "不要执行写入。"}},
		{Assistant: &superagent.AssistantMessage{Text: "evidence"}},
		{Assistant: &superagent.AssistantMessage{Text: "evidenc2"}},
		{User: &superagent.UserMessage{Text: "更正：保留所有用户原文。"}},
	}
	return req
}

type promptSource struct {
	Ref      superagent.ContextSourceRef
	Original string
}

func summarySources(req superagent.ModelRequest) ([]promptSource, error) {
	var sources []promptSource
	err := json.Unmarshal([]byte(req.Messages[0].User.Text), &sources)
	return sources, err
}

func summaryReply(sources ...superagent.ContextSourceRef) superagent.ModelEvent {
	text, _ := json.Marshal([]superagent.ContextSummaryEntry{{Section: "decisions", Text: "compact", Sources: sources}})
	return completed(string(text))
}

func TestContinuousCompactionPreservesUsersAndProvidesSources(t *testing.T) {
	ctx := hostContext(t, "alice")
	var summaryRequests, modelRequests, toolHits int
	var first, second, third superagent.ContextSourceRef
	model := &compactFixture{modelFunc: func(_ context.Context, req superagent.ModelRequest) (superagent.ModelStream, error) {
		if isSummary(req) {
			summaryRequests++
			sources, err := summarySources(req)
			if err != nil || len(req.Tools) != 0 || req.Context.MaxOutputTokens != 16 || len(sources) == 0 {
				t.Errorf("invalid summary request at step %d, sources %v, err %v", req.Step, sources, err)
				return nil, superagent.ErrInvalidRequest
			}
			switch summaryRequests {
			case 1:
				first = sources[0].Ref
				if sources[0].Original != "evidence" {
					t.Errorf("first original = %q", sources[0].Original)
				}
				return scripted(summaryReply(first)), nil
			case 2:
				second = sources[0].Ref
				if sources[0].Original != "evidenc2" {
					t.Errorf("second original = %q", sources[0].Original)
				}
				return scripted(summaryReply(second)), nil
			case 3:
				third = sources[0].Ref
				if !strings.Contains(sources[0].Original, "batch-one") {
					t.Errorf("third original = %q", sources[0].Original)
				}
				return scripted(summaryReply(third)), nil
			}
			t.Error("unexpected summary")
			return nil, superagent.ErrInvalidRequest
		}
		modelRequests++
		var users []string
		prior := 0
		for i, msg := range req.Messages {
			if msg.User != nil {
				users = append(users, msg.User.Text)
			}
			if msg.Assistant != nil && len(msg.Assistant.ToolCalls) != 0 && msg.Assistant.ToolCalls[0].CallID == "same-history-id" {
				prior++
				if i+1 >= len(req.Messages) || req.Messages[i+1].Tool == nil || req.Messages[i+1].Tool.Result.CallID != "same-history-id" {
					t.Error("broken historical pairing")
				}
			}
		}
		if strings.Join(users, "\n") != "不要执行写入。\n更正：保留所有用户原文。\ngreet" || prior != 2 ||
			req.Instruction != "Only use authorized tools." || len(req.Tools) != 2 {
			t.Errorf("protected inputs changed at step %d, user count %d, history pairs %d", req.Step, len(users), prior)
		}
		switch modelRequests {
		case 1:
			if len(req.Context.Summary) != 1 || req.Step != 2 {
				t.Errorf("first view at %d has %d summaries", req.Step, len(req.Context.Summary))
			}
			return scripted(dataCall("batch-one")), nil
		case 2:
			if len(req.Context.Summary) != 2 || req.Step != 4 {
				t.Errorf("second view at %d has %d summaries", req.Step, len(req.Context.Summary))
			}
			return scripted(dataCall("batch-two")), nil
		case 3:
			if len(req.Context.Summary) != 3 || req.Step != 6 {
				t.Errorf("third view at %d has %d summaries", req.Step, len(req.Context.Summary))
			}
			for index, want := range []superagent.ContextSourceRef{first, second, third} {
				if req.Context.Summary[index].Sources[0] != want {
					t.Errorf("summary source changed at index %d", index)
				}
			}
			args, _ := json.Marshal(map[string]any{"ref": first, "offset": 0, "max_bytes": 10})
			return scripted(searchOrCompactionCall(sourceRef, "read-first", string(args))), nil
		default:
			last := req.Messages[len(req.Messages)-1].Tool
			var page superagent.ContextSourcePage
			if last == nil || json.Unmarshal([]byte(last.Result.Completion.Content), &page) != nil ||
				string(page.Content) != "evi" || page.EOF || page.NextOffset != 3 {
				t.Error("source tool returned the wrong original bytes")
			}
			return scripted(completed("done")), nil
		}
	}}
	cfg := compactConfig(model)
	binding := cfg.LocalTools["data"]
	orig := binding.Tool
	binding.Tool = compactTool(func(ctx context.Context, req superagent.ToolRequest) (superagent.ToolResult, error) {
		toolHits++
		return orig.Invoke(ctx, req)
	})
	cfg.LocalTools["data"] = binding
	req := compactRequest()
	for _, id := range []superagent.RunID{"history-a", "history-b"} {
		req.History = append(req.History,
			superagent.Message{Assistant: &superagent.AssistantMessage{ToolCalls: []superagent.ModelToolCall{{
				CallID: "same-history-id", Tool: "data", Arguments: json.RawMessage(`{}`),
			}}}},
			superagent.Message{Tool: &superagent.ToolMessage{Tool: "data", Result: dataReceipt(id, "same-history-id", "historic")}})
		req.PinnedHistory = append(req.PinnedHistory, len(req.History)-1)
	}
	client := newClient(t, cfg)
	run := start(t, ctx, client, req)
	result := wait(t, ctx, run, superagent.Succeeded)
	if summaryRequests != 3 || modelRequests != 4 || toolHits != 2 || len(result.Messages) != 8 ||
		result.Messages[2].Tool.Result.Completion.Content != "medium" || result.Answer.Text != "done" {
		t.Fatalf("summary calls %d, normal calls %d, tools %d, original records %d", summaryRequests, modelRequests, toolHits, len(result.Messages))
	}
	if len(result.ContextCompactions) != 3 {
		t.Fatalf("expected 3 diagnostics, got %+v", result.ContextCompactions)
	}
	for _, trace := range result.ContextCompactions {
		if trace.Selection != "summary" || !trace.HasCoverage || trace.SummaryInputBytes == 0 || trace.SummaryOutputBytes == 0 ||
			trace.BeforeTokens <= trace.AfterTokens {
			t.Fatalf("incomplete compaction trace: %+v", trace)
		}
	}
	for offset, want := range map[uint64]string{0: "evi", 3: "den", 6: "ce"} {
		page, err := run.ReadSource(ctx, first, offset, 8)
		if err != nil || string(page.Content) != want || page.NextOffset != offset+uint64(len(want)) || (offset == 6) != page.EOF {
			t.Fatalf("source page %+v, %v", page, err)
		}
	}
	for _, attempt := range []struct {
		run *superagent.Run
		ctx context.Context
	}{
		{client.Run("another"), ctx}, {run, hostContext(t, "bob")},
	} {
		if _, err := attempt.run.ReadSource(attempt.ctx, first, 0, 3); !errors.Is(err, superagent.ErrContextSourceUnavailable) {
			t.Fatalf("source isolation error = %v", err)
		}
	}
}

func TestCompactionFallbackAndSummaryAdmission(t *testing.T) {
	for _, mode := range []string{"truncated", "invalid ref", "no summary slot", "protected overflow"} {
		t.Run(mode, func(t *testing.T) {
			ctx := hostContext(t, "alice")
			var summaries int
			m := &compactFixture{modelFunc: func(_ context.Context, req superagent.ModelRequest) (superagent.ModelStream, error) {
				if isSummary(req) {
					summaries++
					if mode == "invalid ref" {
						return scripted(summaryReply("wrong-source")), nil
					}
					stream := scripted(delta("draft"))
					close(stream.events)
					return stream, nil
				}
				if len(req.Context.Summary) != 0 || (mode == "truncated" && req.MaxOutputBytes != 4096-5) {
					t.Errorf("fallback did not restore original view or account summary output at step %d", req.Step)
				}
				return scripted(completed("fallback")), nil
			}}
			req := compactRequest()
			req.History = append(req.History[:2], req.History[3:]...)
			cfg := compactConfig(m)
			rc := cfg.LocalRunners["main"]
			p := rc.ContextPolicies["chat"]
			p.Compaction.TriggerRatio, p.Compaction.TargetRatio = .55, .40
			if mode == "no summary slot" {
				req.Budget.MaxModelRequests = pointer(uint64(1))
			}
			if mode == "protected overflow" {
				req.History = compactRequest().History
				req.PinnedHistory = []int{1, 2}
			}
			run := start(t, ctx, newClient(t, cfg), req)
			if mode == "protected overflow" {
				got := wait(t, ctx, run, superagent.BudgetExceeded)
				if m.calls.Load() != 0 || got.Stop.Code != superagent.StopContextWindowLimit {
					t.Fatalf("protected overflow = %+v, requests %d", got.Stop, m.calls.Load())
				}
				return
			}
			wait(t, ctx, run, superagent.Succeeded)
			if mode == "no summary slot" && summaries != 0 || mode != "no summary slot" && summaries != 1 {
				t.Fatalf("summary requests = %d", summaries)
			}
		})
	}
}

func TestCompactionStopsInFlightSummary(t *testing.T) {
	ctx := hostContext(t, "alice")
	entered := make(chan struct{})
	var stream *controlledStream
	model := &compactFixture{modelFunc: func(_ context.Context, req superagent.ModelRequest) (superagent.ModelStream, error) {
		if !isSummary(req) {
			t.Error("normal request dispatched after cancellation")
		}
		stream = scripted()
		close(entered)
		return stream, nil
	}}
	req := compactRequest()
	run := start(t, ctx, newClient(t, compactConfig(model)), req)
	recv(t, ctx, entered)
	if err := run.Cancel(ctx); err != nil {
		t.Fatal(err)
	}
	result := wait(t, ctx, run, superagent.Cancelled)
	recv(t, ctx, stream.closed)
	if len(result.Messages) != 1 || model.calls.Load() != 1 {
		t.Fatalf("cancelled summary changed history: %+v", result)
	}
}

func TestContextSourceExpiresWithRun(t *testing.T) {
	ctx := hostContext(t, "alice")
	var ref superagent.ContextSourceRef
	model := &compactFixture{modelFunc: func(_ context.Context, req superagent.ModelRequest) (superagent.ModelStream, error) {
		if isSummary(req) {
			sources, err := summarySources(req)
			if err != nil {
				return nil, err
			}
			ref = sources[0].Ref
			return scripted(summaryReply(ref)), nil
		}
		return scripted(completed("done")), nil
	}}
	cfg := compactConfig(model)
	policy := cfg.LocalRunners["main"]
	policy.ResultRetention = 250 * time.Millisecond
	cfg.LocalRunners["main"] = policy
	run := start(t, ctx, newClient(t, cfg), compactRequest())
	wait(t, ctx, run, superagent.Succeeded)
	if page, err := run.ReadSource(ctx, ref, 8, 1); err != nil || !page.EOF || len(page.Content) != 0 {
		t.Fatalf("exact EOF page = %+v, %v", page, err)
	}
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		_, err := run.ReadSource(ctx, ref, 0, 3)
		if errors.Is(err, superagent.ErrContextSourceUnavailable) {
			return
		}
		if err != nil && err != io.EOF {
			t.Fatal(err)
		}
		recv(t, ctx, ticker.C)
	}
}

func TestCompactionChoosesDeterministicFallback(t *testing.T) {
	ctx := hostContext(t, "alice")
	var summaryCount int
	var narrowed superagent.ContextSourceRef
	m := &compactFixture{modelFunc: func(_ context.Context, req superagent.ModelRequest) (superagent.ModelStream, error) {
		if isSummary(req) {
			summaryCount++
			return scripted(completed("invalid summary")), nil
		}
		for _, message := range req.Messages {
			if message.Tool != nil {
				var marker struct {
					Source superagent.ContextSourceRef `json:"context_source"`
				}
				if err := json.Unmarshal([]byte(message.Tool.Result.Completion.Content), &marker); err != nil {
					t.Error("old large tool body was not replaced")
				}
				narrowed = marker.Source
				if message.Tool.Result.CallID != "historic" || message.Tool.Result.Completion.Outcome != superagent.ToolSucceeded {
					t.Error("shortening changed the tool receipt")
				}
			}
		}
		return scripted(completed("done")), nil
	}}
	f := compactConfig(m)
	f.LocalRunners["main"].ContextPolicies["chat"].Compaction.TargetRatio = .20
	req := compactRequest()
	req.History = append(req.History[:2], req.History[3:]...)
	req.History = append(req.History,
		superagent.Message{Assistant: &superagent.AssistantMessage{ToolCalls: []superagent.ModelToolCall{{
			CallID: "historic", Tool: "data", Arguments: json.RawMessage(`{}`),
		}}}},
		superagent.Message{Tool: &superagent.ToolMessage{Tool: "data", Result: dataReceipt("old", "historic", "large"+strings.Repeat("x", 500))}})
	run := start(t, ctx, newClient(t, f), req)
	wait(t, ctx, run, superagent.Succeeded)
	page, err := run.ReadSource(ctx, narrowed, 0, 3)
	if summaryCount != 1 || narrowed == "" || err != nil || string(page.Content) != `[{"` {
		t.Fatalf("deterministic fallback: summaries %d, source %q, page %+v, error %v", summaryCount, narrowed, page, err)
	}
}

func TestCompactionFallbackKeepsLatestCommittedBlock(t *testing.T) {
	for _, mode := range []string{"previous view", "full original"} {
		t.Run(mode, func(t *testing.T) {
			ctx := hostContext(t, "alice")
			var calls int
			m := &compactFixture{modelFunc: func(_ context.Context, req superagent.ModelRequest) (superagent.ModelStream, error) {
				if isSummary(req) {
					sources, err := summarySources(req)
					if err != nil {
						return nil, err
					}
					if calls != 0 {
						return scripted(completed("broken")), nil
					}
					return scripted(summaryReply(sources[0].Ref)), nil
				}
				calls++
				if calls == 1 {
					return scripted(dataCall("latest")), nil
				}
				last := req.Messages[len(req.Messages)-1].Tool
				if last == nil || last.Result.CallID != "latest" {
					t.Error("fallback lost latest batch")
				}
				if (mode == "previous view") != (len(req.Context.Summary) == 1) {
					t.Errorf("unexpected fallback summary count %d", len(req.Context.Summary))
				}
				return scripted(completed("done")), nil
			}}
			if mode == "full original" {
				m.measure = func(req superagent.ModelRequest) uint64 {
					tokens := compactMeasure(req)
					if !isSummary(req) && len(req.Context.Summary) != 0 &&
						len(req.Messages) > 0 && req.Messages[len(req.Messages)-1].Tool != nil {
						return tokens + 220
					}
					return tokens
				}
			}
			f := compactConfig(m)
			f.LocalRunners["main"].ContextPolicies["chat"].Compaction.TriggerRatio = .40
			f.LocalRunners["main"].ContextPolicies["chat"].Compaction.TargetRatio = .25
			b := f.LocalTools["data"]
			b.Tool = compactTool(func(_ context.Context, req superagent.ToolRequest) (superagent.ToolResult, error) {
				return dataReceipt(req.RunID, req.CallID, "smallish"), nil
			})
			f.LocalTools["data"] = b
			req := compactRequest()
			if mode == "full original" {
				req.History = append(req.History[:2], req.History[3:]...)
			}
			run := start(t, ctx, newClient(t, f), req)
			res := wait(t, ctx, run, superagent.Succeeded)
			if calls != 2 || len(res.Messages) != 4 || res.Messages[2].Tool.Result.Completion.Content != "smallish" {
				t.Fatalf("fallback result %+v, normal calls %d", res, calls)
			}
		})
	}
}
