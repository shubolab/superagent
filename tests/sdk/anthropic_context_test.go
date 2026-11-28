//go:build darwin || linux

package superagent_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/shubolab/superagent"
)

func profiledAnthropic(t *testing.T, ctx context.Context, endpoint string, count func(context.Context, []byte) (uint64, error)) *superagent.ModelBindings {
	t.Helper()
	profiles := map[string]superagent.AnthropicInputProfile{"metered-id": {
		Limits:  superagent.ModelContextLimits{WindowTokens: 220, MaxInputTokens: 200},
		Version: "local-test-v1", Kind: superagent.MeasurementExact, CountTokens: count,
	}}
	adapter := superagent.NewMeteredAnthropicMessagesAdapter(256, profiles)
	delete(profiles, "metered-id")
	b, err := superagent.NewModelBindings(ctx, superagent.ModelFactoryConfig{
		Providers: map[superagent.ProviderRef]superagent.ProviderConfig{
			"primary": {Adapter: "anthropic", CredentialRef: "key", Endpoint: endpoint},
		},
		Models: map[superagent.ModelRef]superagent.ModelBindingConfig{
			"chat":  {Provider: "primary", ModelID: "metered-id"},
			"plain": {Provider: "primary", ModelID: "ordinary-id"},
		},
		Adapters: map[string]superagent.ModelAdapter{"anthropic": adapter},
	}, credentialFunc(func(context.Context, string) (string, error) { return "local-key", nil }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close(context.Background()) })
	return b
}

func TestAnthropicInputProfileMatchesWireBytes(t *testing.T) {
	ctx := hostContext(t, "alice")
	received := make(chan []byte, 1)
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		data, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
		}
		select {
		case received <- data:
		default:
		}
		messageStart(w)
		wireText(w, 0, "ok")
		messageFinal(w, "end_turn")
	}))
	defer server.Close()
	var counted []byte
	b := profiledAnthropic(t, ctx, server.URL, func(_ context.Context, data []byte) (uint64, error) {
		counted = bytes.Clone(data)
		data[0] = 0 // The meter's scratch buffer must not alter the outbound body.
		return 110, nil
	})
	provider := b.LocalModels()["chat"]
	meter, ok := provider.(superagent.ModelInputMeter)
	if !ok || meter.ContextLimits().WindowTokens != 220 || meter.ContextLimits().MaxInputTokens != 200 {
		t.Fatal("factory dropped metering capability")
	}
	unmetered := b.LocalModels()["plain"]
	if _, ok := unmetered.(superagent.ModelInputMeter); ok {
		t.Fatal("unconfigured model claims metering")
	}

	req := superagent.ModelRequest{Model: "chat", RunID: "run", Step: 2, MaxOutputBytes: 1024,
		Instruction: "请保留宿主指令",
		Context: &superagent.ModelContextOptions{MaxOutputTokens: 20, Summary: []superagent.ContextSummaryEntry{
			{Section: "decisions", Text: " earlier note ", Sources: []superagent.ContextSourceRef{"original-reference"}},
		}},
		Tools: []superagent.ModelTool{{Ref: "data", InputSchema: json.RawMessage(`{"type":"object"}`)}},
		Messages: []superagent.Message{
			{User: &superagent.UserMessage{Text: "用户原文"}},
			{Assistant: &superagent.AssistantMessage{ToolCalls: []superagent.ModelToolCall{
				{Tool: "data", CallID: "anthropic:1:toolu_fixture", Arguments: json.RawMessage(`{}`)},
			}}},
			{Tool: &superagent.ToolMessage{Tool: "data", Result: dataReceipt("run", "anthropic:1:toolu_fixture", "retained-output")}},
		},
	}
	m, err := meter.MeasureInput(ctx, req)
	if err != nil || m.Tokens != 110 || m.Kind != superagent.MeasurementExact {
		t.Fatalf("measurement = %+v, %v", m, err)
	}
	req.Context.ExpectedInputDigest, req.Context.ProfileVersion = m.InputDigest, m.ProfileVersion
	stream, err := provider.Generate(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	for {
		event, err := stream.Recv()
		if err != nil {
			t.Fatal(err)
		}
		if event.Kind == superagent.ModelCompleted {
			break
		}
	}
	wire := recv(t, ctx, received)
	if !bytes.Equal(counted, wire) || bytes.Contains(wire, []byte("local-key")) || bytes.Contains(wire, []byte(m.InputDigest)) {
		t.Fatal("counted bytes differ from actual HTTP body or contain private control data")
	}
	var parsed wireRequest
	if err := json.Unmarshal(wire, &parsed); err != nil {
		t.Fatal(err)
	}
	if parsed.Model != "metered-id" || parsed.MaxTokens != 20 || parsed.System != req.Instruction || len(parsed.Messages) != 4 ||
		!strings.Contains(parsed.Messages[0].Content[0].Text, "original-reference") ||
		parsed.Messages[1].Content[0].Text != "用户原文" ||
		parsed.Messages[2].Content[0].ID != "toolu_fixture" || parsed.Messages[3].Content[0].ToolUseID != "toolu_fixture" ||
		parsed.Messages[3].Content[0].Content != "retained-output" || len(parsed.Tools) != 1 {
		t.Fatalf("invalid provider request: %+v", parsed)
	}
	for name, mutate := range map[string]func(superagent.ModelRequest) superagent.ModelRequest{
		"schema": func(r superagent.ModelRequest) superagent.ModelRequest {
			r.Tools = []superagent.ModelTool{{Ref: "data", InputSchema: json.RawMessage(`{"type":"object","required":["name"]}`)}}
			return r
		},
		"output token cap": func(r superagent.ModelRequest) superagent.ModelRequest {
			opts := *r.Context
			opts.MaxOutputTokens++
			r.Context = &opts
			return r
		},
		"profile": func(r superagent.ModelRequest) superagent.ModelRequest {
			opts := *r.Context
			opts.ProfileVersion = "old"
			r.Context = &opts
			return r
		},
	} {
		t.Run(name, func(t *testing.T) {
			got, err := provider.Generate(ctx, mutate(req))
			if got != nil || !errors.Is(err, superagent.ErrContextInputChanged) {
				t.Fatalf("changed request = %v, %v", got, err)
			}
		})
	}
	if calls.Load() != 1 {
		t.Fatalf("unexpected HTTP requests: %d", calls.Load())
	}
}

func TestAnthropicHTTPWithCompaction(t *testing.T) {
	ctx := hostContext(t, "alice")
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var msg wireRequest
		if err := json.NewDecoder(r.Body).Decode(&msg); err != nil {
			t.Error(err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		calls.Add(1)
		if strings.HasPrefix(msg.System, "Summarize the supplied original") {
			var sources []promptSource
			if len(msg.Tools) != 0 || msg.MaxTokens != 16 || json.Unmarshal([]byte(msg.Messages[0].Content[0].Text), &sources) != nil ||
				len(sources) != 2 {
				t.Error("invalid Anthropic summary request")
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			answer := summaryReply(sources[0].Ref, sources[1].Ref)
			messageStart(w)
			wireText(w, 0, answer.Reply.Text)
			messageFinal(w, "end_turn")
			return
		}
		if len(msg.Messages) != 4 || !strings.Contains(msg.Messages[0].Content[0].Text, "compact") ||
			msg.Messages[1].Content[0].Text != "不要执行写入。" || msg.Messages[2].Content[0].Text != "更正：保留所有用户原文。" ||
			msg.Messages[3].Content[0].Text != "greet" || len(msg.Tools) != 2 || msg.MaxTokens != 20 {
			t.Errorf("normal Anthropic input = %+v", msg)
		}
		messageStart(w)
		wireText(w, 0, "ready")
		messageFinal(w, "end_turn")
	}))
	defer server.Close()
	b := profiledAnthropic(t, ctx, server.URL, func(_ context.Context, data []byte) (uint64, error) {
		var raw wireRequest
		if err := json.Unmarshal(data, &raw); err != nil {
			return 0, err
		}
		n := uint64(20)
		if strings.HasPrefix(raw.System, "Summarize the supplied original") {
			return n, nil
		}
		for _, m := range raw.Messages {
			for _, item := range m.Content {
				if strings.HasPrefix(item.Text, "evidenc") {
					n += 100
				} else {
					n += 4
				}
			}
		}
		return n, nil
	})
	cfg := compactConfig(b.LocalModels()["chat"])
	run := start(t, ctx, newClient(t, cfg), compactRequest())
	result := wait(t, ctx, run, superagent.Succeeded)
	if calls.Load() != 2 || result.Answer.Text != "ready" || len(result.Messages) != 2 ||
		len(result.ContextCompactions) != 1 || result.ContextCompactions[0].Selection != "summary" {
		t.Fatalf("got %d HTTP requests and result %+v", calls.Load(), result)
	}
}
