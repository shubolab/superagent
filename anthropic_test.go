//go:build darwin || linux

package superagent_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/shubolab/superagent"
)

type wireBlock struct {
	Type      string          `json:"type"`
	Text      string          `json:"text"`
	ID        string          `json:"id"`
	Name      string          `json:"name"`
	Input     json.RawMessage `json:"input"`
	ToolUseID string          `json:"tool_use_id"`
	Content   string          `json:"content"`
	IsError   bool            `json:"is_error"`
}

type wireRequest struct {
	Model     string `json:"model"`
	MaxTokens uint64 `json:"max_tokens"`
	Stream    bool   `json:"stream"`
	System    string `json:"system"`
	Messages  []struct {
		Role    string      `json:"role"`
		Content []wireBlock `json:"content"`
	} `json:"messages"`
	Tools []json.RawMessage `json:"tools"`
}

func factoryAnthropic(t *testing.T, ctx context.Context, endpoint, id, key string) *superagent.ModelBindings {
	t.Helper()
	b, err := superagent.NewModelBindings(ctx, superagent.ModelFactoryConfig{
		Providers: map[superagent.ProviderRef]superagent.ProviderConfig{"claude": {
			Adapter: "anthropic", Endpoint: endpoint, CredentialRef: "claude-key",
		}},
		Models:   map[superagent.ModelRef]superagent.ModelBindingConfig{"chat": {Provider: "claude", ModelID: id}},
		Adapters: map[string]superagent.ModelAdapter{"anthropic": superagent.NewAnthropicMessagesAdapter(256)},
	}, credentialFunc(func(_ context.Context, ref string) (string, error) {
		if ref != "claude-key" {
			return "", fmt.Errorf("unexpected credential reference")
		}
		return key, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close(context.Background()) })
	return b
}

func sse(w http.ResponseWriter, event, fields string) {
	fmt.Fprintf(w, "event: %s\ndata: {\"type\":%q%s}\n\n", event, event, fields)
}

func messageStart(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "text/event-stream")
	sse(w, "message_start", `,"message":{"type":"message","role":"assistant","content":[],"usage":{"input_tokens":5,"cache_read_input_tokens":3}}`)
}

func wireText(w http.ResponseWriter, index int, text string) {
	sse(w, "content_block_start", fmt.Sprintf(`,"index":%d,"content_block":{"type":"text","text":""}`, index))
	sse(w, "content_block_delta", fmt.Sprintf(`,"index":%d,"delta":{"type":"text_delta","text":%q}`, index, text))
	sse(w, "content_block_stop", fmt.Sprintf(`,"index":%d`, index))
}

func wireTool(w http.ResponseWriter, fragments ...string) {
	sse(w, "content_block_start", `,"index":1,"content_block":{"type":"tool_use","id":"toolu_fixture","name":"search","input":{}}`)
	for _, fragment := range fragments {
		data, _ := json.Marshal(fragment)
		sse(w, "content_block_delta", `,"index":1,"delta":{"type":"input_json_delta","partial_json":`+string(data)+`}`)
	}
	sse(w, "content_block_stop", `,"index":1`)
}

func messageFinal(w http.ResponseWriter, reason string) {
	sse(w, "message_delta", fmt.Sprintf(`,"delta":{"stop_reason":%q},"usage":{"output_tokens":11}`, reason))
	sse(w, "message_stop", "")
}

func TestAnthropicHTTPToolRoundTrip(t *testing.T) {
	ctx := hostContext(t, "alice")
	f := newSearchFixture(t)
	allowFinish := make(chan struct{})
	var requests atomic.Int32
	firstRequest, secondRequest := make(chan wireRequest, 1), make(chan wireRequest, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/messages" ||
			r.Header.Get("x-api-key") != "test-key" || r.Header.Get("anthropic-version") != "2023-06-01" {
			t.Error("wrong Messages route, method or authentication headers")
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		var body wireRequest
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		switch requests.Add(1) {
		case 1:
			firstRequest <- body
			messageStart(w)
			wireText(w, 0, "查找")
			wireTool(w, `{"path":"docs/`, `health.txt","query":"status"}`)
			sse(w, "message_delta", `,"delta":{"stop_reason":"tool_use"},"usage":{"output_tokens":11}`)
			w.(http.Flusher).Flush()
			select {
			case <-allowFinish:
				sse(w, "message_stop", "")
			case <-r.Context().Done():
			}
		case 2:
			secondRequest <- body
			messageStart(w)
			wireText(w, 0, "服务状态为 ready。")
			messageFinal(w, "end_turn")
		default:
			t.Error("unexpected repeated supplier request")
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer server.Close()
	defer func() {
		select {
		case <-allowFinish:
		default:
			close(allowFinish)
		}
	}()
	f.cfg.LocalModels = factoryAnthropic(t, ctx, server.URL, "fixture-model", "test-key").LocalModels()
	run := start(t, ctx, newClient(t, f.cfg), f.request())
	defer run.Cancel(ctx)
	stream := observeRun(t, ctx, run)
	first := recv(t, ctx, firstRequest)
	if first.Model != "fixture-model" || !first.Stream || first.MaxTokens != 256 || first.System != "answer with evidence" ||
		len(first.Messages) != 1 || first.Messages[0].Role != "user" || len(first.Tools) != 1 {
		t.Fatalf("first request = %+v", first)
	}
	for _, kind := range []superagent.EventKind{superagent.EventRunStarted, superagent.EventTextDelta} {
		event, err := stream.Recv()
		if err != nil || event.Kind != kind {
			t.Fatalf("event = %+v, %v", event, err)
		}
	}
	if f.hits.Load() != 0 || requests.Load() != 1 {
		t.Fatal("execution preceded message_stop")
	}
	close(allowFinish)
	second := recv(t, ctx, secondRequest)
	if len(second.Messages) != 3 || second.Messages[1].Role != "assistant" || second.Messages[2].Role != "user" ||
		len(second.Messages[1].Content) != 2 || len(second.Messages[2].Content) != 1 || !reflect.DeepEqual(first.Tools, second.Tools) {
		t.Fatalf("second request = %+v", second)
	}
	tool, result := second.Messages[1].Content[1], second.Messages[2].Content[0]
	if tool.Type != "tool_use" || tool.ID != "toolu_fixture" || tool.Name != "search" || result.ToolUseID != tool.ID ||
		result.Type != "tool_result" || result.IsError || !strings.Contains(result.Content, "status=ready") {
		t.Fatalf("tool %s result %+v", tool.ID, result)
	}
	res := wait(t, ctx, run, superagent.Succeeded)
	if res.Answer.Text != "服务状态为 ready。" || len(res.Messages) != 4 ||
		res.Messages[2].Tool.Result.CallID != "anthropic:1:toolu_fixture" || f.hits.Load() != 1 ||
		f.env.scanned.Load() != 13 || res.Usage == nil || res.Usage.InputTokens != 16 || res.Usage.OutputTokens != 22 {
		t.Fatalf("result = %+v", res)
	}
	events := allRunEvents(t, stream)
	if len(events) != 6 || events[2].Kind != superagent.EventToolCompleted || events[len(events)-1].Outcome != superagent.Succeeded {
		t.Fatalf("events = %+v", events)
	}
}

func TestAnthropicHTTPIncompleteOrRejected(t *testing.T) {
	for _, tc := range []struct {
		name    string
		reply   func(http.ResponseWriter)
		outcome superagent.RunOutcome
		stop    superagent.StopCode
	}{
		{"end missing", func(w http.ResponseWriter) {
			messageStart(w)
			wireText(w, 0, "working")
			wireTool(w, "{}")
		}, superagent.Failed, superagent.StopModelTruncated},
		{"invalid tool JSON", func(w http.ResponseWriter) {
			messageStart(w)
			wireText(w, 0, "working")
			wireTool(w, "{")
			messageFinal(w, "tool_use")
		}, superagent.Failed, superagent.StopModelProtocol},
		{"token truncation", func(w http.ResponseWriter) {
			messageStart(w)
			wireText(w, 0, "working")
			wireTool(w, "{")
			messageFinal(w, "max_tokens")
		}, superagent.Failed, superagent.StopModelTruncated},
		{"byte limit", func(w http.ResponseWriter) {
			messageStart(w)
			wireText(w, 0, strings.Repeat("a", 4097))
		}, superagent.BudgetExceeded, superagent.StopModelOutputLimit},
		{"frame limit", func(w http.ResponseWriter) {
			messageStart(w)
			sse(w, "ping", `,"padding":"`+strings.Repeat("a", 100<<10)+`"`)
		}, superagent.BudgetExceeded, superagent.StopModelOutputLimit},
		{"authentication", func(w http.ResponseWriter) {
			http.Error(w, "private provider message", http.StatusUnauthorized)
		}, superagent.Failed, superagent.StopModelAuthFailed},
		{"missing model", func(w http.ResponseWriter) {
			messageStart(w)
			sse(w, "error", `,"error":{"type":"not_found_error","message":"private provider message"}`)
		}, superagent.Failed, superagent.StopModelNotFound},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := hostContext(t, "alice")
			f := newSearchFixture(t)
			var requests atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				tc.reply(w)
			}))
			defer server.Close()
			f.cfg.LocalModels = factoryAnthropic(t, ctx, server.URL, "fixture-model", "test-key").LocalModels()
			r := start(t, ctx, newClient(t, f.cfg), f.request())
			res := wait(t, ctx, r, tc.outcome)
			if res.Stop == nil || res.Stop.Code != tc.stop || len(res.Messages) != 1 ||
				requests.Load() != 1 || f.hits.Load() != 0 {
				t.Fatalf("stop = %+v, attempts %d, tools %d, messages %d", res.Stop, requests.Load(), f.hits.Load(), len(res.Messages))
			}
		})
	}
}

func TestAnthropicHTTPCancelClosesStream(t *testing.T) {
	ctx := hostContext(t, "alice")
	f := newSearchFixture(t)
	serverDone := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		messageStart(w)
		wireText(w, 0, "draft")
		w.(http.Flusher).Flush()
		<-r.Context().Done()
		close(serverDone)
	}))
	defer server.Close()
	f.cfg.LocalModels = factoryAnthropic(t, ctx, server.URL, "fixture-model", "test-key").LocalModels()
	r := start(t, ctx, newClient(t, f.cfg), f.request())
	defer r.Cancel(ctx)
	stream := observeRun(t, ctx, r)
	defer stream.Close()
	for _, kind := range []superagent.EventKind{superagent.EventRunStarted, superagent.EventTextDelta} {
		if event, err := stream.Recv(); err != nil || event.Kind != kind {
			t.Fatalf("event = %+v, %v", event, err)
		}
	}
	if err := r.Cancel(ctx); err != nil {
		t.Fatal(err)
	}
	wait(t, ctx, r, superagent.Cancelled)
	recv(t, ctx, serverDone)
	if f.hits.Load() != 0 {
		t.Fatal("cancelled partial stream executed a tool")
	}
}

func TestAnthropicLiveSearch(t *testing.T) {
	if os.Getenv("SUPERAGENT_ANTHROPIC_LIVE") != "1" {
		t.Skip("set SUPERAGENT_ANTHROPIC_LIVE=1 to run the provider smoke test")
	}
	key, id := os.Getenv("ANTHROPIC_API_KEY"), os.Getenv("SUPERAGENT_ANTHROPIC_MODEL")
	if key == "" || id == "" {
		t.Fatal("configure ANTHROPIC_API_KEY and SUPERAGENT_ANTHROPIC_MODEL")
	}
	ctx, cancel := context.WithTimeout(context.WithValue(t.Context(), callerKey{}, superagent.SubjectID("alice")), 60*time.Second)
	defer cancel()
	f := newSearchFixture(t)
	rc := f.cfg.LocalRunners["main"]
	rc.DefaultBudget.Timeout = pointer(45 * time.Second)
	rc.BudgetCaps.Timeout = rc.DefaultBudget.Timeout
	f.cfg.LocalRunners["main"] = rc
	b := factoryAnthropic(t, ctx, "", id, key)
	f.cfg.LocalModels = b.LocalModels()
	req := f.request()
	req.Instruction = "Call search exactly once for path docs/health.txt and query status. After the result, report the status in one sentence."
	req.Input.Text = "Look up the service status from the file."
	run := start(t, ctx, newClient(t, f.cfg), req)
	res := wait(t, ctx, run, superagent.Succeeded)
	if len(res.Messages) != 4 || f.hits.Load() != 1 || f.env.scanned.Load() != 13 ||
		!strings.Contains(strings.ToLower(res.Answer.Text), "ready") {
		t.Fatalf("unexpected live provider output: %s, tool runs %d", res.Outcome, f.hits.Load())
	}
}
