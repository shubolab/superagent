//go:build darwin || linux

package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	sdk "github.com/shubolab/superagent"
)

type wireBlock struct {
	Type      string `json:"type"`
	Text      string `json:"text"`
	ID        string `json:"id"`
	ToolUseID string `json:"tool_use_id"`
	Content   string `json:"content"`
	IsError   bool   `json:"is_error"`
}

type wireRequest struct {
	Messages []struct {
		Role    string      `json:"role"`
		Content []wireBlock `json:"content"`
	} `json:"messages"`
	Tools []struct {
		Name string `json:"name"`
	} `json:"tools"`
}

type wireCall struct {
	request wireRequest
	reply   chan wireReply
}

type wireReply struct {
	prefix, suffix string
	flushed        chan struct{}
	proceed        <-chan struct{}
}

type wireModel struct {
	t      *testing.T
	server *httptest.Server
	calls  chan *wireCall
	mu     sync.Mutex
	seen   []wireRequest
}

func newWireModel(t *testing.T) *wireModel {
	m := &wireModel{t: t, calls: make(chan *wireCall, 64)}
	m.server = httptest.NewServer(http.HandlerFunc(m.serve))
	t.Cleanup(m.server.Close)
	return m
}

func (m *wireModel) serve(out http.ResponseWriter, req *http.Request) {
	var input wireRequest
	if req.URL.Path != "/v1/messages" || req.Method != http.MethodPost || json.NewDecoder(req.Body).Decode(&input) != nil {
		http.Error(out, "unexpected Messages request", http.StatusBadRequest)
		return
	}
	m.mu.Lock()
	m.seen = append(m.seen, input)
	m.mu.Unlock()
	call := &wireCall{request: input, reply: make(chan wireReply, 1)}
	m.calls <- call
	select {
	case reply := <-call.reply:
		out.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(out, reply.prefix)
		out.(http.Flusher).Flush()
		if reply.flushed != nil {
			close(reply.flushed)
		}
		if reply.proceed != nil {
			select {
			case <-reply.proceed:
			case <-req.Context().Done():
				return
			}
		}
		fmt.Fprint(out, reply.suffix)
		out.(http.Flusher).Flush()
	case <-req.Context().Done():
	}
}

func (m *wireModel) next(input string) *wireCall {
	m.t.Helper()
	select {
	case call := <-m.calls:
		if got := call.request.lastUser(); got != input {
			m.t.Fatalf("expected model request %q, got %q", input, got)
		}
		return call
	case <-time.After(8 * time.Second):
		m.t.Fatalf("missing Messages request %q", input)
		return nil
	}
}

func (m *wireModel) count(input string, want int) {
	m.t.Helper()
	m.mu.Lock()
	defer m.mu.Unlock()
	got := 0
	for _, call := range m.seen {
		if input == "" || call.lastUser() == input {
			got++
		}
	}
	if got != want {
		m.t.Fatalf("model requests for %q: %d, want %d", input, got, want)
	}
}

func (r wireRequest) lastUser() string {
	for i := len(r.Messages) - 1; i >= 0; i-- {
		if r.Messages[i].Role == "user" {
			for _, b := range r.Messages[i].Content {
				if b.Type == "text" {
					return b.Text
				}
			}
		}
	}
	return ""
}

func (r wireRequest) results() []wireBlock {
	var results []wireBlock
	for _, message := range r.Messages {
		for _, block := range message.Content {
			if block.Type == "tool_result" {
				results = append(results, block)
			}
		}
	}
	return results
}

func (c *wireCall) report(t *testing.T, id string) sdk.SearchReport {
	t.Helper()
	results := c.request.results()
	var result sdk.SearchReport
	if len(results) != 1 || results[0].IsError || results[0].ToolUseID != id || json.Unmarshal([]byte(results[0].Content), &result) != nil {
		t.Fatalf("expected one paired report for %s, got %+v", id, results)
	}
	return result
}

func (c *wireCall) answer(text string) {
	c.reply <- wireReply{prefix: wireStart + wireText(0, text) + wireFinish("end_turn")}
}

func (c *wireCall) search(id, path, query string) {
	c.reply <- wireReply{prefix: wireStart + wireSearch(0, id, path, query) + wireFinish("tool_use")}
}

func (c *wireCall) splitSearch(id, path, query string) (flushed <-chan struct{}, complete func()) {
	done, release := make(chan struct{}), make(chan struct{})
	c.reply <- wireReply{prefix: wireStart + wireText(0, "待完整回复") + wireSearch(1, id, path, query),
		suffix: wireFinish("tool_use"), flushed: done, proceed: release}
	return done, sync.OnceFunc(func() { close(release) })
}

const wireStart = "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"type\":\"message\",\"role\":\"assistant\",\"content\":[]}}\n\n"

func wireEvent(kind string, fields map[string]any) string {
	if fields == nil {
		fields = make(map[string]any)
	}
	fields["type"] = kind
	data, err := json.Marshal(fields)
	if err != nil {
		panic(err)
	}
	return fmt.Sprintf("event: %s\ndata: %s\n\n", kind, data)
}

func wireText(index int, text string) string {
	return wireEvent("content_block_start", map[string]any{"index": index, "content_block": map[string]any{"type": "text", "text": ""}}) +
		wireEvent("content_block_delta", map[string]any{"index": index, "delta": map[string]any{"type": "text_delta", "text": text}}) + wireBlockEnd(index)
}

func wireSearch(index int, id, path, query string) string {
	data, _ := json.Marshal(map[string]string{"path": path, "query": query})
	return wireEvent("content_block_start", map[string]any{"index": index, "content_block": map[string]any{
		"type": "tool_use", "id": id, "name": "search", "input": map[string]any{},
	}}) + wireEvent("content_block_delta", map[string]any{"index": index, "delta": map[string]any{
		"type": "input_json_delta", "partial_json": string(data),
	}}) + wireBlockEnd(index)
}

func wireBlockEnd(index int) string {
	return wireEvent("content_block_stop", map[string]any{"index": index})
}

func wireFinish(reason string) string {
	return wireEvent("message_delta", map[string]any{"delta": map[string]any{"stop_reason": reason}}) + wireEvent("message_stop", nil)
}

func checkTools(t *testing.T, request wireRequest, enabled bool) {
	t.Helper()
	if enabled {
		if len(request.Tools) != 1 || request.Tools[0].Name != "search" {
			t.Fatalf("tool declarations: %+v", request.Tools)
		}
	} else if len(request.Tools) != 0 {
		t.Fatalf("unexpected tools in text mode: %+v", request.Tools)
	}
}

func checkHistory(t *testing.T, request wireRequest, parts ...string) {
	t.Helper()
	var texts []string
	for _, message := range request.Messages {
		for _, block := range message.Content {
			if block.Type == "text" {
				texts = append(texts, block.Text)
			}
		}
	}
	if strings.Join(texts, "|") != strings.Join(parts, "|") {
		t.Fatalf("messages %q, want %q", texts, parts)
	}
}
