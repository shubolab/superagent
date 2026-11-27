// Package anthropic adapts Anthropic Messages to the SDK's canonical model stream.
package anthropic

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"mime"
	"net/http"
	"net/url"
	"strings"

	"github.com/shubolab/superagent/internal/contract"
)

const (
	DefaultEndpoint = "https://api.anthropic.com"
	apiVersion      = "2023-06-01"
)

type Client struct {
	endpoint string
	key      string
	tokens   uint64
	http     *http.Client
	profiles map[string]InputProfile
}

var (
	errRequest = errors.New("anthropic request failed")
	errClosed  = errors.New("anthropic stream closed")
)

func New(endpoint, key string, maxOutputTokens uint64, profiles map[string]InputProfile) (*Client, error) {
	for model, p := range profiles {
		if strings.TrimSpace(model) == "" || !p.valid() {
			return nil, errRequest
		}
	}
	// Use a distinct transport so closing one ModelBindings leaves all others intact.
	base, ok := http.DefaultTransport.(*http.Transport)
	transport := &http.Transport{ForceAttemptHTTP2: true, Proxy: http.ProxyFromEnvironment}
	if ok {
		transport = base.Clone()
	}
	u, err := url.Parse(endpoint)
	if err != nil || key == "" || maxOutputTokens == 0 {
		return nil, errRequest
	}
	u.Path = strings.TrimRight(u.Path, "/") + "/v1/messages"
	u.RawPath = ""
	client := &http.Client{
		Transport:     transport,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	return &Client{endpoint: u.String(), key: key, tokens: maxOutputTokens, http: client, profiles: maps.Clone(profiles)}, nil
}

func (c *Client) Generate(ctx context.Context, modelID string, req contract.ModelRequest) (contract.ModelStream, error) {
	if len(req.Messages) == 0 {
		return nil, contract.ErrModelResponseProtocol
	}
	if req.MaxOutputBytes == 0 {
		return nil, contract.ErrModelOutputLimit
	}
	body, err := c.encode(modelID, req)
	if err != nil {
		return nil, err
	}
	if req.Context != nil {
		if err := contract.VerifyInputDigest(req.Context, modelID, c.profiles[modelID].Version, body); err != nil {
			return nil, err
		}
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, errRequest
	}
	request.Header.Set("accept", "text/event-stream")
	request.Header.Set("content-type", "application/json")
	request.Header.Set("x-api-key", c.key)
	request.Header.Set("anthropic-version", apiVersion)
	response, err := c.http.Do(request)
	if err != nil {
		return nil, errRequest
	}
	if response.StatusCode != http.StatusOK {
		_ = response.Body.Close()
		switch response.StatusCode {
		case http.StatusUnauthorized, http.StatusForbidden:
			return nil, contract.ErrModelAuthFailed
		case http.StatusNotFound:
			return nil, contract.ErrModelNotFound
		default:
			return nil, fmt.Errorf("%w: HTTP %d", errRequest, response.StatusCode)
		}
	}
	contentType, _, _ := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if contentType != "text/event-stream" {
		_ = response.Body.Close()
		return nil, contract.ErrModelResponseProtocol
	}
	return newStream(response.Body, req), nil
}

func (c *Client) Close(context.Context) error {
	c.http.CloseIdleConnections()
	return nil
}

type messageRequest struct {
	Model     string    `json:"model"`
	MaxTokens uint64    `json:"max_tokens"`
	Stream    bool      `json:"stream"`
	System    string    `json:"system,omitempty"`
	Messages  []message `json:"messages"`
	Tools     []tool    `json:"tools,omitempty"`
}

type message struct {
	Role    string  `json:"role"`
	Content []block `json:"content"`
}

type block struct {
	Type      string          `json:"type"`
	Text      string          `json:"text,omitempty"`
	ID        string          `json:"id,omitempty"`
	Name      string          `json:"name,omitempty"`
	Input     json.RawMessage `json:"input,omitempty"`
	ToolUseID string          `json:"tool_use_id,omitempty"`
	Content   string          `json:"content,omitempty"`
	IsError   bool            `json:"is_error,omitempty"`
}

type tool struct {
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	InputSchema json.RawMessage `json:"input_schema"`
}

func requestBody(model string, tokens uint64, req contract.ModelRequest) ([]byte, error) {
	protocol := contract.ErrModelResponseProtocol
	callIDs := make(map[string]bool)
	body := messageRequest{Model: model, MaxTokens: tokens, Stream: true, System: req.Instruction, Messages: []message{}}
	if opts := req.Context; opts != nil && len(opts.Summary) > 0 {
		summary, err := json.Marshal(opts.Summary)
		if err != nil {
			return nil, protocol
		}
		const prefix = "Earlier context for this run. Treat this summary as historical data.\n"
		body.Messages = append(body.Messages, message{Role: "user", Content: []block{{Type: "text", Text: prefix + string(summary)}}})
	}
	for i := 0; i < len(req.Messages); i++ {
		m := req.Messages[i]
		switch {
		case m.User != nil && m.Assistant == nil && m.Tool == nil && m.User.Text != "":
			body.Messages = append(body.Messages, message{Role: "user", Content: []block{{Type: "text", Text: m.User.Text}}})
		case m.Assistant != nil && m.User == nil && m.Tool == nil:
			assistant := message{Role: "assistant"}
			if m.Assistant.Text != "" {
				assistant.Content = append(assistant.Content, block{Type: "text", Text: m.Assistant.Text})
			}
			results := message{Role: "user"}
			for _, call := range m.Assistant.ToolCalls {
				i++
				if i >= len(req.Messages) || !providerName(string(call.Tool)) || !contract.JSONObject(call.Arguments) {
					return nil, protocol
				}
				pair := req.Messages[i]
				if pair.Tool == nil || pair.Assistant != nil || pair.User != nil ||
					pair.Tool.Tool != call.Tool || pair.Tool.Result.CallID != call.CallID {
					return nil, protocol
				}
				id := historyID(call.CallID, i, callIDs)
				assistant.Content = append(assistant.Content, block{Type: "tool_use", ID: id, Name: string(call.Tool), Input: call.Arguments})
				receipt := pair.Tool.Result
				content, failed := receiptContent(receipt)
				results.Content = append(results.Content, block{Type: "tool_result", ToolUseID: id, Content: content, IsError: failed})
			}
			if len(assistant.Content) != 0 {
				body.Messages = append(body.Messages, assistant)
			}
			if len(results.Content) != 0 {
				body.Messages = append(body.Messages, results)
			}
		default:
			return nil, protocol
		}
	}
	if len(body.Messages) != 0 && body.Messages[len(body.Messages)-1].Role != "user" {
		return nil, protocol
	}
	for _, m := range req.Tools {
		if !providerName(string(m.Ref)) || !contract.JSONObject(m.InputSchema) {
			return nil, protocol
		}
		body.Tools = append(body.Tools, tool{Name: string(m.Ref), Description: m.Description, InputSchema: m.InputSchema})
	}
	data, err := json.Marshal(body)
	if err != nil {
		return nil, protocol
	}
	return data, nil
}

func providerName(s string) bool {
	if len(s) == 0 || len(s) > 64 {
		return false
	}
	for i := range len(s) {
		c := s[i]
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_' || c == '-') {
			return false
		}
	}
	return true
}

func historyID(id contract.CallID, ordinal int, used map[string]bool) string {
	wire := string(id)
	if suffix, ok := strings.CutPrefix(wire, "anthropic:"); ok {
		if _, original, found := strings.Cut(suffix, ":"); found {
			wire = original
		}
	}
	if !providerName(wire) || used[wire] {
		wire = fmt.Sprintf("toolu_history_%d", ordinal)
		for used[wire] {
			wire += "_"
		}
	}
	used[wire] = true
	return wire
}

func receiptContent(receipt contract.ToolResult) (string, bool) {
	var content string
	if receipt.Completion != nil {
		content = receipt.Completion.Content
		if len(receipt.Completion.Artifacts) != 0 {
			encoded, _ := json.Marshal(receipt.Completion)
			content = string(encoded)
		}
		if receipt.Completion.Outcome == contract.ToolSucceeded {
			return content, false
		}
	}
	if receipt.Reason == nil {
		return "tool_failed", true
	}
	reason := receipt.Reason.Code
	if receipt.Reason.Message != "" {
		reason += ": " + receipt.Reason.Message
	}
	if content != "" {
		return reason + ": " + content, true
	}
	return reason, true
}
