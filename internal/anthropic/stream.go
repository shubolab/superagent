package anthropic

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"unicode/utf8"

	"github.com/shubolab/superagent/internal/contract"
)

const maxBlocks = 256

type stream struct {
	reader          *bufio.Reader
	closeBody       func() error
	frameLimit      int
	bytesLeft       uint64
	thinkingLeft    uint64
	discardThinking bool
	step            uint32
	started         bool
	complete        bool
	invalidArgs     bool
	stop            string
	current         *block
	blockIndex      uint32
	text            strings.Builder
	partial         bytes.Buffer
	toolCalls       []contract.ModelToolCall
	ids             map[string]bool
	usage           tokenUsage
}

type frame struct {
	Type    string `json:"type"`
	Message *struct {
		Type    string     `json:"type"`
		Role    string     `json:"role"`
		Content []block    `json:"content"`
		Usage   tokenUsage `json:"usage"`
	} `json:"message"`
	Index *uint32 `json:"index"`
	Block *block  `json:"content_block"`
	Delta *struct {
		Type        string `json:"type"`
		Text        string `json:"text"`
		PartialJSON string `json:"partial_json"`
		StopReason  string `json:"stop_reason"`
		Thinking    string `json:"thinking"`
		Signature   string `json:"signature"`
	} `json:"delta"`
	Usage *tokenUsage `json:"usage"`
	Error *struct {
		Type string `json:"type"`
	} `json:"error"`
}

type tokenUsage struct {
	Input         *uint64 `json:"input_tokens"`
	CacheCreation *uint64 `json:"cache_creation_input_tokens"`
	CacheRead     *uint64 `json:"cache_read_input_tokens"`
	Output        *uint64 `json:"output_tokens"`
}

func newStream(body io.ReadCloser, req contract.ModelRequest, discardThinking bool) *stream {
	frameLimit := int(4096 + 6*min(req.MaxOutputBytes, 42<<10))
	return &stream{
		reader: bufio.NewReaderSize(body, frameLimit), closeBody: sync.OnceValue(body.Close),
		frameLimit: frameLimit, bytesLeft: req.MaxOutputBytes, thinkingLeft: req.MaxOutputBytes, discardThinking: discardThinking, step: req.Step, ids: make(map[string]bool),
	}
}

func (s *stream) Close() error { return s.closeBody() }

func (s *stream) Recv() (contract.ModelEvent, error) {
	if s.complete {
		return contract.ModelEvent{}, io.EOF
	}
	for {
		event, err := s.readFrame()
		if err != nil {
			_ = s.Close()
			return contract.ModelEvent{}, err
		}
		out, err := s.handle(event)
		if err != nil {
			_ = s.Close()
			return contract.ModelEvent{}, err
		}
		if out.Kind != "" {
			return out, nil
		}
	}
}

func (s *stream) readFrame() (frame, error) {
	var data []byte
	var name string
	for {
		line, err := s.reader.ReadSlice('\n')
		if err != nil {
			switch {
			case errors.Is(err, bufio.ErrBufferFull):
				return frame{}, contract.ErrModelOutputLimit
			case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF):
				return frame{}, contract.ErrModelResponseTruncated
			default:
				return frame{}, errClosed
			}
		}
		line = bytes.TrimSuffix(bytes.TrimSuffix(line, []byte{'\n'}), []byte{'\r'})
		if len(line) == 0 {
			if len(data) == 0 {
				name = ""
				continue
			}
			var event frame
			if !utf8.Valid(data) || json.Unmarshal(data, &event) != nil || event.Type == "" || (name != "" && name != event.Type) {
				return frame{}, contract.ErrModelResponseProtocol
			}
			return event, nil
		}
		key, field, _ := bytes.Cut(line, []byte{':'})
		field = bytes.TrimPrefix(field, []byte{' '})
		switch string(key) {
		case "event":
			name = string(field)
		case "data":
			if len(data)+len(field)+1 > s.frameLimit {
				return frame{}, contract.ErrModelOutputLimit
			}
			data = append(data, field...)
			data = append(data, '\n')
		}
	}
}

func (s *stream) handle(event frame) (contract.ModelEvent, error) {
	protocol := contract.ErrModelResponseProtocol
	switch event.Type {
	case "message_start":
		if s.started || event.Message == nil || event.Message.Type != "message" || event.Message.Role != "assistant" ||
			len(event.Message.Content) != 0 {
			return contract.ModelEvent{}, protocol
		}
		s.started, s.usage = true, event.Message.Usage
	case "content_block_start":
		if !s.started || s.current != nil || s.stop != "" || event.Index == nil || event.Block == nil ||
			*event.Index != s.blockIndex || s.blockIndex >= maxBlocks {
			return contract.ModelEvent{}, protocol
		}
		s.current = event.Block
		switch s.current.Type {
		case "text":
			return s.appendText(s.current.Text)
		case "thinking":
			if !s.discardThinking {
				return contract.ModelEvent{}, protocol
			}
			if err := s.consumeThinking(len(s.current.Thinking) + len(s.current.Signature)); err != nil {
				return contract.ModelEvent{}, err
			}
			s.current.Thinking, s.current.Signature = "", ""
		case "tool_use":
			wireID := s.current.ID
			if !providerName(wireID) || !providerName(s.current.Name) || s.ids[wireID] || !contract.JSONObject(s.current.Input) {
				return contract.ModelEvent{}, protocol
			}
			s.ids[wireID] = true
			s.current.ID = fmt.Sprintf("anthropic:%d:%s", s.step, wireID)
			if !s.charge(len(s.current.ID) + len(s.current.Name)) {
				return contract.ModelEvent{}, contract.ErrModelOutputLimit
			}
		default:
			return contract.ModelEvent{}, protocol
		}
	case "content_block_delta":
		if !s.isCurrent(event.Index) || event.Delta == nil {
			return contract.ModelEvent{}, protocol
		}
		switch {
		case s.current.Type == "text" && event.Delta.Type == "text_delta":
			return s.appendText(event.Delta.Text)
		case s.current.Type == "thinking" && event.Delta.Type == "thinking_delta":
			return contract.ModelEvent{}, s.consumeThinking(len(event.Delta.Thinking))
		case s.current.Type == "thinking" && event.Delta.Type == "signature_delta":
			return contract.ModelEvent{}, s.consumeThinking(len(event.Delta.Signature))
		case s.current.Type == "tool_use" && event.Delta.Type == "input_json_delta":
			if !s.charge(len(event.Delta.PartialJSON)) {
				return contract.ModelEvent{}, contract.ErrModelOutputLimit
			}
			s.partial.WriteString(event.Delta.PartialJSON)
		default:
			return contract.ModelEvent{}, protocol
		}
	case "content_block_stop":
		if !s.isCurrent(event.Index) {
			return contract.ModelEvent{}, protocol
		}
		if s.current.Type == "tool_use" {
			raw := json.RawMessage(s.partial.Bytes())
			if len(raw) == 0 {
				raw = s.current.Input
				if !s.charge(len(raw)) {
					return contract.ModelEvent{}, contract.ErrModelOutputLimit
				}
			}
			if !contract.JSONObject(raw) {
				s.invalidArgs = true
			}
			s.toolCalls = append(s.toolCalls, contract.ModelToolCall{
				CallID: contract.CallID(s.current.ID), Tool: contract.ToolRef(s.current.Name), Arguments: bytes.Clone(raw),
			})
			s.partial.Reset()
		}
		s.current = nil
		s.blockIndex++
	case "message_delta":
		if !s.started || s.current != nil || event.Delta == nil {
			return contract.ModelEvent{}, protocol
		}
		if event.Delta.StopReason != "" {
			if s.stop != "" {
				return contract.ModelEvent{}, protocol
			}
			s.stop = event.Delta.StopReason
		}
		if event.Usage != nil {
			s.usage.update(*event.Usage)
		}
	case "message_stop":
		return s.finish()
	case "error":
		if event.Error == nil {
			return contract.ModelEvent{}, protocol
		}
		switch event.Error.Type {
		case "authentication_error", "permission_error":
			return contract.ModelEvent{}, contract.ErrModelAuthFailed
		case "not_found_error":
			return contract.ModelEvent{}, contract.ErrModelNotFound
		default:
			return contract.ModelEvent{}, errRequest
		}
	}
	return contract.ModelEvent{}, nil
}

func (s *stream) isCurrent(index *uint32) bool {
	return s.current != nil && index != nil && *index == s.blockIndex
}

func (s *stream) finish() (contract.ModelEvent, error) {
	if s.current != nil || !s.started {
		return contract.ModelEvent{}, contract.ErrModelResponseProtocol
	}
	var finish contract.ModelFinishReason
	switch {
	case s.stop == "max_tokens":
		return contract.ModelEvent{}, contract.ErrModelResponseTruncated
	case s.invalidArgs:
		return contract.ModelEvent{}, contract.ErrModelResponseProtocol
	case s.stop == "end_turn" && len(s.toolCalls) == 0:
		finish = contract.ModelFinishStop
	case s.stop == "tool_use" && len(s.toolCalls) != 0:
		finish = contract.ModelFinishToolCalls
	default:
		return contract.ModelEvent{}, contract.ErrModelResponseProtocol
	}
	s.complete = true
	return contract.ModelEvent{Kind: contract.ModelCompleted, Reply: &contract.ModelReply{
		Finish: finish, Text: s.text.String(), ToolCalls: s.toolCalls, Usage: s.usage.total(),
	}}, nil
}

func (s *stream) appendText(text string) (contract.ModelEvent, error) {
	if text == "" {
		return contract.ModelEvent{}, nil
	}
	if !s.charge(len(text)) {
		return contract.ModelEvent{}, contract.ErrModelOutputLimit
	}
	s.text.WriteString(text)
	return contract.ModelEvent{Kind: contract.ModelTextDelta, TextDelta: text}, nil
}

func (s *stream) consumeThinking(n int) error {
	if uint64(n) > s.thinkingLeft {
		return contract.ErrModelOutputLimit
	}
	s.thinkingLeft -= uint64(n)
	return nil
}

func (s *stream) charge(n int) bool {
	if uint64(n) > s.bytesLeft {
		return false
	}
	s.bytesLeft -= uint64(n)
	return true
}

func (u *tokenUsage) update(next tokenUsage) {
	if next.Input != nil {
		u.Input = next.Input
	}
	if next.CacheCreation != nil {
		u.CacheCreation = next.CacheCreation
	}
	if next.CacheRead != nil {
		u.CacheRead = next.CacheRead
	}
	if next.Output != nil {
		u.Output = next.Output
	}
}

func (u tokenUsage) total() *contract.Usage {
	if u.Input == nil && u.CacheCreation == nil && u.CacheRead == nil && u.Output == nil {
		return nil
	}
	usage := &contract.Usage{}
	for _, value := range []*uint64{u.Input, u.CacheCreation, u.CacheRead} {
		if value != nil {
			usage.InputTokens += min(*value, ^uint64(0)-usage.InputTokens)
		}
	}
	if u.Output != nil {
		usage.OutputTokens = *u.Output
	}
	return usage
}
