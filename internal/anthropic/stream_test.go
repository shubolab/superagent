package anthropic

import (
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/shubolab/superagent/internal/contract"
)

func TestThinkingBlocksAreOptInAndBounded(t *testing.T) {
	start := sseFrame("message_start", `,"message":{"type":"message","role":"assistant","content":[]}`)
	block := sseFrame("content_block_start", `,"index":0,"content_block":{"type":"thinking","thinking":""}`)
	thinking := sseFrame("content_block_delta", `,"index":0,"delta":{"type":"thinking_delta","thinking":"private"}`)
	signature := sseFrame("content_block_delta", `,"index":0,"delta":{"type":"signature_delta","signature":"proof"}`)
	closeBlock := sseFrame("content_block_stop", `,"index":0`)
	finish := sseFrame("message_delta", `,"delta":{"stop_reason":"end_turn"}`) + sseFrame("message_stop", "")
	text := sseFrame("content_block_start", `,"index":1,"content_block":{"type":"text","text":"ready"}`) +
		sseFrame("content_block_stop", `,"index":1`)
	for _, tc := range []struct {
		name, frames string
		allow        bool
		limit        uint64
		wantErr      error
	}{
		{"option disabled", start + block + closeBlock + finish, false, 128, contract.ErrModelResponseProtocol},
		{"wrong index", start + block + sseFrame("content_block_delta", `,"index":2,"delta":{"type":"thinking_delta","thinking":"private"}`), true, 128, contract.ErrModelResponseProtocol},
		{"incomplete block", start + block + finish, true, 128, contract.ErrModelResponseProtocol},
		{"truncated frame", start + block + thinking, true, 128, contract.ErrModelResponseTruncated},
		{"cumulative private output", start + block + thinking + signature + closeBlock + finish, true, 11, contract.ErrModelOutputLimit},
		{"independent exact limit", start + block + thinking + signature + closeBlock + text + finish, true, 12, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stream := newStream(io.NopCloser(strings.NewReader(tc.frames)), contract.ModelRequest{MaxOutputBytes: tc.limit}, tc.allow)
			defer stream.Close()
			event, err := stream.Recv()
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) || event.Kind != "" {
					t.Fatalf("event kind %q, error %v, want %v", event.Kind, err, tc.wantErr)
				}
				return
			}
			if err != nil || event.Kind != contract.ModelTextDelta || event.TextDelta != "ready" {
				t.Fatalf("thinking leaked into first event: %+v, %v", event, err)
			}
			event, err = stream.Recv()
			if err != nil || event.Kind != contract.ModelCompleted || event.Reply.Text != "ready" {
				t.Fatalf("wrong accepted reply: %+v, %v", event, err)
			}
		})
	}
}

func sseFrame(name, fields string) string {
	return fmt.Sprintf("event: %s\ndata: {\"type\":%q%s}\n\n", name, name, fields)
}
