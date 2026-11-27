package search

import (
	"context"
	"encoding/json"
	"io"
	"strings"
	"testing"

	"github.com/shubolab/superagent/internal/contract"
)

type countedReader struct {
	io.Reader
	bytes int
	chunk int
}

func (c *countedReader) Read(p []byte) (int, error) {
	c.chunk = max(c.chunk, len(p))
	n, err := c.Reader.Read(p)
	c.bytes += n
	return n, err
}

func TestScanResourceBounds(t *testing.T) {
	for _, tc := range []struct {
		name, input, coverage, reason string
		bytes, results                uint64
		hits                          int
		wantBytes                     uint64
	}{
		{"fixed", "status=ready\n", "complete", "", 32, 4096, 1, 13},
		{"complete zero", "idle", "complete", "", 32, 4096, 0, 4},
		{"EOF line", "status=ready", "complete", "", 32, 4096, 1, 12},
		{"scan limited", strings.Repeat("idle\n", 20_000), "partial", "scan_limit", 32, 4096, 0, 32},
		{"no EOF evidence at limit", "status", "partial", "scan_limit", 6, 4096, 0, 6},
		{"large line", strings.Repeat("A", 1<<20), "partial", "line_limit", 1 << 20, 4096, 0, maxLineBytes + chunkBytes},
		{"match dense", strings.Repeat("status\n", 50000), "partial", "match_limit", 1 << 20, 16000, maxMatches, chunkBytes},
		{"preview limit", "status" + strings.Repeat("A", 400), "partial", "preview_limit", 1024, 4096, 1, 406},
		{"envelope limit", strings.Repeat("status\n", 30), "partial", "result_limit", 1024, 330, -1, 210},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := &countedReader{Reader: strings.NewReader(tc.input)}
			ctx := contract.WithFileOpener(t.Context(), func(context.Context, contract.ToolRequest, string) (io.ReadCloser, error) {
				return io.NopCloser(source), nil
			})
			req := contract.ToolRequest{RunID: "run", CallID: "c1", Tool: "search",
				Arguments: json.RawMessage(`{"path":"docs/health.txt","query":"status"}`),
				Limits:    contract.ToolLimits{MaxScanBytes: tc.bytes, MaxResultBytes: tc.results}}
			result, err := New().Tool.Invoke(ctx, req)
			if err != nil || result.Completion == nil || result.Completion.Outcome != contract.ToolSucceeded {
				t.Fatalf("result = %+v, %v", result, err)
			}
			var report Report
			if err := json.Unmarshal([]byte(result.Completion.Content), &report); err != nil {
				t.Fatal(err)
			}
			encoded, err := json.Marshal(result)
			if err != nil || uint64(len(encoded)) > tc.results || source.chunk > chunkBytes ||
				uint64(source.bytes) != tc.wantBytes || report.ScannedBytes != tc.wantBytes || *result.Usage.ScannedBytes != tc.wantBytes ||
				report.Coverage != tc.coverage || report.Reason != tc.reason || (tc.hits >= 0 && len(report.Matches) != tc.hits) {
				t.Fatalf("report = %+v, result bytes = %d, read = %d, chunk = %d", report, len(encoded), source.bytes, source.chunk)
			}
			for _, m := range report.Matches {
				if len(m.Preview) > previewBytes || m.Line == 0 {
					t.Fatalf("invalid match: %+v", m)
				}
			}
		})
	}
}
