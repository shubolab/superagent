// Package search implements bounded literal search over a granted workspace file.
package search

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"

	"github.com/shubolab/superagent/internal/contract"
)

const (
	maxQueryBytes = 256
	chunkBytes    = 4096
	maxLineBytes  = 16 * 1024
	maxMatches    = 50
	previewBytes  = 256
)

type binding struct{}

type arguments struct {
	Path  string `json:"path"`
	Query string `json:"query"`
}

type Report struct {
	Coverage     string  `json:"coverage"`
	ScannedBytes uint64  `json:"scannedBytes"`
	Matches      []Match `json:"matches"`
	Reason       string  `json:"reason,omitempty"`
}

type Match struct {
	Line    uint64 `json:"line"`
	Preview string `json:"preview"`
}

func New() contract.LocalToolBinding {
	return contract.LocalToolBinding{
		Spec: contract.ToolSpec{
			Description:     "Search one authorized file for a case-sensitive literal.",
			InputSchema:     json.RawMessage(`{"type":"object","properties":{"path":{"type":"string"},"query":{"type":"string","maxLength":256}},"required":["path","query"],"additionalProperties":false}`),
			NeedsReadAccess: true,
			Limits:          contract.ToolLimits{MaxScanBytes: 1 << 20, MaxResultBytes: 16 << 10},
		},
		Tool: binding{},
		Prepare: func(raw json.RawMessage) (string, error) {
			args, err := parse(raw)
			return args.Path, err
		},
	}
}

func parse(raw json.RawMessage) (arguments, error) {
	var args arguments
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if !contract.JSONObject(raw) || dec.Decode(&args) != nil || args.Path == "" ||
		len(args.Query) == 0 || len(args.Query) > maxQueryBytes || strings.ContainsAny(args.Query, "\r\n") {
		return arguments{}, contract.ErrInvalidRequest
	}
	return args, nil
}

func (binding) Invoke(ctx context.Context, req contract.ToolRequest) (contract.ToolResult, error) {
	result := contract.ToolResult{RunID: req.RunID, CallID: req.CallID}
	args, err := parse(req.Arguments)
	if err != nil {
		return skipped(result, "invalid_arguments"), nil
	}
	reader, err := contract.OpenToolFile(ctx, req, args.Path)
	if err != nil {
		return skipped(result, "read_not_authorized"), nil
	}
	defer reader.Close()
	input := &meteredInput{Source: reader, Remaining: req.Limits.MaxScanBytes}
	report, err := scan(ctx, input, args.Query)
	result.State = contract.ToolExecutionKnown
	result.Usage = &contract.ToolUsage{ScannedBytes: &report.ScannedBytes}
	result.Completion = &contract.ToolCompletion{Outcome: contract.ToolSucceeded}
	if err != nil {
		result.Completion.Outcome = contract.ToolFailed
		result.Reason = &contract.ToolReason{Code: "read_failed"}
		if ctx.Err() != nil {
			result.Completion.Outcome = contract.ToolCancelled
			result.Reason.Code = "cancelled"
		}
		return result, nil
	}
	for {
		content, err := json.Marshal(report)
		if err != nil {
			return contract.ToolResult{}, err
		}
		result.Completion.Content = string(content)
		encoded, _ := json.Marshal(result)
		if uint64(len(encoded)) <= req.Limits.MaxResultBytes {
			return result, nil
		}
		if len(report.Matches) == 0 {
			result.Completion = &contract.ToolCompletion{Outcome: contract.ToolFailed}
			result.Reason = &contract.ToolReason{Code: "result_limit"}
			return result, nil
		}
		report.Coverage, report.Reason = "partial", "result_limit"
		report.Matches = report.Matches[:len(report.Matches)-1]
	}
}

func skipped(result contract.ToolResult, code string) contract.ToolResult {
	zero := uint64(0)
	result.State = contract.ToolExecutionNotExecuted
	result.Reason = &contract.ToolReason{Code: code}
	result.Usage = &contract.ToolUsage{ScannedBytes: &zero}
	return result
}

type meteredInput struct {
	Source    io.Reader
	Remaining uint64
	Scanned   uint64
}

func (r *meteredInput) Read(p []byte) (int, error) {
	if r.Remaining == 0 {
		return 0, contract.ErrReadLimit
	}
	n, err := r.Source.Read(p[:min(r.Remaining, uint64(len(p)))])
	r.Remaining -= uint64(n)
	r.Scanned += uint64(n)
	return n, err
}

func scan(ctx context.Context, input *meteredInput, query string) (Report, error) {
	reader := bufio.NewReaderSize(input, chunkBytes)
	report := Report{Coverage: "partial", Matches: []Match{}}
	line := make([]byte, 0, maxLineBytes)
	lineNumber := uint64(1)
	needle := []byte(query)
	for {
		if err := ctx.Err(); err != nil {
			report.ScannedBytes = input.Scanned
			return report, err
		}
		chunk, err := reader.ReadSlice('\n')
		report.ScannedBytes = input.Scanned
		// Only evaluate full lines or an EOF-terminated last line.
		if errors.Is(err, contract.ErrReadLimit) {
			report.Reason = "scan_limit"
			return report, nil
		}
		if err != nil && err != io.EOF && err != bufio.ErrBufferFull {
			return report, err
		}
		chunk = bytes.TrimSuffix(chunk, []byte{'\n'})
		if len(line)+len(chunk) > maxLineBytes {
			report.Reason = "line_limit"
			return report, nil
		}
		line = append(line, chunk...)
		if err == bufio.ErrBufferFull {
			continue
		}
		if bytes.Contains(line, needle) {
			if len(report.Matches) == maxMatches {
				report.Reason = "match_limit"
				return report, nil
			}
			preview := strings.ToValidUTF8(string(line[:min(len(line), previewBytes)]), "?")
			report.Matches = append(report.Matches, Match{Line: lineNumber, Preview: preview})
			if len(line) > previewBytes {
				report.Reason = "preview_limit"
				return report, nil
			}
		}
		if err == io.EOF {
			report.Coverage = "complete"
			return report, nil
		}
		line, lineNumber = line[:0], lineNumber+1
	}
}
