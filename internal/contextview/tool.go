package contextview

import (
	"bytes"
	"context"
	"encoding/json"

	"github.com/shubolab/superagent/internal/contract"
)

type sourceTool struct{}

type sourceRequest struct {
	Ref      contract.ContextSourceRef `json:"ref"`
	Offset   *uint64                   `json:"offset"`
	MaxBytes uint64                    `json:"max_bytes"`
}

func NewSourceTool() contract.LocalToolBinding {
	return contract.LocalToolBinding{
		Spec: contract.ToolSpec{
			Description:        "Read original context. Content is a base64 byte range; continue with NextOffset until EOF.",
			InputSchema:        json.RawMessage(`{"type":"object","properties":{"ref":{"type":"string"},"offset":{"type":"integer","minimum":0},"max_bytes":{"type":"integer","minimum":1}},"required":["ref","offset","max_bytes"],"additionalProperties":false}`),
			NeedsContextSource: true,
			Limits:             contract.ToolLimits{MaxResultBytes: 8192},
		},
		Tool: sourceTool{},
		Prepare: func(raw json.RawMessage) (string, error) {
			_, err := parseSourceRequest(raw)
			return "", err
		},
	}
}

func parseSourceRequest(raw json.RawMessage) (sourceRequest, error) {
	var args sourceRequest
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if !contract.JSONObject(raw) || dec.Decode(&args) != nil || args.Ref == "" || args.Offset == nil || args.MaxBytes == 0 {
		return sourceRequest{}, contract.ErrInvalidRequest
	}
	return args, nil
}

func (sourceTool) Invoke(ctx context.Context, req contract.ToolRequest) (contract.ToolResult, error) {
	result := contract.ToolResult{RunID: req.RunID, CallID: req.CallID, State: contract.ToolExecutionNotExecuted}
	reader := contract.ContextSource(ctx)
	args, err := parseSourceRequest(req.Arguments)
	if reader == nil || err != nil {
		result.Reason = &contract.ToolReason{Code: "context_source_unavailable"}
		return result, nil
	}
	result.State = contract.ToolExecutionKnown
	result.Completion = &contract.ToolCompletion{Outcome: contract.ToolSucceeded}
	base, _ := json.Marshal(result)
	envelope := uint64(len(base) + 100)
	if req.Limits.MaxResultBytes <= envelope {
		return sourceFailure(req, "result_limit"), nil
	}
	maxBytes := (req.Limits.MaxResultBytes - envelope) / 4 * 3
	if maxBytes == 0 {
		return sourceFailure(req, "result_limit"), nil
	}
	page, err := reader.ReadSource(ctx, args.Ref, *args.Offset, min(args.MaxBytes, maxBytes))
	if err != nil {
		return sourceFailure(req, "context_source_unavailable"), nil
	}
	body, _ := json.Marshal(page)
	result.Completion.Content = string(body)
	encoded, _ := json.Marshal(result)
	if uint64(len(encoded)) > req.Limits.MaxResultBytes {
		return sourceFailure(req, "result_limit"), nil
	}
	return result, nil
}

func sourceFailure(req contract.ToolRequest, reason string) contract.ToolResult {
	return contract.ToolResult{RunID: req.RunID, CallID: req.CallID, State: contract.ToolExecutionNotExecuted,
		Reason: &contract.ToolReason{Code: reason}}
}
