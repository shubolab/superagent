package runner

import (
	"encoding/json"
	"time"

	"github.com/shubolab/superagent/internal/contract"
)

type limits struct {
	Timeout       time.Duration
	ModelRequests uint64
	ModelInput    uint64
	Output        uint64
	ToolRequests  uint64
	Scan          uint64
	Raw           uint64
}

func configuredBudget(b contract.Budget) (limits, bool) {
	if b.Timeout == nil || b.MaxModelRequests == nil || b.MaxModelInputBytes == nil || b.MaxOutputBytes == nil {
		return limits{}, false
	}
	l := limits{
		Timeout: *b.Timeout, ModelRequests: *b.MaxModelRequests, ModelInput: *b.MaxModelInputBytes, Output: *b.MaxOutputBytes,
		ToolRequests: valueOr(b.MaxToolRequests, 0), Scan: valueOr(b.MaxToolScanBytes, 0), Raw: valueOr(b.MaxToolRawOutputBytes, 0),
	}
	return l, l.Timeout > 0 && l.ModelRequests > 0 && l.ModelInput > 0 && l.Output > 0
}

func requestBudget(req contract.Budget, defaults, caps limits) (limits, bool) {
	l := limits{
		Timeout:       min(valueOr(req.Timeout, defaults.Timeout), caps.Timeout),
		ModelRequests: min(valueOr(req.MaxModelRequests, defaults.ModelRequests), caps.ModelRequests),
		ModelInput:    min(valueOr(req.MaxModelInputBytes, defaults.ModelInput), caps.ModelInput),
		Output:        min(valueOr(req.MaxOutputBytes, defaults.Output), caps.Output),
		ToolRequests:  min(valueOr(req.MaxToolRequests, defaults.ToolRequests), caps.ToolRequests),
		Scan:          min(valueOr(req.MaxToolScanBytes, defaults.Scan), caps.Scan),
		Raw:           min(valueOr(req.MaxToolRawOutputBytes, defaults.Raw), caps.Raw),
	}
	return l, l.Timeout > 0 && l.ModelRequests > 0 && l.ModelInput > 0 && l.Output > 0
}

func valueOr[T any](ptr *T, fallback T) T {
	if ptr != nil {
		return *ptr
	}
	return fallback
}

// Input metering uses the compact JSON object containing Instruction and Messages.
// Measure its values separately so an oversized request never creates a full encoded copy.
func inputFits(req contract.ModelRequest, limit uint64) bool {
	_, ok := inputSize(req, limit)
	return ok
}

func inputSize(req contract.ModelRequest, limit uint64) (uint64, bool) {
	size := uint64(len(`{"Instruction":,"Messages":[]}`))
	if size > limit || uint64(len(req.Instruction)) > limit-size {
		return 0, false
	}
	instruction, _ := json.Marshal(req.Instruction)
	size += uint64(len(instruction))
	for i, message := range req.Messages {
		if i > 0 {
			size++ // The comma between messages.
		}
		text := ""
		if message.User != nil {
			text = message.User.Text
		} else if message.Assistant != nil {
			text = message.Assistant.Text
		} else if message.Tool != nil && message.Tool.Result.Completion != nil {
			text = message.Tool.Result.Completion.Content
		}
		if size > limit || uint64(len(text)) > limit-size {
			return 0, false
		}
		encoded, _ := json.Marshal(message)
		size += uint64(len(encoded))
	}
	if len(req.Tools) > 0 && size <= limit {
		encoded, _ := json.Marshal(req.Tools)
		size += uint64(len(`,"Tools":`) + len(encoded))
	}
	if req.Context != nil && len(req.Context.Summary) > 0 && size <= limit {
		encoded, _ := json.Marshal(req.Context.Summary)
		size += uint64(len(`,"Summary":`) + len(encoded))
	}
	return size, size <= limit
}
