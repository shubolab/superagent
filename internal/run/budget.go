package run

import (
	"encoding/json"
	"time"

	"github.com/shubolab/superagent/internal/contract"
)

type limits struct {
	timeout       time.Duration
	modelRequests uint64
	modelInput    uint64
	output        uint64
	toolRequests  uint64
	scan          uint64
}

func configuredBudget(b contract.Budget) (limits, bool) {
	if b.Timeout == nil || b.MaxModelRequests == nil || b.MaxModelInputBytes == nil || b.MaxOutputBytes == nil {
		return limits{}, false
	}
	l := limits{
		timeout: *b.Timeout, modelRequests: *b.MaxModelRequests, modelInput: *b.MaxModelInputBytes, output: *b.MaxOutputBytes,
		toolRequests: valueOr(b.MaxToolRequests, 0), scan: valueOr(b.MaxToolScanBytes, 0),
	}
	return l, l.timeout > 0 && l.modelRequests > 0 && l.modelInput > 0 && l.output > 0
}

func requestBudget(req contract.Budget, defaults, caps limits) (limits, bool) {
	l := limits{
		timeout:       min(valueOr(req.Timeout, defaults.timeout), caps.timeout),
		modelRequests: min(valueOr(req.MaxModelRequests, defaults.modelRequests), caps.modelRequests),
		modelInput:    min(valueOr(req.MaxModelInputBytes, defaults.modelInput), caps.modelInput),
		output:        min(valueOr(req.MaxOutputBytes, defaults.output), caps.output),
		toolRequests:  min(valueOr(req.MaxToolRequests, defaults.toolRequests), caps.toolRequests),
		scan:          min(valueOr(req.MaxToolScanBytes, defaults.scan), caps.scan),
	}
	return l, l.timeout > 0 && l.modelRequests > 0 && l.modelInput > 0 && l.output > 0
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
	return size, size <= limit
}
