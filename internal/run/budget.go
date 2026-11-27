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
}

func configuredBudget(b contract.Budget) (limits, bool) {
	if b.Timeout == nil || b.MaxModelRequests == nil || b.MaxModelInputBytes == nil || b.MaxOutputBytes == nil {
		return limits{}, false
	}
	l := limits{*b.Timeout, *b.MaxModelRequests, *b.MaxModelInputBytes, *b.MaxOutputBytes}
	return l, l.timeout > 0 && l.modelRequests > 0 && l.modelInput > 0 && l.output > 0
}

func requestBudget(req contract.Budget, defaults, caps limits) (limits, bool) {
	l := limits{
		timeout:       min(valueOr(req.Timeout, defaults.timeout), caps.timeout),
		modelRequests: min(valueOr(req.MaxModelRequests, defaults.modelRequests), caps.modelRequests),
		modelInput:    min(valueOr(req.MaxModelInputBytes, defaults.modelInput), caps.modelInput),
		output:        min(valueOr(req.MaxOutputBytes, defaults.output), caps.output),
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
	size := uint64(len(`{"Instruction":,"Messages":[]}`))
	if size > limit || uint64(len(req.Instruction)) > limit-size {
		return false
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
		} else {
			text = message.Assistant.Text
		}
		if size > limit || uint64(len(text)) > limit-size {
			return false
		}
		encoded, _ := json.Marshal(message)
		size += uint64(len(encoded))
	}
	return size <= limit
}
