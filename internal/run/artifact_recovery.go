package run

import (
	"context"

	"github.com/shubolab/superagent/internal/contract"
	"github.com/shubolab/superagent/internal/mesh"
)

func meterRetainedResult(result *contract.ToolResult, limits contract.ToolLimits) (uint64, bool) {
	if result.Completion == nil || len(result.Completion.Artifacts) != 1 || limits.MaxRawOutputBytes == 0 {
		return 0, false
	}
	raw := result.Completion.Artifacts[0].SizeBytes
	if raw > limits.MaxRawOutputBytes {
		return 0, false
	}
	if _, fits := receiptSize(*result, limits.MaxResultBytes); !fits {
		return 0, false
	}
	if result.Usage == nil {
		result.Usage = &contract.ToolUsage{}
	}
	result.Usage.RawOutputBytes = &raw
	_, fits := receiptSize(*result, limits.MaxResultBytes)
	return raw, fits
}

func verifyResumedArtifacts(s *state, a *activity, verifiedCall contract.CallID) bool {
	s.mu.Lock()
	c := &s.checkpoint.data
	if c.ArtifactStoreID == "" {
		s.mu.Unlock()
		return true
	}
	var refs []contract.ArtifactRef
	collect := func(result contract.ToolResult) {
		if result.CallID != verifiedCall && result.Completion != nil {
			refs = append(refs, result.Completion.Artifacts...)
		}
	}
	if c.Pending != nil {
		for _, result := range c.ToolResults {
			collect(result)
		}
	} else {
		for i := len(c.Messages) - 1; i >= 0 && c.Messages[i].Tool != nil; i-- {
			collect(c.Messages[i].Tool.Result)
		}
	}
	s.mu.Unlock()
	if len(refs) == 0 {
		return true
	}
	ctx, cancel := context.WithDeadline(s.ctx, s.reconciliationDeadline())
	defer cancel()
	for _, ref := range refs {
		if err := mesh.CheckRetainedArtifact(ctx, a.transport, a.env, ref); err != nil {
			s.failModel(contract.Failed, contract.StopArtifactUnavailable, 0)
			return false
		}
	}
	return true
}
