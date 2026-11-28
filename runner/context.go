package runner

import (
	"bytes"
	"context"
	"slices"
	"time"

	"github.com/shubolab/superagent/internal/contract"
	"github.com/shubolab/superagent/mesh"
)

type modelInput struct {
	measurement contract.InputTokenMeasurement
	limit       uint64
}

func (r *Runner) measureInput(s *state, subject contract.SubjectID, req *contract.ModelRequest, policy contract.ModelContextPolicy) (modelInput, error) {
	reply, err := mesh.MeasureInput(s.ctx, r.devices, subject, measurementSnapshot(*req))
	if err != nil {
		return modelInput{}, err
	}
	m := reply.Measurement
	if m.InputDigest == "" || m.ProfileVersion == "" ||
		(m.Kind != contract.MeasurementExact && (m.Kind != contract.MeasurementEstimated || m.ProfileVersion != policy.CalibratedEstimateVersion)) {
		return modelInput{}, contract.ErrContextMeasurementUnavailable
	}
	limit, valid := windowLimit(reply.ModelLimits, req.Context.MaxOutputTokens, policy.SafetyMarginTokens)
	if !valid {
		return modelInput{}, contract.ErrContextMeasurementUnavailable
	}
	req.Context.ExpectedInputDigest = m.InputDigest
	req.Context.ProfileVersion = m.ProfileVersion
	return modelInput{measurement: m, limit: limit}, nil
}

func measurementSnapshot(req contract.ModelRequest) contract.ModelRequest {
	req.Messages = cloneMessages(req.Messages)
	req.Tools = slices.Clone(req.Tools)
	for i := range req.Tools {
		req.Tools[i].InputSchema = bytes.Clone(req.Tools[i].InputSchema)
	}
	req.Context = copyValue(req.Context)
	req.Context.Summary = slices.Clone(req.Context.Summary)
	for i := range req.Context.Summary {
		req.Context.Summary[i].Sources = slices.Clone(req.Context.Summary[i].Sources)
	}
	return req
}

func windowLimit(limits contract.ModelContextLimits, reserved, margin uint64) (uint64, bool) {
	if reserved == 0 || reserved >= limits.WindowTokens {
		return 0, false
	}
	usable := limits.WindowTokens - reserved
	if limits.MaxInputTokens != 0 {
		usable = min(usable, limits.MaxInputTokens)
	}
	if margin >= usable {
		return 0, false
	}
	return usable - margin, true
}

func (s *state) failWindow(limit uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.runningLocked() {
		s.finishLocked(contract.BudgetExceeded, nil, s.usage, &contract.StopDetail{
			Code: contract.StopContextWindowLimit, Step: s.step, LimitTokens: limit,
		})
	}
}

func (r *Runner) recordEstimate(subject contract.SubjectID, ref contract.ModelRef, m contract.InputTokenMeasurement, actual *contract.Usage, margin uint64) {
	if actual == nil || m.Kind != contract.MeasurementEstimated || actual.InputTokens <= m.Tokens ||
		actual.InputTokens-m.Tokens <= margin {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_ = mesh.InvalidateEstimate(ctx, r.devices, subject, ref, m.ProfileVersion)
}
