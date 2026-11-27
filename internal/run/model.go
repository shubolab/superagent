package run

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"unicode/utf8"

	"github.com/shubolab/superagent/internal/contract"
)

func (r *Runner) generate(s *state, subject contract.SubjectID) {
	stopDeadline := context.AfterFunc(s.ctx, func() {
		if errors.Is(s.ctx.Err(), context.DeadlineExceeded) {
			s.mu.Lock()
			defer s.mu.Unlock()
			s.runningLocked()
		}
	})
	defer stopDeadline()

	env := r.newActivity(s, subject)
	if len(s.request.Tools) != 0 {
		defer env.finish(s)
	}
	balance := s.budget
	firstStep := uint32(1)
	var pending *contract.AssistantMessage
	if d := s.checkpoint; d != nil && d.resumed {
		s.mu.Lock()
		balance, firstStep, pending = d.data.Balance, d.data.Step, d.data.Pending
		s.mu.Unlock()
		if !r.resumePosition(s, env, &balance) {
			return
		}
	}
	if !env.allocate(s) {
		return
	}

	committedIDs := make(map[contract.CallID]struct{})
	for _, message := range s.messages {
		if message.Assistant != nil {
			for _, call := range message.Assistant.ToolCalls {
				committedIDs[call.CallID] = struct{}{}
			}
		}
	}
	if pending != nil {
		for _, call := range pending.ToolCalls {
			committedIDs[call.CallID] = struct{}{}
		}
		if !r.toolBatch(s, env, &balance, pending) {
			return
		}
		firstStep++
	}
	for step := firstStep; ; step++ {
		s.mu.Lock()
		if !s.runningLocked() {
			s.mu.Unlock()
			return
		}
		s.step = step
		committed := s.messages
		s.mu.Unlock()
		req := contract.ModelRequest{
			RunID: s.request.ID, Step: step, Model: s.request.Model, Instruction: s.request.Instruction,
			Messages: append(cloneMessages(s.request.History), cloneMessages(committed)...),
			Tools:    r.modelTools(s.request.Tools), MaxOutputBytes: balance.Output,
		}
		switch {
		case balance.ModelRequests == 0 || step == 0:
			s.failModel(contract.BudgetExceeded, contract.StopModelRequestLimit, s.budget.ModelRequests)
			return
		case balance.Output == 0:
			s.failModel(contract.BudgetExceeded, contract.StopModelOutputLimit, s.budget.Output)
			return
		}
		policy, metered := r.contextPolicies[req.Model]
		var measured modelInput
		var input uint64
		if s.compaction != nil {
			checked, ok := r.prepareCompaction(s, subject, req, policy, &balance)
			if !ok {
				if errors.Is(checked.err, errMessageVersionChanged) {
					s.mu.Lock()
					step = s.step
					s.mu.Unlock()
					continue
				}
				return
			}
			req, input, measured = checked.req, checked.bytes, checked.input
			step = req.Step
		} else {
			var fits bool
			input, fits = inputSize(req, balance.ModelInput)
			if !fits {
				s.failModel(contract.BudgetExceeded, contract.StopModelInputLimit, s.budget.ModelInput)
				return
			}
			if metered {
				req.Context = &contract.ModelContextOptions{MaxOutputTokens: policy.MaxOutputTokens}
				var err error
				measured, err = r.measureInput(s, subject, &req, policy)
				if err != nil {
					s.failModel(contract.Failed, contract.StopContextMeasurementUnavailable, 0)
					return
				}
				if measured.measurement.Tokens > measured.limit {
					s.failWindow(measured.limit)
					return
				}
			}
		}
		balance.ModelRequests--
		balance.ModelInput -= input
		if !s.recordModelIntent(step, balance) {
			return
		}
		reply, output := r.modelReply(s, subject, req, committedIDs)
		if reply == nil {
			return
		}
		balance.Output -= output
		if metered {
			r.recordEstimate(subject, req.Model, measured.measurement, reply.Usage, policy.SafetyMarginTokens)
		}
		s.mu.Lock()
		if !s.runningLocked() {
			s.mu.Unlock()
			return
		}
		s.addUsageLocked(reply.Usage)
		assistant := &contract.AssistantMessage{Text: reply.Text, ToolCalls: cloneCalls(reply.ToolCalls)}
		if d := s.checkpoint; d != nil {
			d.data.ModelInFlight, d.data.ModelAdmitted, d.data.Balance = false, false, balance
			if reply.Finish == contract.ModelFinishToolCalls {
				d.data.Pending = assistant
				if !s.saveLocked() {
					s.mu.Unlock()
					return
				}
			}
		}
		if reply.Finish == contract.ModelFinishStop {
			s.finishLocked(contract.Succeeded, assistant, s.usage, nil)
			s.mu.Unlock()
			return
		}
		preview := &contract.AssistantPreview{Text: reply.Text}
		for _, call := range assistant.ToolCalls {
			preview.Calls = append(preview.Calls, contract.ToolEvent{CallID: call.CallID, Tool: call.Tool})
			committedIDs[call.CallID] = struct{}{}
		}
		s.events.publish(contract.Event{Kind: contract.EventMessageCompleted, Step: step, Message: preview})
		s.mu.Unlock()
		if !r.toolBatch(s, env, &balance, assistant) {
			return
		}
	}
}

func (r *Runner) modelReply(s *state, subject contract.SubjectID, req contract.ModelRequest, used map[contract.CallID]struct{}) (*contract.ModelReply, uint64) {
	if s.ctx.Err() != nil {
		s.failModel(contract.Failed, contract.StopModelFailure, 0)
		return nil, 0
	}
	if s.compaction != nil {
		req = measurementSnapshot(req)
	}
	stream, err := r.open(s.ctx, subject, req)
	if err != nil || stream == nil {
		s.failAdapter(err)
		return nil, 0
	}
	closeStream := sync.OnceFunc(func() { _ = stream.Close() })
	defer closeStream()
	stopClose := context.AfterFunc(s.ctx, closeStream)
	defer stopClose()

	var draft strings.Builder
	for s.ctx.Err() == nil {
		event, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				err = contract.ErrModelResponseTruncated
			}
			s.failAdapter(err)
			return nil, 0
		}
		switch event.Kind {
		case contract.ModelTextDelta:
			if event.TextDelta == "" || event.Reply != nil || !utf8.ValidString(event.TextDelta) {
				s.failModel(contract.Failed, contract.StopModelProtocol, 0)
				return nil, 0
			}
			if uint64(draft.Len())+uint64(len(event.TextDelta)) > req.MaxOutputBytes {
				s.failModel(contract.BudgetExceeded, contract.StopModelOutputLimit, s.budget.Output)
				return nil, 0
			}
			s.mu.Lock()
			if !s.runningLocked() {
				s.mu.Unlock()
				return nil, 0
			}
			draft.WriteString(event.TextDelta)
			s.events.publish(contract.Event{Kind: contract.EventTextDelta, Step: req.Step, TextDelta: event.TextDelta})
			s.mu.Unlock()
		case contract.ModelCompleted:
			return s.acceptModel(req, draft.String(), event, used)
		default:
			s.failModel(contract.Failed, contract.StopModelProtocol, 0)
			return nil, 0
		}
	}
	s.failModel(contract.Failed, contract.StopModelFailure, 0)
	return nil, 0
}

func (s *state) failAdapter(err error) {
	var code contract.StopCode
	switch {
	case errors.Is(err, contract.ErrContextMeasurementUnavailable):
		code = contract.StopContextMeasurementUnavailable
	case errors.Is(err, contract.ErrContextInputChanged):
		code = contract.StopContextInputChanged
	case errors.Is(err, contract.ErrModelContextOverflow):
		code = contract.StopModelContextOverflow
	case errors.Is(err, contract.ErrModelAuthFailed):
		code = contract.StopModelAuthFailed
	case errors.Is(err, contract.ErrModelNotFound):
		code = contract.StopModelNotFound
	case errors.Is(err, contract.ErrModelResponseTruncated):
		code = contract.StopModelTruncated
	case errors.Is(err, contract.ErrModelResponseProtocol):
		code = contract.StopModelProtocol
	case errors.Is(err, contract.ErrModelOutputLimit):
		s.failModel(contract.BudgetExceeded, contract.StopModelOutputLimit, s.budget.Output)
		return
	default:
		code = contract.StopModelFailure
	}
	s.failModel(contract.Failed, code, 0)
}

func (s *state) failModel(outcome contract.RunOutcome, code contract.StopCode, limit uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.runningLocked() {
		s.finishLocked(outcome, nil, s.usage, &contract.StopDetail{Code: code, Step: s.step, LimitBytes: limit})
	}
}

func (s *state) acceptModel(req contract.ModelRequest, draft string, event contract.ModelEvent, used map[contract.CallID]struct{}) (*contract.ModelReply, uint64) {
	reply := event.Reply
	if reply == nil || event.TextDelta != "" || !utf8.ValidString(reply.Text) ||
		(reply.Finish == contract.ModelFinishStop && len(reply.ToolCalls) != 0) ||
		(reply.Finish == contract.ModelFinishToolCalls && (len(reply.ToolCalls) == 0 || len(req.Tools) == 0)) ||
		(reply.Finish != contract.ModelFinishStop && reply.Finish != contract.ModelFinishToolCalls) {
		s.failModel(contract.Failed, contract.StopModelProtocol, 0)
		return nil, 0
	}
	output := uint64(len(draft)) + uint64(len(reply.Text)-sharedTextPrefix(draft, reply.Text))
	for _, call := range reply.ToolCalls {
		bytes := uint64(len(call.CallID)) + uint64(len(call.Tool)) + uint64(len(call.Arguments))
		if output > req.MaxOutputBytes || bytes > req.MaxOutputBytes-output {
			s.failModel(contract.BudgetExceeded, contract.StopModelOutputLimit, s.budget.Output)
			return nil, 0
		}
		output += bytes
	}
	if output > req.MaxOutputBytes {
		s.failModel(contract.BudgetExceeded, contract.StopModelOutputLimit, s.budget.Output)
		return nil, 0
	}
	if !validCalls(reply.ToolCalls, used) {
		s.failModel(contract.Failed, contract.StopModelProtocol, 0)
		return nil, 0
	}
	snapshot := *reply
	snapshot.Usage = copyValue(reply.Usage)
	snapshot.ToolCalls = cloneCalls(reply.ToolCalls)
	return &snapshot, output
}

func validCalls(calls []contract.ModelToolCall, prior map[contract.CallID]struct{}) bool {
	seen := make(map[contract.CallID]struct{}, len(calls))
	for _, call := range calls {
		_, repeated := seen[call.CallID]
		_, reused := prior[call.CallID]
		if call.CallID == "" || call.Tool == "" || !utf8.ValidString(string(call.CallID)) ||
			!utf8.ValidString(string(call.Tool)) || !utf8.Valid(call.Arguments) ||
			!contract.JSONObject(call.Arguments) || repeated || reused {
			return false
		}
		seen[call.CallID] = struct{}{}
	}
	return true
}

func (s *state) addUsageLocked(usage *contract.Usage) {
	if usage == nil {
		return
	}
	if s.usage == nil {
		s.usage = &contract.Usage{}
	}
	s.usage.InputTokens = addUsage(s.usage.InputTokens, usage.InputTokens)
	s.usage.OutputTokens = addUsage(s.usage.OutputTokens, usage.OutputTokens)
}

func addUsage(previous, added uint64) uint64 {
	return previous + min(added, ^uint64(0)-previous)
}

func sharedTextPrefix(a, b string) int {
	i := 0
	for i < len(a) && i < len(b) && a[i] == b[i] {
		i++
	}
	for i < len(b) && !utf8.RuneStart(b[i]) {
		i--
	}
	return i
}
