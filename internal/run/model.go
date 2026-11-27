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
	if !env.allocate(s) {
		return
	}

	balance := s.budget
	committedIDs := make(map[contract.CallID]struct{})
	for step := uint32(1); ; step++ {
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
			Tools:    r.modelTools(s.request.Tools), MaxOutputBytes: balance.output,
		}
		input, fits := inputSize(req, balance.modelInput)
		switch {
		case balance.modelRequests == 0 || step == 0:
			s.failModel(contract.BudgetExceeded, contract.StopModelRequestLimit, s.budget.modelRequests)
			return
		case !fits:
			s.failModel(contract.BudgetExceeded, contract.StopModelInputLimit, s.budget.modelInput)
			return
		case balance.output == 0:
			s.failModel(contract.BudgetExceeded, contract.StopModelOutputLimit, s.budget.output)
			return
		}
		balance.modelRequests--
		balance.modelInput -= input
		reply, output := r.modelReply(s, subject, req, committedIDs)
		if reply == nil {
			return
		}
		balance.output -= output
		s.mu.Lock()
		if !s.runningLocked() {
			s.mu.Unlock()
			return
		}
		if reply.Usage != nil {
			if s.usage == nil {
				s.usage = &contract.Usage{}
			}
			s.usage.InputTokens = addUsage(s.usage.InputTokens, reply.Usage.InputTokens)
			s.usage.OutputTokens = addUsage(s.usage.OutputTokens, reply.Usage.OutputTokens)
		}
		assistant := &contract.AssistantMessage{Text: reply.Text, ToolCalls: cloneCalls(reply.ToolCalls)}
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
				s.failModel(contract.BudgetExceeded, contract.StopModelOutputLimit, s.budget.output)
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
	case errors.Is(err, contract.ErrModelAuthFailed):
		code = contract.StopModelAuthFailed
	case errors.Is(err, contract.ErrModelNotFound):
		code = contract.StopModelNotFound
	case errors.Is(err, contract.ErrModelResponseTruncated):
		code = contract.StopModelTruncated
	case errors.Is(err, contract.ErrModelResponseProtocol):
		code = contract.StopModelProtocol
	case errors.Is(err, contract.ErrModelOutputLimit):
		s.failModel(contract.BudgetExceeded, contract.StopModelOutputLimit, s.budget.output)
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
			s.failModel(contract.BudgetExceeded, contract.StopModelOutputLimit, s.budget.output)
			return nil, 0
		}
		output += bytes
	}
	if output > req.MaxOutputBytes {
		s.failModel(contract.BudgetExceeded, contract.StopModelOutputLimit, s.budget.output)
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
