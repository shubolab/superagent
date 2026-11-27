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

	if s.ctx.Err() != nil {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.runningLocked()
		return
	}
	input := s.request.Input
	req := contract.ModelRequest{
		RunID: s.request.ID, Step: 1, Model: s.request.Model, Instruction: s.request.Instruction,
		Messages:       append(cloneMessages(s.request.History), contract.Message{User: &input}),
		MaxOutputBytes: s.budget.output,
	}
	if !inputFits(req, s.budget.modelInput) {
		s.failModel(contract.BudgetExceeded, contract.StopModelInputLimit, s.budget.modelInput)
		return
	}
	s.mu.Lock()
	proceed := s.runningLocked()
	s.mu.Unlock()
	if !proceed {
		return
	}
	// The pure text slice schedules only this one attempt.
	stream, err := r.open(s.ctx, subject, req)
	if err != nil || stream == nil {
		s.failModel(contract.Failed, contract.StopModelFailure, 0)
		return
	}
	closeStream := sync.OnceFunc(func() { _ = stream.Close() })
	defer closeStream()
	stopClose := context.AfterFunc(s.ctx, closeStream)
	defer stopClose()

	var draft strings.Builder
	for s.ctx.Err() == nil {
		event, err := stream.Recv()
		if err != nil {
			code := contract.StopModelFailure
			if errors.Is(err, io.EOF) {
				code = contract.StopModelTruncated
			}
			s.failModel(contract.Failed, code, 0)
			return
		}
		switch event.Kind {
		case contract.ModelTextDelta:
			if event.TextDelta == "" || event.Reply != nil {
				s.failModel(contract.Failed, contract.StopModelProtocol, 0)
				return
			}
			if uint64(draft.Len())+uint64(len(event.TextDelta)) > s.budget.output {
				s.failModel(contract.BudgetExceeded, contract.StopModelOutputLimit, s.budget.output)
				return
			}
			if !utf8.ValidString(event.TextDelta) {
				s.failModel(contract.Failed, contract.StopModelProtocol, 0)
				return
			}
			s.mu.Lock()
			if !s.runningLocked() {
				s.mu.Unlock()
				return
			}
			draft.WriteString(event.TextDelta)
			s.events.publish(contract.Event{Kind: contract.EventTextDelta, Step: 1, TextDelta: event.TextDelta})
			s.mu.Unlock()
		case contract.ModelCompleted:
			s.completeModel(draft.String(), event)
			return
		default:
			s.failModel(contract.Failed, contract.StopModelProtocol, 0)
			return
		}
	}
	// A deadline can exit the receive loop before the AfterFunc starts.
	s.mu.Lock()
	defer s.mu.Unlock()
	s.runningLocked()
}

func (s *state) failModel(outcome contract.RunOutcome, code contract.StopCode, limit uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.runningLocked() {
		s.finishLocked(outcome, nil, nil, &contract.StopDetail{Code: code, Step: 1, LimitBytes: limit})
	}
}

func (s *state) completeModel(draft string, event contract.ModelEvent) {
	reply := event.Reply
	if reply == nil || reply.Finish != contract.ModelFinishStop || event.TextDelta != "" {
		s.failModel(contract.Failed, contract.StopModelProtocol, 0)
		return
	}
	if uint64(len(reply.Text)) > s.budget.output {
		s.failModel(contract.BudgetExceeded, contract.StopModelOutputLimit, s.budget.output)
		return
	}
	if !utf8.ValidString(reply.Text) {
		s.failModel(contract.Failed, contract.StopModelProtocol, 0)
		return
	}
	if uint64(len(draft))+uint64(len(reply.Text)-sharedTextPrefix(draft, reply.Text)) > s.budget.output {
		s.failModel(contract.BudgetExceeded, contract.StopModelOutputLimit, s.budget.output)
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.runningLocked() {
		s.finishLocked(contract.Succeeded, &contract.AssistantMessage{Text: reply.Text}, reply.Usage, nil)
	}
}

func sharedTextPrefix(a, b string) int {
	i := 0
	for i < len(a) && i < len(b) && a[i] == b[i] {
		i++
	}
	// Only complete UTF-8 characters are shared between the draft and snapshot.
	for i < len(b) && !utf8.RuneStart(b[i]) {
		i--
	}
	return i
}
