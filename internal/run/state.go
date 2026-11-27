package run

import (
	"context"
	"sync"
	"time"

	"github.com/shubolab/superagent/internal/contract"
)

type state struct {
	request    contract.StartRequest
	budget     limits
	deadline   time.Time
	compaction *compactionState
	ctx        context.Context
	cancel     context.CancelFunc
	done       chan struct{}

	mu          sync.Mutex
	finishedAt  time.Time
	result      contract.Result
	events      observationBuffer
	step        uint32
	messages    []contract.Message
	activeCall  contract.CallID
	stopOutcome contract.RunOutcome
	stop        *contract.StopDetail
	usage       *contract.Usage
}

func newState(req contract.StartRequest, budget limits, deadline time.Time, bufferBytes uint64) *state {
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	input := req.Input
	s := &state{
		request: req, budget: budget, deadline: deadline, ctx: ctx, cancel: cancel,
		done: make(chan struct{}), events: newObservationBuffer(req.ID, bufferBytes),
		messages: []contract.Message{{User: &input}},
	}
	s.events.publish(contract.Event{Kind: contract.EventRunStarted})
	return s
}

func (s *state) expired(now time.Time, retention time.Duration) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return !s.finishedAt.IsZero() && !now.Before(s.finishedAt.Add(retention))
}

func (s *state) runningLocked() bool {
	if s.result.Outcome != "" || s.stop != nil {
		return false
	}
	if !time.Now().Before(s.deadline) {
		s.stopLocked(contract.TimedOut, contract.StopDeadline)
		return false
	}
	return true
}

func (s *state) stopLocked(outcome contract.RunOutcome, code contract.StopCode) {
	s.stopOutcome = outcome
	s.stop = &contract.StopDetail{Code: code, Step: s.step, CallID: s.activeCall}
	if outcome != contract.TimedOut {
		s.cancel()
	}
	if s.activeCall == "" {
		s.finishLocked(outcome, nil, nil, s.stop)
	}
}

func (s *state) finishLocked(outcome contract.RunOutcome, answer *contract.AssistantMessage, usage *contract.Usage, stop *contract.StopDetail) {
	result := contract.Result{
		RunID: s.request.ID, Outcome: outcome, Answer: answer, Usage: copyValue(usage), Stop: stop,
		Messages: s.messages,
	}
	if answer != nil {
		result.Messages = append(result.Messages, contract.Message{Assistant: answer})
		s.events.publish(contract.Event{
			Kind: contract.EventMessageCompleted, Step: s.step, Message: &contract.AssistantPreview{Text: answer.Text},
		})
	}
	s.result = result
	// Runs which may hold environment resources publish after their bounded cleanup attempt.
	if len(s.request.Tools) == 0 {
		s.publishFinishedLocked("")
	}
}

func (s *state) publishFinishedLocked(cleanup contract.CleanupStatus) {
	s.result.Cleanup = cleanup
	if s.compaction != nil {
		s.result.ContextCompactions = s.compaction.diagnostics
	}
	s.finishedAt = time.Now()
	s.events.publish(contract.Event{Kind: contract.EventRunFinished, Outcome: s.result.Outcome})
	if s.result.Outcome != contract.TimedOut {
		s.cancel()
	}
	close(s.done)
}
