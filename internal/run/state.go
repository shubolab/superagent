package run

import (
	"context"
	"sync"
	"time"

	"github.com/shubolab/superagent/internal/contract"
)

type state struct {
	request  contract.StartRequest
	budget   limits
	deadline time.Time
	ctx      context.Context
	cancel   context.CancelFunc
	done     chan struct{}

	mu         sync.Mutex
	finishedAt time.Time
	result     contract.Result
	events     observationBuffer
}

func newState(req contract.StartRequest, budget limits, deadline time.Time, bufferBytes uint64) *state {
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	s := &state{
		request: req, budget: budget, deadline: deadline, ctx: ctx, cancel: cancel,
		done: make(chan struct{}), events: newObservationBuffer(req.ID, bufferBytes),
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
	if !s.finishedAt.IsZero() {
		return false
	}
	if !time.Now().Before(s.deadline) {
		s.finishLocked(contract.TimedOut, nil, nil, &contract.StopDetail{Code: contract.StopDeadline})
		return false
	}
	return true
}

func (s *state) finishLocked(outcome contract.RunOutcome, answer *contract.AssistantMessage, usage *contract.Usage, stop *contract.StopDetail) {
	input := s.request.Input
	result := contract.Result{
		RunID: s.request.ID, Outcome: outcome, Answer: answer, Usage: copyValue(usage), Stop: stop,
		Messages: []contract.Message{{User: &input}},
	}
	if answer != nil {
		result.Messages = append(result.Messages, contract.Message{Assistant: answer})
		s.events.publish(contract.Event{
			Kind: contract.EventMessageCompleted, Step: 1, Message: &contract.AssistantPreview{Text: answer.Text},
		})
	}
	s.result = result
	s.finishedAt = time.Now()
	s.events.publish(contract.Event{Kind: contract.EventRunFinished, Outcome: outcome})
	// A timed-out Run lets the deadline context report DeadlineExceeded to its model.
	if outcome != contract.TimedOut {
		s.cancel()
	}
	close(s.done)
}
