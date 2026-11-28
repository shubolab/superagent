package sdk

import (
	"context"

	"github.com/shubolab/superagent/internal/contract"
	"github.com/shubolab/superagent/schedule"
)

type StartPhase string

const (
	StartQueued    StartPhase = "queued"
	StartAdmitting StartPhase = "admitting"
	StartResolved  StartPhase = "resolved"
)

// StartUpdate is an admission milestone or its final outcome.
type StartUpdate struct {
	RunID contract.RunID
	Phase StartPhase
	Run   *Run
	Err   error
}

func startUpdate(id contract.RunID, phase StartPhase, run *Run, err error) StartUpdate {
	return StartUpdate{RunID: id, Phase: phase, Run: run, Err: err}
}

// StartWithProgress freezes the request before returning a bounded stream of updates.
func (s *Session) StartWithProgress(ctx context.Context, req contract.StartRequest) <-chan StartUpdate {
	return StartWithProgress(s, ctx, req, startUpdate)
}

// StartWithProgress freezes the input before returning and reports progress
// using the caller's SDK handle and event types.
func StartWithProgress[T any](s *Session, ctx context.Context, req contract.StartRequest, update func(contract.RunID, StartPhase, *Run, error) T) <-chan T {
	if !validSessionStart(req) {
		return failedStartUpdates(update(req.ID, StartResolved, nil, contract.ErrInvalidRequest))
	}
	req.Recoverable = true
	frozen, digest, err := freezeScheduledRequest(req)
	if err != nil {
		return failedStartUpdates(update(req.ID, StartResolved, nil, err))
	}
	return startUpdates(req.ID, update, func(report func(schedule.AdmissionPhase)) (*Run, error) {
		return s.startSession(ctx, frozen, digest, report)
	})
}

func failedStartUpdates[T any](failure T) <-chan T {
	updates := make(chan T, 1)
	updates <- failure
	close(updates)
	return updates
}

func startUpdates[T any](id contract.RunID, update func(contract.RunID, StartPhase, *Run, error) T, work func(func(schedule.AdmissionPhase)) (*Run, error)) <-chan T {
	updates := make(chan T, 3)
	go func() {
		defer close(updates)
		run, err := work(func(phase schedule.AdmissionPhase) {
			updates <- update(id, StartPhase(phase), nil, nil)
		})
		updates <- update(id, StartResolved, run, err)
	}()
	return updates
}
