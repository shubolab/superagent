package superagent

import (
	"context"

	"github.com/shubolab/superagent/internal/schedule"
)

type StartPhase string

const (
	StartQueued    StartPhase = "queued"
	StartAdmitting StartPhase = "admitting"
	StartResolved  StartPhase = "resolved"
)

// StartUpdate is one admission milestone or the final outcome of the same Start.
type StartUpdate struct {
	RunID RunID
	Phase StartPhase
	Run   *Run
	Err   error
}

// StartWithProgress freezes the input before returning and closes its bounded stream
// after the final reply. A slow reader does not delay admission.
func (s *Session) StartWithProgress(ctx context.Context, req StartRequest) <-chan StartUpdate {
	if !validSessionStart(req) {
		return failedStartUpdates(req.ID, ErrInvalidRequest)
	}
	req.Recoverable = true
	frozen, digest, err := freezeScheduledRequest(req)
	if err != nil {
		return failedStartUpdates(req.ID, err)
	}
	return startUpdates(req.ID, func(report func(schedule.AdmissionPhase)) (*Run, error) {
		return s.startSession(ctx, frozen, digest, report)
	})
}

func failedStartUpdates(id RunID, err error) <-chan StartUpdate {
	updates := make(chan StartUpdate, 1)
	updates <- StartUpdate{RunID: id, Phase: StartResolved, Err: err}
	close(updates)
	return updates
}

func startUpdates(id RunID, work func(func(schedule.AdmissionPhase)) (*Run, error)) <-chan StartUpdate {
	updates := make(chan StartUpdate, 3)
	go func() {
		defer close(updates)
		run, err := work(func(phase schedule.AdmissionPhase) {
			updates <- StartUpdate{RunID: id, Phase: StartPhase(phase)}
		})
		updates <- StartUpdate{RunID: id, Phase: StartResolved, Run: run, Err: err}
	}()
	return updates
}
