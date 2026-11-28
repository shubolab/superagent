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
	updates := make(chan StartUpdate, 3)
	finish := func(run *Run, err error) {
		updates <- StartUpdate{RunID: req.ID, Phase: StartResolved, Run: run, Err: err}
		close(updates)
	}
	if !validSessionStart(req) {
		finish(nil, ErrInvalidRequest)
		return updates
	}
	req.Recoverable = true
	frozen, digest, err := freezeScheduledRequest(req)
	if err != nil {
		finish(nil, err)
		return updates
	}
	go func() {
		run, err := s.startSession(ctx, frozen, digest, func(phase schedule.AdmissionPhase) {
			updates <- StartUpdate{RunID: frozen.ID, Phase: StartPhase(phase)}
		})
		finish(run, err)
	}()
	return updates
}
