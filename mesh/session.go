package mesh

import (
	"context"

	"github.com/shubolab/superagent/internal/contract"
	"github.com/shubolab/superagent/schedule"
)

// SessionCheckpoint reports the retained state of a session-owned Run.
type SessionCheckpoint = contract.SessionRunCheckpoint

// SessionRunner is the Mesh capability for authenticated session recovery.
type SessionRunner interface {
	RunnerEndpoint
	Recover(context.Context, contract.SubjectID, contract.RunID) (contract.RunID, error)
	RecoverSession(context.Context, contract.SubjectID, contract.RunID, string) (contract.RunID, error)
	SessionBinding(context.Context) (string, error)
	InspectSessionBinding(context.Context) (string, error)
	SessionRequestDigest(contract.StartRequest) (string, error)
	SessionCheckpoint(contract.SubjectID, contract.RunID, string) (SessionCheckpoint, error)
	SessionScheduler() schedule.Admission
}

// SessionResolver supplies an authenticated runner and caller for session operations.
type SessionResolver interface {
	ResolveSessionRunner(context.Context, contract.RunnerRef) (SessionRunner, contract.SubjectID, error)
}

func (l *Local) ResolveSessionRunner(ctx context.Context, ref contract.RunnerRef) (SessionRunner, contract.SubjectID, error) {
	endpoint, subject, err := l.runnerCall(ctx, ref)
	if err != nil {
		return nil, "", err
	}
	runner, ok := endpoint.(SessionRunner)
	if !ok {
		return nil, "", contract.ErrRecoveryUnavailable
	}
	return runner, subject, nil
}
