package mesh

import (
	"context"

	"github.com/shubolab/superagent/internal/contract"
	"github.com/shubolab/superagent/internal/schedule"
)

// SessionRunner is the local Mesh capability for authenticated session recovery.
type SessionRunner interface {
	RunnerEndpoint
	Recover(context.Context, contract.SubjectID, contract.RunID) (contract.RunID, error)
	RecoverSession(context.Context, contract.SubjectID, contract.RunID, string) (contract.RunID, error)
	SessionBinding(context.Context) (string, error)
	InspectSessionBinding(context.Context) (string, error)
	SessionRequestDigest(contract.StartRequest) (string, error)
	SessionCheckpoint(contract.SubjectID, contract.RunID, string) (contract.SessionRunCheckpoint, error)
	SessionScheduler() *schedule.Scheduler
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
