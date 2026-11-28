package mesh

import (
	"context"

	"github.com/shubolab/superagent/internal/contract"
)

// SessionRunner is the local Mesh capability for authenticated session recovery.
type SessionRunner interface {
	RunnerEndpoint
	Recover(context.Context, contract.SubjectID, contract.RunID) (contract.RunID, error)
	SessionBinding(context.Context) (string, error)
	SessionRequestDigest(contract.StartRequest) (string, error)
	SessionCheckpoint(contract.SubjectID, contract.RunID, string) (bool, error)
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
