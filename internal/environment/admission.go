package environment

import (
	"context"

	"github.com/shubolab/superagent/internal/contract"
)

type ReadOptions struct {
	MaxScanBytes       uint64
	MaxConcurrentReads int
	MaxQueuedReads     int
}

func DefaultReadOptions() ReadOptions {
	return ReadOptions{MaxScanBytes: 1 << 20, MaxConcurrentReads: 8, MaxQueuedReads: 16}
}

type readAdmission struct {
	active  chan struct{}
	waiting chan struct{}
}

func (a readAdmission) acquire(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case a.active <- struct{}{}:
		return nil
	default:
	}
	select {
	case a.waiting <- struct{}{}:
		defer func() { <-a.waiting }()
	default:
		return contract.ErrEnvironmentBusy
	}
	select {
	case a.active <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (a readAdmission) release() { <-a.active }

func (l *Local) DescribeRead(ctx context.Context, workspace contract.WorkspaceRef) (contract.EnvironmentReadCapabilities, error) {
	if err := ctx.Err(); err != nil {
		return contract.EnvironmentReadCapabilities{}, err
	}
	if _, ok := l.workspaces[workspace]; !ok {
		return contract.EnvironmentReadCapabilities{}, contract.ErrAccessDenied
	}
	return contract.EnvironmentReadCapabilities{
		Scan: contract.CapabilityEnforced, Deadline: contract.CapabilityEnforced,
		MaxScanBytes: l.maxScanBytes, MaxConcurrentReads: cap(l.admission.active), MaxQueuedReads: cap(l.admission.waiting),
	}, nil
}
