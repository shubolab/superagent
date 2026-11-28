package environment

import (
	"context"
	"errors"

	"github.com/shubolab/superagent/internal/contract"
)

type TakeoverLocal struct{ *Local }

func (l *TakeoverLocal) RecoveryWorkspace(ctx context.Context, scope contract.RunScope) (string, error) {
	return takeoverWorkspaceVersion(l.Local, ctx, scope)
}

func (l *TakeoverLocal) FencesWorkspace(ref contract.WorkspaceRef) bool {
	_, ok := l.workspaces[ref]
	return ok
}

// PublishWorkspace appends staged bytes while Mesh holds the ownership lock.
func (l *TakeoverLocal) PublishWorkspace(ctx context.Context, scope contract.CallScope, ref contract.EnvironmentGrantRef, data []byte) error {
	if len(data) == 0 || len(data) > contract.MaxWorkspacePublishBytes ||
		!contract.HasWorkspacePublication(ctx, scope, ref) {
		return contract.ErrAccessDenied
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	_, g := l.findGrant(scope, ref)
	if g == nil || expired(ctx, scope.Run) {
		return contract.ErrAccessDenied
	}
	f, err := openTakeoverAppend(g.root, g.file)
	if err != nil {
		return err
	}
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() {
		_ = f.Close()
		return contract.ErrAccessDenied
	}
	_, writeErr := f.Write(data)
	return errors.Join(writeErr, f.Sync(), f.Close())
}

func NewTakeoverLocal(config map[contract.WorkspaceRef]WorkspaceConfig) (*TakeoverLocal, error) {
	local, err := NewLocal(config)
	if err != nil {
		return nil, err
	}
	return &TakeoverLocal{Local: local}, nil
}
