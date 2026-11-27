//go:build darwin || linux

package environment

import (
	"context"
	"fmt"
	"os"
	"syscall"

	"github.com/shubolab/superagent/internal/contract"
)

const supportsRootedRead = true

func openNonblocking(root *os.Root, name string) (*os.File, error) {
	return root.OpenFile(name, os.O_RDONLY|syscall.O_NONBLOCK, 0)
}

// RecoveryWorkspace identifies the same retained directory across process restarts.
func (l *Local) RecoveryWorkspace(ctx context.Context, scope contract.RunScope) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	ws, ok := l.workspaces[scope.Workspace]
	if !ok {
		return "", contract.ErrAccessDenied
	}
	current, err := os.Stat(ws.path)
	if err != nil || !os.SameFile(ws.identity, current) {
		return "", contract.ErrAccessDenied
	}
	stat, ok := current.Sys().(*syscall.Stat_t)
	if !ok {
		return "", contract.ErrRecoveryUnavailable
	}
	return fmt.Sprintf("%d:%d", stat.Dev, stat.Ino), nil
}
