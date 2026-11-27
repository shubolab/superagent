//go:build !darwin && !linux

package environment

import (
	"context"
	"os"

	"github.com/shubolab/superagent/internal/contract"
)

const supportsRootedRead = false

func openNonblocking(*os.Root, string) (*os.File, error) {
	return nil, contract.ErrAccessDenied
}

func openTakeoverAppend(root *os.Root, name string) (*os.File, error) {
	return nil, os.ErrPermission
}

func takeoverWorkspaceVersion(*Local, context.Context, contract.RunScope) (string, error) {
	return "", contract.ErrRecoveryUnavailable
}
