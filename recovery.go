package superagent

import (
	"context"
	"fmt"

	"github.com/shubolab/superagent/internal/contract"
	"github.com/shubolab/superagent/internal/durable"
)

// NewLocalReceiptTool wraps a local tool in a process-safe receipt journal.
// The tool must complete its effects before returning a known result.
func NewLocalReceiptTool(directory string, tool Tool) (Tool, error) {
	if tool == nil || !durable.Supported {
		return nil, ErrInvalidConfiguration
	}
	if err := durable.Directory(directory); err != nil {
		return nil, fmt.Errorf("%w: receipt directory: %v", ErrInvalidConfiguration, err)
	}
	return &durable.ReceiptTool{Directory: directory, Tool: tool}, nil
}

// InvocationReceiptKey returns the key bound by Mesh for a recoverable invocation.
func InvocationReceiptKey(ctx context.Context) (ReceiptKey, bool) {
	return contract.InvocationReceiptKey(ctx)
}
