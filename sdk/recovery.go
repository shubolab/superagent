package sdk

import (
	"context"
	"fmt"

	"github.com/shubolab/superagent/internal/contract"
	"github.com/shubolab/superagent/internal/durable"
	"github.com/shubolab/superagent/internal/environment"
)

// NewLocalReceiptTool wraps a local tool in a process-safe receipt journal.
// The tool must complete its effects before returning a known result.
func NewLocalReceiptTool(directory string, tool contract.Tool) (contract.Tool, error) {
	if tool == nil || !durable.Supported {
		return nil, contract.ErrInvalidConfiguration
	}
	if err := durable.Directory(directory); err != nil {
		return nil, fmt.Errorf("%w: receipt directory: %v", contract.ErrInvalidConfiguration, err)
	}
	return &durable.ReceiptTool{Directory: directory, Tool: tool}, nil
}

// InvocationReceiptKey returns the key bound by Mesh for a recoverable invocation.
func InvocationReceiptKey(ctx context.Context) (contract.ReceiptKey, bool) {
	return contract.InvocationReceiptKey(ctx)
}

// NewLocalTakeoverEnvironment exposes scoped append operations, with no writable handles.
func NewLocalTakeoverEnvironment(workspaces map[contract.WorkspaceRef]WorkspaceConfig) (contract.EnvironmentProvider, error) {
	return environment.NewTakeoverLocal(workspaces)
}

// PublishToolFile appends staged data to the single path authorized for this invocation.
func PublishToolFile(ctx context.Context, req contract.ToolRequest, path string, data []byte) error {
	return contract.PublishToolFile(ctx, req, path, data)
}

type EvidenceWorkspaceConfig = environment.EvidenceWorkspaceConfig

// NewLocalEvidenceEnvironment uses a private durable index as the workspace's published view.
func NewLocalEvidenceEnvironment(workspaces map[contract.WorkspaceRef]WorkspaceConfig, cfg EvidenceWorkspaceConfig) (contract.EnvironmentProvider, error) {
	return environment.NewEvidenceLocal(workspaces, cfg)
}
