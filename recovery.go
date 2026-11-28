package superagent

import (
	"context"

	"github.com/shubolab/superagent/internal/environment"
	"github.com/shubolab/superagent/sdk"
)

// NewLocalReceiptTool wraps a local tool in a process-safe receipt journal.
// The tool must complete its effects before returning a known result.
func NewLocalReceiptTool(directory string, tool Tool) (Tool, error) {
	return sdk.NewLocalReceiptTool(directory, tool)
}

// InvocationReceiptKey returns the key bound by Mesh for a recoverable invocation.
func InvocationReceiptKey(ctx context.Context) (ReceiptKey, bool) {
	return sdk.InvocationReceiptKey(ctx)
}

// NewLocalTakeoverEnvironment exposes scoped append operations, with no writable handles.
func NewLocalTakeoverEnvironment(workspaces map[WorkspaceRef]WorkspaceConfig) (EnvironmentProvider, error) {
	return sdk.NewLocalTakeoverEnvironment(workspaces)
}

// PublishToolFile appends staged data to the single path authorized for this invocation.
func PublishToolFile(ctx context.Context, req ToolRequest, path string, data []byte) error {
	return sdk.PublishToolFile(ctx, req, path, data)
}

type EvidenceWorkspaceConfig = environment.EvidenceWorkspaceConfig

// NewLocalEvidenceEnvironment uses a private durable index as the workspace's published view.
func NewLocalEvidenceEnvironment(workspaces map[WorkspaceRef]WorkspaceConfig, cfg EvidenceWorkspaceConfig) (EnvironmentProvider, error) {
	return sdk.NewLocalEvidenceEnvironment(workspaces, cfg)
}
