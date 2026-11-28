package superagent

import (
	"context"
	"io"

	"github.com/shubolab/superagent/internal/environment"
	"github.com/shubolab/superagent/internal/search"
	"github.com/shubolab/superagent/sdk"
)

type WorkspaceConfig = environment.WorkspaceConfig

// NewLocalReadEnvironment registers host-owned local workspace directory permissions.
func NewLocalReadEnvironment(workspaces map[WorkspaceRef]WorkspaceConfig) (EnvironmentProvider, error) {
	return sdk.NewLocalReadEnvironment(workspaces)
}

type LocalReadEnvironmentOptions = environment.ReadOptions

// NewLocalReadEnvironmentWithOptions sets shared capacity for all workspaces in the binding.
func NewLocalReadEnvironmentWithOptions(workspaces map[WorkspaceRef]WorkspaceConfig, options LocalReadEnvironmentOptions) (EnvironmentProvider, error) {
	return sdk.NewLocalReadEnvironmentWithOptions(workspaces, options)
}

type SearchReport = search.Report
type SearchMatch = search.Match

// NewBoundedFileSearch returns the built-in read-only single-file literal search tool.
func NewBoundedFileSearch() LocalToolBinding {
	return sdk.NewBoundedFileSearch()
}

// OpenToolFile uses the workspace permission carried with an admitted tool invocation.
func OpenToolFile(ctx context.Context, req ToolRequest, path string) (io.ReadCloser, error) {
	return sdk.OpenToolFile(ctx, req, path)
}
