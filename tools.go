package superagent

import (
	"context"
	"io"

	"github.com/shubolab/superagent/internal/contract"
	"github.com/shubolab/superagent/internal/environment"
	"github.com/shubolab/superagent/internal/search"
)

type WorkspaceConfig = environment.WorkspaceConfig

// NewLocalReadEnvironment registers host-owned local workspace directory permissions.
func NewLocalReadEnvironment(workspaces map[WorkspaceRef]WorkspaceConfig) (EnvironmentProvider, error) {
	return environment.NewLocal(workspaces)
}

type LocalReadEnvironmentOptions = environment.ReadOptions

// NewLocalReadEnvironmentWithOptions sets shared capacity for all workspaces in the binding.
func NewLocalReadEnvironmentWithOptions(workspaces map[WorkspaceRef]WorkspaceConfig, options LocalReadEnvironmentOptions) (EnvironmentProvider, error) {
	return environment.NewLocalWithOptions(workspaces, options)
}

type SearchReport = search.Report
type SearchMatch = search.Match

// NewBoundedFileSearch returns the built-in read-only single-file literal search tool.
func NewBoundedFileSearch() LocalToolBinding {
	return search.New()
}

// OpenToolFile uses the workspace permission carried with an admitted tool invocation.
func OpenToolFile(ctx context.Context, req ToolRequest, path string) (io.ReadCloser, error) {
	return contract.OpenToolFile(ctx, req, path)
}
