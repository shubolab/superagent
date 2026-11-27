package superagent

import (
	"github.com/shubolab/superagent/internal/environment"
	"github.com/shubolab/superagent/internal/search"
)

type WorkspaceConfig = environment.WorkspaceConfig

// NewLocalReadEnvironment registers host-owned local workspace directory permissions.
func NewLocalReadEnvironment(workspaces map[WorkspaceRef]WorkspaceConfig) (EnvironmentProvider, error) {
	return environment.NewLocal(workspaces)
}

type SearchReport = search.Report
type SearchMatch = search.Match

// NewBoundedFileSearch returns the built-in read-only single-file literal search tool.
func NewBoundedFileSearch() LocalToolBinding {
	return search.New()
}
